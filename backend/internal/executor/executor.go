package executor

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/models"
	"github.com/nexus/backend/internal/repository"
	"github.com/nexus/backend/internal/services"
	"github.com/nexus/backend/internal/websocket"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/shopspring/decimal"
)

type Executor struct {
	config           *config.Config
	orderRepo        *repository.OrderRepository
	fillRepo         *repository.FillRepository
	balanceRepo      *repository.BalanceRepository
	bscClient        *ethclient.Client
	baseClient       *ethclient.Client
	settlementBSC    common.Address
	settlementBase   common.Address
	executorAddr     common.Address
	abi              *abi.ABI
	bscGasPrice      *big.Int
	baseGasPrice     *big.Int
	privateKey       *ecdsa.PrivateKey
	solanaSettlement *services.SolanaSettlementService
	refundService    *services.RefundService

	nonceMu sync.Mutex

	mu        sync.RWMutex
	isRunning bool
	stopChan  chan struct{}

	stats ExecutorStats

	// Additional dependencies for broadcasting
	hub        *websocket.Hub
	pairRepo   *repository.PairRepository
	ethService *services.EthereumService
}

type ExecutorStats struct {
	TotalSettled   int64
	TotalFailed    int64
	TotalPending   int64
	LastSettleTime time.Time
}

type SettlementResult struct {
	Success bool
	FillID  uint
	TxHash  string
	Error   error
	GasUsed uint64
}

const (
	DefaultGasLimit uint64        = 300000
	MaxConcurrent   int           = 5
	RetryCount      int           = 3
	RetryDelay      time.Duration = 2 * time.Second
)

func NewExecutor(cfg *config.Config, orderRepo *repository.OrderRepository, fillRepo *repository.FillRepository, balanceRepo *repository.BalanceRepository, hub *websocket.Hub, pairRepo *repository.PairRepository, ethService *services.EthereumService, solanaSettlement *services.SolanaSettlementService, refundService *services.RefundService) (*Executor, error) {
	bscRPC := cfg.ExecutorRPCURL
	if bscRPC == "" {
		bscRPC = "https://bsc-dataseed.binance.org"
	}

	bscClient, err := ethclient.Dial(bscRPC)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to BSC RPC: %w", err)
	}

	baseRPC := cfg.ExecutorRPCURLBase
	if baseRPC == "" {
		baseRPC = "https://base-mainnet.infura.io/v3/f4c82c2334c043678a712a5e860c7edf"
	}

	baseClient, err := ethclient.Dial(baseRPC)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Base RPC: %w", err)
	}

	settlementBSC := common.HexToAddress(cfg.SettlementAddress)
	settlementBase := common.HexToAddress(cfg.SettlementAddressBase)

	settlementABI, err := abi.JSON(strings.NewReader(SettlementABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse settlement ABI: %w", err)
	}

	bscGasPrice, err := bscClient.SuggestGasPrice(context.Background())
	if err != nil {
		bscGasPrice = big.NewInt(50_000_000_000)
	}

	baseGasPrice, err := baseClient.SuggestGasPrice(context.Background())
	if err != nil {
		baseGasPrice = big.NewInt(50_000_000_000)
	}

	var privateKey *ecdsa.PrivateKey
	var executorAddr common.Address
	if cfg.ExecutorPrivateKey != "" {
		key := cfg.ExecutorPrivateKey
		if len(key) > 2 && key[:2] == "0x" {
			key = key[2:]
		}
		privateKey, err = crypto.HexToECDSA(key)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}
		executorAddr = crypto.PubkeyToAddress(privateKey.PublicKey)
	}

	return &Executor{
		config:           cfg,
		orderRepo:        orderRepo,
		fillRepo:         fillRepo,
		balanceRepo:      balanceRepo,
		bscClient:        bscClient,
		baseClient:       baseClient,
		settlementBSC:    settlementBSC,
		settlementBase:   settlementBase,
		executorAddr:     executorAddr,
		abi:              &settlementABI,
		bscGasPrice:      bscGasPrice,
		baseGasPrice:     baseGasPrice,
		privateKey:       privateKey,
		solanaSettlement: solanaSettlement,
		refundService:    refundService,
		stopChan:         make(chan struct{}),
		hub:              hub,
		pairRepo:         pairRepo,
		ethService:       ethService,
	}, nil
}

func (e *Executor) settleFillBalances(ctx context.Context, fill *models.Fill, maker, taker *models.Order) error {
	if fill == nil {
		return fmt.Errorf("fill is nil")
	}
	if maker == nil || taker == nil {
		return fmt.Errorf("maker or taker order is nil")
	}

	getLockedCredit := func(order *models.Order) (locked, credit decimal.Decimal) {
		if order.Side == models.OrderSideBuy {
			return fill.AmountIn, fill.AmountOut
		}
		return fill.AmountOut, fill.AmountIn
	}

	makerLocked, makerCredit := getLockedCredit(maker)
	takerLocked, takerCredit := getLockedCredit(taker)

	return e.balanceRepo.SettleFillBalances(ctx, fill.Network, maker.UserID, taker.UserID,
		maker.TokenIn, maker.TokenOut, makerLocked, makerCredit,
		taker.TokenIn, taker.TokenOut, takerLocked, takerCredit)
}

func (e *Executor) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.isRunning {
		e.mu.Unlock()
		return fmt.Errorf("executor already running")
	}
	e.isRunning = true
	e.mu.Unlock()

	fmt.Println("[Executor] Starting settlement executor...")
	fmt.Printf("[Executor] RPC BSC: %s, Settlement BSC: %s\n", e.config.ExecutorRPCURL, e.config.SettlementAddress)
	fmt.Printf("[Executor] RPC Base: %s, Settlement Base: %s\n", e.config.ExecutorRPCURLBase, e.config.SettlementAddressBase)
	fmt.Printf("[Executor] PrivateKey loaded: %v\n", e.privateKey != nil)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[Executor] panic in runSettlementLoop: %v\n", r)
			}
		}()
		e.runSettlementLoop(ctx)
	}()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[Executor] panic in runMetricsReporter: %v\n", r)
			}
		}()
		e.runMetricsReporter(ctx)
	}()

	return nil
}

func (e *Executor) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isRunning {
		return fmt.Errorf("executor not running")
	}

	close(e.stopChan)
	e.isRunning = false
	fmt.Println("[Executor] Executor stopped")
	return nil
}

func (e *Executor) runSettlementLoop(ctx context.Context) {
	intervalMs := e.config.ExecutorIntervalMs
	if intervalMs <= 0 {
		intervalMs = 5000
	}

	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopChan:
			return
		case <-ticker.C:
			e.processPendingSettlements(ctx)
		}
	}
}

func (e *Executor) processPendingSettlements(ctx context.Context) {
	// Warn if executor wallet balance is low so ops can top it up before fills start failing
	e.checkExecutorBalance(ctx)

	fills, err := e.fillRepo.GetPendingSettlements(ctx, 100) // Increased to batch efficiently
	if err != nil {
		fmt.Printf("[Executor] Failed to get pending settlements: %v\n", err)
		return
	}

	fmt.Printf("[Executor] Found %d pending fill(s)\n", len(fills))
	if len(fills) == 0 {
		return
	}

	e.mu.Lock()
	e.stats.TotalPending = int64(len(fills))
	e.mu.Unlock()

	// ============ NEW: Batch Settlement Logic ============
	batchConfig := DefaultBatchConfig()

	// Determine if we should batch or use individual settlement
	shouldUseBatch := shouldBatch(len(fills), 0, batchConfig)

	if shouldUseBatch && len(fills) >= 2 {
		fmt.Printf("[Executor][Batch] Using batch settlement for %d fills (strategy: %s)\n",
			len(fills), batchConfig.BatchStrategy)
		// Convert []models.Fill to []*models.Fill
		fillPtrs := make([]*models.Fill, len(fills))
		for i := range fills {
			fillPtrs[i] = &fills[i]
		}
		e.processSettlementsInBatches(ctx, fillPtrs, batchConfig)
		return
	}

	// ============ FALLBACK: Individual Settlement (for small batches or debugging) ============
	fmt.Printf("[Executor] Using individual settlement for %d fills\n", len(fills))

	var wg sync.WaitGroup
	sem := make(chan struct{}, MaxConcurrent)
	results := make(chan SettlementResult, len(fills))

	for _, fill := range fills {
		wg.Add(1)
		go func(fill models.Fill) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			result := e.settleFill(ctx, &fill)
			results <- result
		}(fill)
	}

	wg.Wait()
	close(results)

	for result := range results {
		if result.Success {
			e.mu.Lock()
			e.stats.TotalSettled++
			e.stats.LastSettleTime = time.Now()
			e.mu.Unlock()
		} else {
			e.mu.Lock()
			e.stats.TotalFailed++
			e.mu.Unlock()
			fmt.Printf("[Executor] Settlement failed for fill %d: %v\n", result.FillID, result.Error)
		}
	}
}

// processSettlementsInBatches handles settlement using batch contract functions
func (e *Executor) processSettlementsInBatches(ctx context.Context, fills []*models.Fill, config BatchConfig) {
	// Create batches from fills
	batches, fallbackFills := createBatches(ctx, fills, e.orderRepo, config)
	fmt.Printf("[Executor][Batch] Created %d batch(es) from %d fills\n", len(batches), len(fills))

	for batchIdx, batch := range batches {
		// Validate batch before settling
		err := validateBatch(ctx, batch, e.fillRepo, e.orderRepo, config.ValidationMode)
		if err != nil {
			fmt.Printf("[Executor][Batch] Batch %d validation failed: %v\n", batchIdx, err)
			// In partial mode, continue anyway; in strict mode, skip batch
			if config.ValidationMode == "strict" {
				continue
			}
		}

		result := settleBatch(ctx, batch, e)
		fmt.Printf("[Executor][Batch] Batch %d (%s) completed: %d/%d settled, gas: %d\n",
			batchIdx, result.BatchID, result.SuccessCount, result.TotalFills, result.GasUsed)

		if result.Error != nil {
			fmt.Printf("[Executor][Batch] Batch %d settlement error: %v\n", batchIdx, result.Error)
		}

		e.mu.Lock()
		e.stats.TotalSettled += int64(result.SuccessCount)
		e.stats.TotalFailed += int64(result.FailedCount)
		e.stats.LastSettleTime = time.Now()
		e.mu.Unlock()

		if len(result.FailedFillIDs) > 0 {
			fmt.Printf("[Executor][Batch] Batch %d failed fills: %v\n", batchIdx, result.FailedFillIDs)
		}
	}

	if len(fallbackFills) > 0 {
		fmt.Printf("[Executor][Batch] Falling back to individual settlement for %d unsupported fills\n", len(fallbackFills))
		for _, fill := range fallbackFills {
			result := e.settleFill(ctx, fill)
			if result.Success {
				e.mu.Lock()
				e.stats.TotalSettled++
				e.mu.Unlock()
			} else {
				e.mu.Lock()
				e.stats.TotalFailed++
				e.mu.Unlock()
				fmt.Printf("[Executor][Batch] Fallback fill %d failed: %v\n", result.FillID, result.Error)
			}
		}
	}
}

func (e *Executor) settleFill(ctx context.Context, fill *models.Fill) SettlementResult {
	var err error
	for i := 0; i < RetryCount; i++ {
		err = e.executeSettlement(ctx, fill)
		if err == nil {
			return SettlementResult{
				Success: true,
				FillID:  fill.ID,
				TxHash:  fill.TxHash,
			}
		}

		if isRetryableError(err) {
			time.Sleep(RetryDelay)
			continue
		}
		break
	}

	return SettlementResult{
		Success: false,
		FillID:  fill.ID,
		Error:   err,
	}
}

func (e *Executor) executeSettlement(ctx context.Context, fill *models.Fill) error {
	makerOrder, err := e.orderRepo.GetByID(ctx, fill.MakerOrderID)
	if err != nil {
		return fmt.Errorf("failed to get maker order: %w", err)
	}

	takerOrder, err := e.orderRepo.GetByID(ctx, fill.TakerOrderID)
	if err != nil {
		return fmt.Errorf("failed to get taker order: %w", err)
	}

	if err := e.validateOrderState(makerOrder); err != nil {
		return e.markFillFailed(ctx, fill, err)
	}
	if err := e.validateOrderState(takerOrder); err != nil {
		return e.markFillFailed(ctx, fill, err)
	}

	// Route Solana fills to the dedicated Solana settlement service.
	// EVM fills (BSC, Base) go through the standard on-chain matchOrders path below.
	if fill.Network == models.NetworkSolana {
		return e.settleSolanaFill(ctx, fill, makerOrder, takerOrder)
	}

	if makerOrder.LadderParentID != nil || takerOrder.LadderParentID != nil {
		return e.settleLadderOrder(ctx, fill, makerOrder, takerOrder)
	}

	return e.settleRegularOrder(ctx, fill, makerOrder, takerOrder)
}

// settleSolanaFill handles settlement for Solana network fills via the dedicated Solana service.
// It is completely isolated from the EVM settlement path.
func (e *Executor) settleSolanaFill(ctx context.Context, fill *models.Fill, maker, taker *models.Order) error {
	if e.solanaSettlement == nil {
		return fmt.Errorf("solana settlement service not configured for fill %d", fill.ID)
	}

	result, err := e.solanaSettlement.SettleFill(ctx, fill, maker, taker)
	if err != nil {
		return err
	}

	if err := e.markFillSettled(ctx, fill, result.TxHashes); err != nil {
		return err
	}

	// Update maker order status
	if maker.FilledAmount.GreaterThanOrEqual(maker.Amount) {
		maker.Status = models.OrderStatusFilled
		if e.refundService != nil {
			if err := e.refundService.CreateRefundForOrder(ctx, maker); err != nil {
				fmt.Printf("[Executor] Failed to check/create refund for filled maker order %d: %v\n", maker.ID, err)
			}
		}
	}
	if err := e.orderRepo.Update(ctx, maker); err != nil {
		fmt.Printf("[Executor] Failed to update maker order %d: %v\n", maker.ID, err)
	}

	// Update taker order status
	if taker.FilledAmount.GreaterThanOrEqual(taker.Amount) {
		taker.Status = models.OrderStatusFilled
		if e.refundService != nil {
			if err := e.refundService.CreateRefundForOrder(ctx, taker); err != nil {
				fmt.Printf("[Executor] Failed to check/create refund for filled taker order %d: %v\n", taker.ID, err)
			}
		}
	}
	if err := e.orderRepo.Update(ctx, taker); err != nil {
		fmt.Printf("[Executor] Failed to update taker order %d: %v\n", taker.ID, err)
	}

	e.broadcastTradeUpdate(fill)
	e.broadcastTickerUpdate(ctx, fill)

	return nil
}

func (e *Executor) settleRegularOrder(ctx context.Context, fill *models.Fill, maker, taker *models.Order) error {
	client, gasPrice, settlement, chainID, executorAddr := e.getNetworkConfig(fill.Network)

	fmt.Printf("[Executor][Debug] settleRegularOrder fillID=%d network=%s executorAddr=%s settlement=%s chainID=%s gasPrice=%s\n",
		fill.ID, fill.Network, executorAddr.Hex(), settlement.Hex(), chainID.String(), gasPrice.String())
	fmt.Printf("[Executor][Debug] makerOrderID=%d takerOrderID=%d makerSide=%s takerSide=%s fillAmount=%s fillAmountIn=%s fillAmountOut=%s\n",
		maker.ID, taker.ID, maker.Side, taker.Side, fill.Amount.String(), fill.AmountIn.String(), fill.AmountOut.String())

	// Determine which order is buy and which is sell
	var buyOrder, sellOrder *models.Order
	var sigBuy, sigSell []byte

	if maker.Side == models.OrderSideBuy {
		buyOrder = maker
		sellOrder = taker
		sigBuy, _ = hex.DecodeString(strings.TrimPrefix(maker.Signature, "0x"))
		sigSell, _ = hex.DecodeString(strings.TrimPrefix(taker.Signature, "0x"))
	} else {
		buyOrder = taker
		sellOrder = maker
		sigBuy, _ = hex.DecodeString(strings.TrimPrefix(taker.Signature, "0x"))
		sigSell, _ = hex.DecodeString(strings.TrimPrefix(maker.Signature, "0x"))
	}

	fmt.Printf("[Executor][Debug] buySignature len=%d sigHex=%s\n", len(sigBuy), strings.TrimPrefix(buyOrder.Signature, "0x"))
	fmt.Printf("[Executor][Debug] sellSignature len=%d sigHex=%s\n", len(sigSell), strings.TrimPrefix(sellOrder.Signature, "0x"))

	buyOrderStruct := buildOrderStruct(buyOrder)
	sellOrderStruct := buildOrderStruct(sellOrder)

	if err := e.validateOrderBalanceAndAllowance(ctx, client, buyOrder, fill, settlement); err != nil {
		fmt.Printf("[Executor][Debug] validateOrderBalanceAndAllowance failed buyOrderID=%d err=%v\n", buyOrder.ID, err)
		return err
	}
	if err := e.validateOrderBalanceAndAllowance(ctx, client, sellOrder, fill, settlement); err != nil {
		fmt.Printf("[Executor][Debug] validateOrderBalanceAndAllowance failed sellOrderID=%d err=%v\n", sellOrder.ID, err)
		return err
	}

	fmt.Printf("[Executor] Buy order: maker=%s, receiver=%s, tokenIn=%s, tokenOut=%s, amountIn=%s, amountOutMin=%s, nonce=%d, salt=%d\n",
		buyOrder.Maker, buyOrder.Receiver, buyOrder.TokenIn, buyOrder.TokenOut, buyOrder.AmountIn.String(), buyOrder.AmountOutMin.String(), buyOrder.Nonce, buyOrder.Salt)
	fmt.Printf("[Executor] Sell order: maker=%s, receiver=%s, tokenIn=%s, tokenOut=%s, amountIn=%s, amountOutMin=%s, nonce=%d, salt=%d\n",
		sellOrder.Maker, sellOrder.Receiver, sellOrder.TokenIn, sellOrder.TokenOut, sellOrder.AmountIn.String(), sellOrder.AmountOutMin.String(), sellOrder.Nonce, sellOrder.Salt)
	fmt.Printf("[Executor] Fill: amount=%s, amountIn=%s, amountOut=%s\n",
		fill.Amount.String(), fill.AmountIn.String(), fill.AmountOut.String())

	amountBase := fill.Amount
	fmt.Printf("[Executor][Debug] matchOrders payload: amountBase=%s buyOrder=%d sellOrder=%d\n", amountBase.String(), buyOrder.ID, sellOrder.ID)
	input, err := e.abi.Pack("matchOrders", buyOrderStruct, sigBuy, sellOrderStruct, sigSell, amountBase.BigInt())
	if err != nil {
		fmt.Printf("[Executor][Debug] failed to pack matchOrders payload: %v\n", err)
		return fmt.Errorf("failed to pack matchOrders data: %w", err)
	}
	fmt.Printf("[Executor][Debug] matchOrders input len=%d\n", len(input))

	nonce, err := e.getPendingNonce(ctx, client, executorAddr)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %w", err)
	}

	gasLimit := e.estimateGasWithFallback(ctx, client, executorAddr, settlement, input)
	tx := types.NewTransaction(nonce, settlement, big.NewInt(0), gasLimit, gasPrice, input)
	fmt.Printf("[Executor][Debug] tx created nonce=%d to=%s gasLimit=%d gasPrice=%s value=%s\n",
		nonce, settlement.Hex(), gasLimit, gasPrice.String(), "0")

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), e.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %w", err)
	}

	err = client.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send transaction: %w", err)
	}

	// Set tx_hash immediately and broadcast notification right away
	fill.TxHash = signedTx.Hash().Hex()
	fill.Status = "pending" // Mark as pending until confirmed

	// Update fill in database immediately with tx_hash
	if err := e.fillRepo.Update(ctx, fill); err != nil {
		return fmt.Errorf("failed to update fill with tx_hash: %w", err)
	}

	// Broadcast immediate notification to frontend with tx_hash
	fmt.Printf("[Executor] Sent fill %d transaction: %s - broadcasting immediate notification\n", fill.ID, fill.TxHash)
	e.broadcastTradeUpdate(fill)
	// Broadcast immediate ticker update for real-time price data
	e.broadcastTickerUpdate(ctx, fill)

	// Now wait for on-chain confirmation
	receipt, err := e.waitForReceipt(ctx, client, signedTx.Hash())
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("transaction reverted - check tx %s on explorer for reason", signedTx.Hash().Hex())
	}

	fmt.Printf("[Executor] Confirmed fill %d on-chain, tx: %s\n", fill.ID, signedTx.Hash().Hex())

	// Update with final confirmation details
	fill.BlockNumber = receipt.BlockNumber.Uint64()
	fill.GasUsed = receipt.GasUsed
	fill.Status = "settled"

	if err := e.fillRepo.Update(ctx, fill); err != nil {
		return fmt.Errorf("failed to update fill confirmation: %w", err)
	}

	// Update maker order status to filled
	if maker.FilledAmount.GreaterThanOrEqual(maker.Amount) {
		maker.Status = models.OrderStatusFilled
	}
	if err := e.orderRepo.Update(ctx, maker); err != nil {
		fmt.Printf("[Executor] Failed to update maker order %d: %v\n", maker.ID, err)
	}

	// Update taker order status to filled
	if taker.FilledAmount.GreaterThanOrEqual(taker.Amount) {
		taker.Status = models.OrderStatusFilled
	}
	if err := e.orderRepo.Update(ctx, taker); err != nil {
		fmt.Printf("[Executor] Failed to update taker order %d: %v\n", taker.ID, err)
	}

	// Broadcast updates to UI after successful settlement
	e.broadcastTradeUpdate(fill)
	e.broadcastTickerUpdate(ctx, fill)

	return nil
}

func (e *Executor) getPendingNonce(ctx context.Context, client *ethclient.Client, executorAddr common.Address) (uint64, error) {
	e.nonceMu.Lock()
	defer e.nonceMu.Unlock()

	return client.PendingNonceAt(ctx, executorAddr)
}

func (e *Executor) validateOrderState(order *models.Order) error {
	if order.Status == models.OrderStatusCancelled || order.Status == models.OrderStatusExpired || order.Status == models.OrderStatusTriggered {
		return fmt.Errorf("order %d not fillable: status=%s", order.ID, order.Status)
	}

	if order.CommitExpired {
		return fmt.Errorf("order %d commit expired", order.ID)
	}

	return nil
}

func (e *Executor) validateOrderBalanceAndAllowance(ctx context.Context, client *ethclient.Client, order *models.Order, fill *models.Fill, settlement common.Address) error {
	if !common.IsHexAddress(order.TokenIn) {
		return fmt.Errorf("order %d has invalid token_in address %q", order.ID, order.TokenIn)
	}
	tokenAddr := common.HexToAddress(order.TokenIn)
	owner := common.HexToAddress(order.Maker)
	requiredAmount := orderRequiredTokenAmount(order, fill)

	bal, err := e.getERC20Balance(ctx, client, tokenAddr, owner)
	if err != nil {
		return fmt.Errorf("failed to check balance for order %d maker %s: %w", order.ID, order.Maker, err)
	}
	if bal.Cmp(requiredAmount) < 0 {
		return fmt.Errorf("maker %s has insufficient balance for order %d: token=%s have=%s need=%s", order.Maker, order.ID, order.TokenIn, bal.String(), requiredAmount.String())
	}

	allowance, err := e.getERC20Allowance(ctx, client, tokenAddr, owner, settlement)
	if err != nil {
		return fmt.Errorf("failed to check allowance for order %d maker %s: %w", order.ID, order.Maker, err)
	}
	if allowance.Cmp(requiredAmount) < 0 {
		return fmt.Errorf("maker %s has insufficient allowance for order %d: token=%s have=%s need=%s", order.Maker, order.ID, order.TokenIn, allowance.String(), requiredAmount.String())
	}

	return nil
}

func orderRequiredTokenAmount(order *models.Order, fill *models.Fill) *big.Int {
	if order.Side == models.OrderSideBuy {
		return fill.AmountIn.BigInt()
	}
	return fill.Amount.BigInt()
}

func (e *Executor) markFillFailed(ctx context.Context, fill *models.Fill, err error) error {
	fill.Status = "failed"
	if updateErr := e.fillRepo.Update(ctx, fill); updateErr != nil {
		return fmt.Errorf("%w; additionally failed to mark fill failed: %v", err, updateErr)
	}
	return err
}

func (e *Executor) markFillSettled(ctx context.Context, fill *models.Fill, txHashes []string) error {
	if fill.Status == "settled" {
		return nil
	}

	fill.Status = "settled"
	if len(txHashes) > 0 {
		fill.TxHash = txHashes[0]
		if fill.Network == models.NetworkSolana {
			fill.TxHashBuy = txHashes[0]
			if len(txHashes) > 1 {
				fill.TxHashSell = txHashes[1]
			}
		}
	}

	if err := e.fillRepo.Update(ctx, fill); err != nil {
		return fmt.Errorf("failed to mark fill settled: %w", err)
	}

	return nil
}

func getFunctionSelector(funcName string) []byte {
	hash := crypto.Keccak256([]byte(funcName))
	return hash[:4]
}

func (e *Executor) getERC20Balance(ctx context.Context, client *ethclient.Client, tokenAddr, owner common.Address) (*big.Int, error) {
	data := append(getFunctionSelector("balanceOf(address)"), common.LeftPadBytes(owner.Bytes(), 32)...)
	res, err := client.CallContract(ctx, ethereum.CallMsg{To: &tokenAddr, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	if len(res) < 32 {
		return nil, fmt.Errorf("invalid balanceOf response for %s", tokenAddr.Hex())
	}
	return new(big.Int).SetBytes(res[:32]), nil
}

func (e *Executor) getERC20Allowance(ctx context.Context, client *ethclient.Client, tokenAddr, owner, spender common.Address) (*big.Int, error) {
	data := append(getFunctionSelector("allowance(address,address)"), common.LeftPadBytes(owner.Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(spender.Bytes(), 32)...)
	res, err := client.CallContract(ctx, ethereum.CallMsg{To: &tokenAddr, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	if len(res) < 32 {
		return nil, fmt.Errorf("invalid allowance response for %s", tokenAddr.Hex())
	}
	return new(big.Int).SetBytes(res[:32]), nil
}

func (e *Executor) logLadderTokenDebug(ctx context.Context, client *ethclient.Client, settlement common.Address, executorAddr, maker common.Address, tokenIn, tokenOut common.Address) {
	executorTokenOutBal, balErr := e.getERC20Balance(ctx, client, tokenOut, executorAddr)
	executorTokenOutAllow, allowErr := e.getERC20Allowance(ctx, client, tokenOut, executorAddr, settlement)
	makerTokenInBal, makerBalErr := e.getERC20Balance(ctx, client, tokenIn, maker)
	makerTokenInAllow, makerAllowErr := e.getERC20Allowance(ctx, client, tokenIn, maker, settlement)

	fmt.Printf("[Executor][LadderDebug] executor=%s maker=%s tokenIn=%s tokenOut=%s settlement=%s\n",
		executorAddr.Hex(), maker.Hex(), tokenIn.Hex(), tokenOut.Hex(), settlement.Hex())
	if balErr == nil {
		fmt.Printf("[Executor][LadderDebug] executor tokenOut balance=%s\n", executorTokenOutBal.String())
	} else {
		fmt.Printf("[Executor][LadderDebug] executor tokenOut balance err=%v\n", balErr)
	}
	if allowErr == nil {
		fmt.Printf("[Executor][LadderDebug] executor tokenOut allowance=%s\n", executorTokenOutAllow.String())
	} else {
		fmt.Printf("[Executor][LadderDebug] executor tokenOut allowance err=%v\n", allowErr)
	}
	if makerBalErr == nil {
		fmt.Printf("[Executor][LadderDebug] maker tokenIn balance=%s\n", makerTokenInBal.String())
	} else {
		fmt.Printf("[Executor][LadderDebug] maker tokenIn balance err=%v\n", makerBalErr)
	}
	if makerAllowErr == nil {
		fmt.Printf("[Executor][LadderDebug] maker tokenIn allowance=%s\n", makerTokenInAllow.String())
	} else {
		fmt.Printf("[Executor][LadderDebug] maker tokenIn allowance err=%v\n", makerAllowErr)
	}
}

func (e *Executor) settleLadderOrder(ctx context.Context, fill *models.Fill, maker, taker *models.Order) error {
	client, gasPrice, settlement, chainID, executorAddr := e.getNetworkConfig(fill.Network)

	makerIsLadder := maker.LadderParentID != nil
	takerIsLadder := taker.LadderParentID != nil

	if makerIsLadder && takerIsLadder {
		// Both sides are ladder-child orders: use matchLadderOrders so executor is only matcher.
		buyOrder, sellOrder := maker, taker
		if maker.Side != models.OrderSideBuy {
			buyOrder = taker
			sellOrder = maker
		}

		resolvedBuyOrder, err := e.resolveLadderParentOrder(ctx, buyOrder)
		if err != nil {
			return err
		}
		resolvedSellOrder, err := e.resolveLadderParentOrder(ctx, sellOrder)
		if err != nil {
			return err
		}

		fmt.Printf("[Executor][Debug] settleLadderOrder both ladder fillID=%d network=%s executorAddr=%s settlement=%s chainID=%s gasPrice=%s\n",
			fill.ID, fill.Network, executorAddr.Hex(), settlement.Hex(), chainID.String(), gasPrice.String())
		fmt.Printf("[Executor][Debug] buyOrderID=%d sellOrderID=%d buySigLen=%d sellSigLen=%d\n",
			buyOrder.ID, sellOrder.ID, len(strings.TrimPrefix(buyOrder.Signature, "0x"))/2, len(strings.TrimPrefix(sellOrder.Signature, "0x"))/2)

		if err := e.validateOrderState(buyOrder); err != nil {
			return e.markFillFailed(ctx, fill, err)
		}
		if err := e.validateOrderState(sellOrder); err != nil {
			return e.markFillFailed(ctx, fill, err)
		}
		if err := e.validateOrderState(resolvedBuyOrder); err != nil {
			return e.markFillFailed(ctx, fill, err)
		}
		if err := e.validateOrderState(resolvedSellOrder); err != nil {
			return e.markFillFailed(ctx, fill, err)
		}

		if err := e.validateOrderBalanceAndAllowance(ctx, client, buyOrder, fill, settlement); err != nil {
			return err
		}
		if err := e.validateOrderBalanceAndAllowance(ctx, client, sellOrder, fill, settlement); err != nil {
			return err
		}

		buyAuth := buildLadderAuthStruct(resolvedBuyOrder)
		sellAuth := buildLadderAuthStruct(resolvedSellOrder)
		buyLevelIndex := getLadderLevelIndex(resolvedBuyOrder, fill.Price)
		sellLevelIndex := getLadderLevelIndex(resolvedSellOrder, fill.Price)
		amountBase := fill.Amount.BigInt()

		fmt.Printf("[Executor][LadderDebug] MATCH_LADDER fillID=%d makerOrderID=%d takerOrderID=%d buyLevelIndex=%d sellLevelIndex=%d amountBase=%s\n",
			fill.ID, maker.ID, taker.ID, buyLevelIndex, sellLevelIndex, amountBase.String())
		fmt.Printf("[Executor][LadderDebug] buyAuth maker=%s tokenIn=%s tokenOut=%s totalAmount=%s levels=%d expiration=%s\n",
			resolvedBuyOrder.Maker, resolvedBuyOrder.TokenIn, resolvedBuyOrder.TokenOut,
			resolvedBuyOrder.LadderTotalAmountIn.String(), resolvedBuyOrder.LadderLevels, resolvedBuyOrder.Expiration.String())
		fmt.Printf("[Executor][LadderDebug] sellAuth maker=%s tokenIn=%s tokenOut=%s totalAmount=%s levels=%d expiration=%s\n",
			resolvedSellOrder.Maker, resolvedSellOrder.TokenIn, resolvedSellOrder.TokenOut,
			resolvedSellOrder.LadderTotalAmountIn.String(), resolvedSellOrder.LadderLevels, resolvedSellOrder.Expiration.String())

		buySig := hexDecodeSignature(buyOrder.Signature)
		sellSig := hexDecodeSignature(sellOrder.Signature)
		fmt.Printf("[Executor][LadderDebug] matchLadderOrders buySigLen=%d sellSigLen=%d\n", len(buySig), len(sellSig))
		input, err := e.abi.Pack("matchLadderOrders",
			buyAuth,
			buySig,
			sellAuth,
			sellSig,
			big.NewInt(int64(buyLevelIndex)),
			big.NewInt(int64(sellLevelIndex)),
			amountBase,
		)
		if err != nil {
			fmt.Printf("[Executor][LadderDebug] failed to pack matchLadderOrders: %v\n", err)
			return fmt.Errorf("failed to pack matchLadderOrders: %w", err)
		}

		nonce, err := e.getPendingNonce(ctx, client, executorAddr)
		if err != nil {
			return fmt.Errorf("failed to get nonce: %w", err)
		}

		tx := types.NewTransaction(nonce, settlement, big.NewInt(0), DefaultGasLimit, gasPrice, input)
		signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), e.privateKey)
		if err != nil {
			return fmt.Errorf("failed to sign transaction: %w", err)
		}

		err = client.SendTransaction(ctx, signedTx)
		if err != nil {
			return fmt.Errorf("failed to send transaction: %w", err)
		}

		// Set tx_hash immediately and broadcast notification right away for ladder orders
		fill.TxHash = signedTx.Hash().Hex()
		fill.Status = "pending" // Mark as pending until confirmed

		// Update fill in database immediately with tx_hash
		if err := e.fillRepo.Update(ctx, fill); err != nil {
			return fmt.Errorf("failed to update ladder fill with tx_hash: %w", err)
		}

		// Broadcast immediate notification to frontend with tx_hash
		fmt.Printf("[Executor] Sent ladder fill %d transaction: %s - broadcasting immediate notification\n", fill.ID, fill.TxHash)
		e.broadcastTradeUpdate(fill)
		// Broadcast immediate ticker update for real-time price data
		e.broadcastTickerUpdate(ctx, fill)

		// Now wait for on-chain confirmation
		receipt, err := e.waitForReceipt(ctx, client, signedTx.Hash())
		if err != nil {
			return fmt.Errorf("transaction failed: %w", err)
		}

		if receipt.Status == 0 {
			return fmt.Errorf("transaction reverted")
		}

		fmt.Printf("[Executor] Confirmed ladder fill %d on-chain, tx: %s\n", fill.ID, signedTx.Hash().Hex())

		// Update with final confirmation details
		fill.BlockNumber = receipt.BlockNumber.Uint64()
		fill.GasUsed = receipt.GasUsed
		fill.Status = "settled"

		if err := e.fillRepo.Update(ctx, fill); err != nil {
			return fmt.Errorf("failed to update ladder fill confirmation: %w", err)
		}

		// Update both orders to filled
		if maker.FilledAmount.GreaterThanOrEqual(maker.Amount) {
			maker.Status = models.OrderStatusFilled
		}
		e.orderRepo.Update(ctx, maker)
		if taker.FilledAmount.GreaterThanOrEqual(taker.Amount) {
			taker.Status = models.OrderStatusFilled
		}
		e.orderRepo.Update(ctx, taker)

		// Broadcast updates to UI after successful settlement
		e.broadcastTradeUpdate(fill)
		e.broadcastTickerUpdate(ctx, fill)

		return nil
	}

	// Single ladder side: use a neutral matcher path when a sell ladder matches a regular buy order.
	ladderOrder := maker
	regularOrder := taker
	if !makerIsLadder {
		ladderOrder = taker
		regularOrder = maker
	}
	ladderIsBuy := ladderOrder.Side == models.OrderSideBuy

	resolvedLadderOrder, err := e.resolveLadderParentOrder(ctx, ladderOrder)
	if err != nil {
		return err
	}

	ladderAuth := buildLadderAuthStruct(resolvedLadderOrder)
	levelIndex := getLadderLevelIndex(resolvedLadderOrder, fill.Price)

	if !ladderIsBuy && regularOrder.Side == models.OrderSideBuy {
		if err := e.validateOrderState(regularOrder); err != nil {
			return e.markFillFailed(ctx, fill, err)
		}
		if err := e.validateOrderState(ladderOrder); err != nil {
			return e.markFillFailed(ctx, fill, err)
		}
		if err := e.validateOrderState(resolvedLadderOrder); err != nil {
			return e.markFillFailed(ctx, fill, err)
		}
		if err := e.validateOrderBalanceAndAllowance(ctx, client, regularOrder, fill, settlement); err != nil {
			return err
		}
		if err := e.validateOrderBalanceAndAllowance(ctx, client, ladderOrder, fill, settlement); err != nil {
			return err
		}
		return e.settleRegularBuyWithLadderSell(ctx, fill, regularOrder, ladderOrder, resolvedLadderOrder, ladderAuth, levelIndex)
	}

	if err := e.validateOrderState(regularOrder); err != nil {
		return e.markFillFailed(ctx, fill, err)
	}
	if err := e.validateOrderState(ladderOrder); err != nil {
		return e.markFillFailed(ctx, fill, err)
	}
	if err := e.validateOrderState(resolvedLadderOrder); err != nil {
		return e.markFillFailed(ctx, fill, err)
	}

	if err := e.validateOrderBalanceAndAllowance(ctx, client, regularOrder, fill, settlement); err != nil {
		return err
	}
	if err := e.validateOrderBalanceAndAllowance(ctx, client, ladderOrder, fill, settlement); err != nil {
		return err
	}

	var amountIn *big.Int
	var takerMinAmountOut *big.Int
	if ladderIsBuy {
		amountIn = fill.AmountIn.BigInt()
		takerMinAmountOut = fill.AmountOut.BigInt()
	} else {
		amountIn = fill.Amount.BigInt()
		takerMinAmountOut = fill.AmountIn.BigInt()
	}

	tokenOutAddr := common.HexToAddress(resolvedLadderOrder.TokenOut)
	executorTokenOutBal, balErr := e.getERC20Balance(ctx, client, tokenOutAddr, executorAddr)
	executorTokenOutAllow, allowErr := e.getERC20Allowance(ctx, client, tokenOutAddr, executorAddr, settlement)
	if balErr == nil && executorTokenOutBal.Cmp(amountIn) < 0 {
		return fmt.Errorf("executor insufficient tokenOut balance: have %s, need %s", executorTokenOutBal.String(), amountIn.String())
	}
	if allowErr == nil && executorTokenOutAllow.Cmp(amountIn) < 0 {
		return fmt.Errorf("executor insufficient tokenOut allowance: have %s, need %s", executorTokenOutAllow.String(), amountIn.String())
	}

	fmt.Printf("[Executor][LadderDebug] fillID=%d makerOrderID=%d takerOrderID=%d ladderIsBuy=%v levelIndex=%d fill.Price=%s amountIn=%s takerMinAmountOut=%s\n",
		fill.ID, maker.ID, taker.ID, ladderIsBuy, levelIndex, fill.Price.String(), amountIn.String(), takerMinAmountOut.String())
	fmt.Printf("[Executor][LadderDebug] auth maker=%s tokenIn=%s tokenOut=%s totalAmount=%s priceStart=%s priceEnd=%s levels=%d expiration=%s\n",
		resolvedLadderOrder.Maker, resolvedLadderOrder.TokenIn, resolvedLadderOrder.TokenOut,
		resolvedLadderOrder.LadderTotalAmountIn.String(), resolvedLadderOrder.LadderPriceStart.String(), resolvedLadderOrder.LadderPriceEnd.String(), resolvedLadderOrder.LadderLevels, resolvedLadderOrder.Expiration.String())
	e.logLadderTokenDebug(ctx, client, settlement, executorAddr, common.HexToAddress(resolvedLadderOrder.Maker), common.HexToAddress(resolvedLadderOrder.TokenIn), tokenOutAddr)

	ladderSig := hexDecodeSignature(ladderOrder.Signature)
	fmt.Printf("[Executor][LadderDebug] fillLadderOrder sigLen=%d sigHex=%s\n", len(ladderSig), strings.TrimPrefix(ladderOrder.Signature, "0x"))
	input, err := e.abi.Pack("fillLadderOrder",
		ladderAuth,
		ladderSig,
		big.NewInt(int64(levelIndex)),
		amountIn,
		takerMinAmountOut,
	)
	if err != nil {
		return fmt.Errorf("failed to pack fillLadderOrder: %w", err)
	}

	nonce, err := e.getPendingNonce(ctx, client, executorAddr)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %w", err)
	}

	tx := types.NewTransaction(nonce, settlement, big.NewInt(0), DefaultGasLimit, gasPrice, input)

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), e.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %w", err)
	}

	err = client.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send transaction: %w", err)
	}

	// Set tx_hash immediately and broadcast notification right away for regular+ladder orders
	fill.TxHash = signedTx.Hash().Hex()
	fill.Status = "pending" // Mark as pending until confirmed

	// Update fill in database immediately with tx_hash
	if err := e.fillRepo.Update(ctx, fill); err != nil {
		return fmt.Errorf("failed to update regular+ladder fill with tx_hash: %w", err)
	}

	// Broadcast immediate notification to frontend with tx_hash
	fmt.Printf("[Executor] Sent regular+ladder fill %d transaction: %s - broadcasting immediate notification\n", fill.ID, fill.TxHash)
	e.broadcastTradeUpdate(fill)

	// Now wait for on-chain confirmation
	receipt, err := e.waitForReceipt(ctx, client, signedTx.Hash())
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("transaction reverted")
	}

	fmt.Printf("[Executor] Confirmed regular+ladder fill %d on-chain, tx: %s\n", fill.ID, signedTx.Hash().Hex())

	// Update with final confirmation details
	fill.BlockNumber = receipt.BlockNumber.Uint64()
	fill.GasUsed = receipt.GasUsed
	fill.Status = "settled"

	if err := e.fillRepo.Update(ctx, fill); err != nil {
		return fmt.Errorf("failed to update regular+ladder fill confirmation: %w", err)
	}

	// Update both orders to filled
	if regularOrder.FilledAmount.GreaterThanOrEqual(regularOrder.Amount) {
		regularOrder.Status = models.OrderStatusFilled
	}
	e.orderRepo.Update(ctx, regularOrder)
	if ladderOrder.FilledAmount.GreaterThanOrEqual(ladderOrder.Amount) {
		ladderOrder.Status = models.OrderStatusFilled
	}
	e.orderRepo.Update(ctx, ladderOrder)

	// Broadcast updates to UI after successful settlement
	e.broadcastTradeUpdate(fill)
	e.broadcastTickerUpdate(ctx, fill)

	return nil
}

func (e *Executor) waitForReceipt(ctx context.Context, client *ethclient.Client, txHash common.Hash) (*types.Receipt, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(60 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, fmt.Errorf("transaction timeout")
		case <-ticker.C:
			receipt, err := client.TransactionReceipt(ctx, txHash)
			if err == nil {
				return receipt, nil
			}
		}
	}
}

func (e *Executor) runMetricsReporter(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopChan:
			return
		case <-ticker.C:
			e.mu.RLock()
			fmt.Printf("[Executor Stats] Settled: %d, Failed: %d, Pending: %d, LastSettle: %v\n",
				e.stats.TotalSettled, e.stats.TotalFailed, e.stats.TotalPending, e.stats.LastSettleTime)
			e.mu.RUnlock()
		}
	}
}

func buildOrderStruct(order *models.Order) interface{} {
	return struct {
		Maker        common.Address
		TokenIn      common.Address
		TokenOut     common.Address
		AmountIn     *big.Int
		Amount       *big.Int
		AmountOutMin *big.Int
		Expiration   *big.Int
		Nonce        *big.Int
		Receiver     common.Address
		Salt         *big.Int
	}{
		Maker:        common.HexToAddress(order.Maker),
		TokenIn:      common.HexToAddress(order.TokenIn),
		TokenOut:     common.HexToAddress(order.TokenOut),
		AmountIn:     order.AmountIn.BigInt(),
		Amount:       order.Amount.BigInt(),
		AmountOutMin: order.AmountOutMin.BigInt(),
		Expiration:   big.NewInt(order.Expiration.Unix()),
		Nonce:        new(big.Int).SetUint64(order.Nonce),
		Receiver:     common.HexToAddress(order.Receiver),
		Salt:         new(big.Int).SetUint64(order.Salt),
	}
}

func hexDecodeSignature(signature string) []byte {
	sig, err := hex.DecodeString(strings.TrimPrefix(signature, "0x"))
	if err != nil {
		return nil
	}
	return sig
}

func (e *Executor) resolveLadderParentOrder(ctx context.Context, order *models.Order) (*models.Order, error) {
	if order.LadderParentID == nil {
		return order, nil
	}

	parentOrder, err := e.orderRepo.GetByID(ctx, *order.LadderParentID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve ladder parent order: %w", err)
	}

	return parentOrder, nil
}

func (e *Executor) settleRegularBuyWithLadderSell(
	ctx context.Context,
	fill *models.Fill,
	buyOrder, ladderOrder *models.Order,
	resolvedLadderOrder *models.Order,
	ladderAuth interface{},
	levelIndex int,
) error {
	client, gasPrice, settlement, chainID, executorAddr := e.getNetworkConfig(fill.Network)
	amountBase := fill.Amount.BigInt()

	if err := e.validateOrderState(buyOrder); err != nil {
		return e.markFillFailed(ctx, fill, err)
	}
	if err := e.validateOrderState(ladderOrder); err != nil {
		return e.markFillFailed(ctx, fill, err)
	}
	if err := e.validateOrderState(resolvedLadderOrder); err != nil {
		return e.markFillFailed(ctx, fill, err)
	}
	if err := e.validateOrderBalanceAndAllowance(ctx, client, buyOrder, fill, settlement); err != nil {
		return err
	}
	if err := e.validateOrderBalanceAndAllowance(ctx, client, ladderOrder, fill, settlement); err != nil {
		return err
	}

	fmt.Printf("[Executor][LadderDebug] NEUTRAL_MATCH fillID=%d buyOrderID=%d ladderOrderID=%d levelIndex=%d amountBase=%s\n",
		fill.ID, buyOrder.ID, ladderOrder.ID, levelIndex, amountBase.String())

	input, err := e.abi.Pack("matchOrderWithLadder",
		buildOrderStruct(buyOrder),
		hexDecodeSignature(buyOrder.Signature),
		ladderAuth,
		hexDecodeSignature(ladderOrder.Signature),
		big.NewInt(int64(levelIndex)),
		amountBase,
	)
	if err != nil {
		return fmt.Errorf("failed to pack matchOrderWithLadder: %w", err)
	}

	nonce, err := e.getPendingNonce(ctx, client, executorAddr)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %w", err)
	}

	tx := types.NewTransaction(nonce, settlement, big.NewInt(0), DefaultGasLimit, gasPrice, input)
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), e.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %w", err)
	}

	err = client.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send transaction: %w", err)
	}

	// Set tx_hash immediately and broadcast notification right away for ladder buy orders
	fill.TxHash = signedTx.Hash().Hex()
	fill.Status = "pending" // Mark as pending until confirmed

	// Update fill in database immediately with tx_hash
	if err := e.fillRepo.Update(ctx, fill); err != nil {
		return fmt.Errorf("failed to update ladder buy fill with tx_hash: %w", err)
	}

	// Broadcast immediate notification to frontend with tx_hash
	fmt.Printf("[Executor] Sent ladder buy fill %d transaction: %s - broadcasting immediate notification\n", fill.ID, fill.TxHash)
	e.broadcastTradeUpdate(fill)

	// Now wait for on-chain confirmation
	receipt, err := e.waitForReceipt(ctx, client, signedTx.Hash())
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	if receipt.Status == 0 {
		return fmt.Errorf("transaction reverted")
	}

	fmt.Printf("[Executor] Confirmed ladder buy fill %d on-chain, tx: %s\n", fill.ID, signedTx.Hash().Hex())

	// Update with final confirmation details
	fill.BlockNumber = receipt.BlockNumber.Uint64()
	fill.GasUsed = receipt.GasUsed
	fill.Status = "settled"

	if err := e.fillRepo.Update(ctx, fill); err != nil {
		return fmt.Errorf("failed to update ladder buy fill confirmation: %w", err)
	}

	// Update both orders to filled
	if buyOrder.FilledAmount.GreaterThanOrEqual(buyOrder.Amount) {
		buyOrder.Status = models.OrderStatusFilled
	}
	e.orderRepo.Update(ctx, buyOrder)
	if ladderOrder.FilledAmount.GreaterThanOrEqual(ladderOrder.Amount) {
		ladderOrder.Status = models.OrderStatusFilled
	}
	e.orderRepo.Update(ctx, ladderOrder)

	// Broadcast updates to UI after successful settlement
	e.broadcastTradeUpdate(fill)
	e.broadcastTickerUpdate(ctx, fill)

	return nil
}

func getLadderLevelIndex(order *models.Order, fillPrice decimal.Decimal) int {
	if order.LadderLevels <= 1 {
		return 0
	}

	var bestIndex int
	var bestDiff decimal.Decimal
	for i := 0; i < order.LadderLevels; i++ {
		levelPrice := order.LadderPriceStart
		if order.LadderLevels > 1 {
			levelPrice = order.LadderPriceStart.Add(
				order.LadderPriceEnd.Sub(order.LadderPriceStart).Mul(decimal.NewFromInt(int64(i))).Div(decimal.NewFromInt(int64(order.LadderLevels - 1))),
			)
		}

		diff := fillPrice.Sub(levelPrice).Abs()
		if i == 0 || diff.LessThan(bestDiff) {
			bestDiff = diff
			bestIndex = i
		}
	}

	return bestIndex
}

func buildLadderAuthStruct(order *models.Order) interface{} {
	priceScale := decimal.NewFromInt(100000000)
	totalAmount := order.AmountIn
	if order.IsLadder && order.LadderTotalAmountIn.GreaterThan(decimal.Zero) {
		totalAmount = order.LadderTotalAmountIn
	}
	return struct {
		Maker       common.Address
		TokenIn     common.Address
		TokenOut    common.Address
		TotalAmount *big.Int
		PriceStart  *big.Int
		PriceEnd    *big.Int
		Levels      *big.Int
		Expiration  *big.Int
		Nonce       *big.Int
		Salt        *big.Int
	}{
		Maker:       common.HexToAddress(order.Maker),
		TokenIn:     common.HexToAddress(order.TokenIn),
		TokenOut:    common.HexToAddress(order.TokenOut),
		TotalAmount: totalAmount.BigInt(),
		PriceStart:  order.LadderPriceStart.Mul(priceScale).BigInt(),
		PriceEnd:    order.LadderPriceEnd.Mul(priceScale).BigInt(),
		Levels:      big.NewInt(int64(order.LadderLevels)),
		Expiration:  big.NewInt(order.Expiration.Unix()),
		Nonce:       new(big.Int).SetUint64(order.Nonce),
		Salt:        new(big.Int).SetUint64(order.Salt),
	}
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "insufficient") ||
		strings.Contains(errStr, "nonce") ||
		strings.Contains(errStr, "underpriced") ||
		strings.Contains(errStr, "network")
}

// checkExecutorBalance logs a warning when the executor wallet is running low on native gas token.
// Thresholds: BSC < 0.01 BNB (1e16 wei) = WARNING, < 0.002 BNB (2e15 wei) = CRITICAL
func (e *Executor) checkExecutorBalance(ctx context.Context) {
	checkNetwork := func(client *ethclient.Client, network string, warnThreshold, critThreshold *big.Int) {
		bal, err := client.BalanceAt(ctx, e.executorAddr, nil)
		if err != nil {
			fmt.Printf("[Executor][Balance] failed to check %s executor balance: %v\n", network, err)
			return
		}
		switch {
		case bal.Cmp(critThreshold) < 0:
			fmt.Printf("[Executor][Balance] ⚠️  CRITICAL: %s executor wallet %s has only %s wei — fills WILL fail. Top up immediately.\n",
				network, e.executorAddr.Hex(), bal.String())
		case bal.Cmp(warnThreshold) < 0:
			fmt.Printf("[Executor][Balance] ⚠️  WARNING: %s executor wallet %s balance is low (%s wei). Top up soon.\n",
				network, e.executorAddr.Hex(), bal.String())
		}
	}

	// BSC: warn < 0.001 BNB, critical < 0.0002 BNB (at 0.05 Gwei each tx costs ~0.000008 BNB)
	checkNetwork(e.bscClient, "BSC",
		big.NewInt(1_000_000_000_000_000),   // 0.001 BNB  (~120 txs)
		big.NewInt(200_000_000_000_000),     // 0.0002 BNB (~24 txs)
	)
	// Base: warn < 0.0005 ETH, critical < 0.0001 ETH
	checkNetwork(e.baseClient, "Base",
		big.NewInt(500_000_000_000_000),     // 0.0005 ETH
		big.NewInt(100_000_000_000_000),     // 0.0001 ETH
	)
}

// estimateGasWithFallback estimates the gas needed for a transaction.
// It adds a 20% buffer on top of the estimate and caps at DefaultGasLimit.
// If estimation fails it returns DefaultGasLimit so the tx still goes through.
func (e *Executor) estimateGasWithFallback(ctx context.Context, client *ethclient.Client, from, to common.Address, input []byte) uint64 {
	msg := ethereum.CallMsg{
		From: from,
		To:   &to,
		Data: input,
	}
	estimated, err := client.EstimateGas(ctx, msg)
	if err != nil {
		fmt.Printf("[Executor] gas estimation failed, using default %d: %v\n", DefaultGasLimit, err)
		return DefaultGasLimit
	}
	// Add 20% buffer
	withBuffer := estimated + estimated/5
	if withBuffer > DefaultGasLimit {
		withBuffer = DefaultGasLimit
	}
	fmt.Printf("[Executor] gas estimated=%d buffered=%d\n", estimated, withBuffer)
	return withBuffer
}

func (e *Executor) getNetworkConfig(network models.Network) (*ethclient.Client, *big.Int, common.Address, *big.Int, common.Address) {
	if network == models.NetworkBase {
		// Base floor: 0.001 Gwei = 1,000,000 wei
		gasPrice := e.getFreshGasPrice(context.Background(), e.baseClient, big.NewInt(1_000_000))
		return e.baseClient, gasPrice, e.settlementBase, big.NewInt(e.config.ChainIDBase), e.executorAddr
	}
	// BSC floor: 0.05 Gwei = 50,000,000 wei (actual BSC minimum)
	gasPrice := e.getFreshGasPrice(context.Background(), e.bscClient, big.NewInt(50_000_000))
	return e.bscClient, gasPrice, e.settlementBSC, big.NewInt(e.config.ChainID), e.executorAddr
}

// getFreshGasPrice fetches the current suggested gas price from the RPC and applies a minimum floor.
// This is called per-transaction so the executor never uses a stale gas price from startup.
// BSC floor: 0.05 Gwei (actual BSC minimum base fee)
// Base floor: 0.001 Gwei
func (e *Executor) getFreshGasPrice(ctx context.Context, client *ethclient.Client, floor *big.Int) *big.Int {
	suggested, err := client.SuggestGasPrice(ctx)
	if err != nil || suggested == nil || suggested.Sign() == 0 {
		fmt.Printf("[Executor] gas price fetch failed (%v), using floor %s\n", err, floor.String())
		return new(big.Int).Set(floor)
	}
	// Add a small 10% tip to improve inclusion without overpaying
	tipped := new(big.Int).Add(suggested, new(big.Int).Div(suggested, big.NewInt(10)))
	if tipped.Cmp(floor) < 0 {
		tipped = new(big.Int).Set(floor)
	}
	fmt.Printf("[Executor] gas price: suggested=%s tipped=%s\n", suggested.String(), tipped.String())
	return tipped
}

// convertFromWei converts a decimal value from raw token format (with decimals) to human-readable
func (e *Executor) convertFromWei(amount decimal.Decimal, decimals int) decimal.Decimal {
	if amount.IsZero() {
		return decimal.Zero
	}
	if decimals == 0 {
		return amount
	}
	divisor := decimal.NewFromFloat(math.Pow10(decimals))
	return amount.Div(divisor)
}

// broadcastTradeUpdate sends a trade update via websocket after settlement
func (e *Executor) broadcastTradeUpdate(fill *models.Fill) {
	// Get the maker order to get the correct decimals
	order, err := e.orderRepo.GetByID(context.Background(), fill.MakerOrderID)
	if err != nil {
		log.Printf("Failed to get order for fill broadcast: %v", err)
		return
	}

	var amountDecimals int
	if order.Side == models.OrderSideBuy {
		amountDecimals = int(order.AmountOutDecimals)
	} else {
		amountDecimals = int(order.AmountInDecimals)
	}

	// Convert amounts to human-readable format using same logic as handlers
	amountHuman := e.convertFromWei(fill.Amount, amountDecimals)
	priceHuman := fill.Price

	// Format amount human string with same logic as convertWeiToHuman
	amountHumanStr := amountHuman.String()
	if strings.Contains(amountHumanStr, ".") {
		// Round to 6 decimal places
		rounded := amountHuman.Round(6)
		amountHumanStr = rounded.String()
		// Remove trailing zeros after decimal point
		amountHumanStr = strings.TrimRight(amountHumanStr, "0")
		amountHumanStr = strings.TrimRight(amountHumanStr, ".")
	}

	// Format price human string
	priceHumanStr := priceHuman.String()
	if strings.Contains(priceHumanStr, ".") {
		priceHumanStr = strings.TrimRight(priceHumanStr, "0")
		priceHumanStr = strings.TrimRight(priceHumanStr, ".")
	}

	trade := websocket.TradeUpdate{
		ID:           int64(fill.ID),
		Price:        fill.Price.String(),
		PriceHuman:   priceHumanStr,
		Amount:       fill.Amount.String(),
		AmountHuman:  amountHumanStr,
		Side:         string(fill.Side),
		Time:         fill.CreatedAt.UnixMilli(),
		TxHash:       fill.TxHash,
		TxHashBuy:    fill.TxHashBuy,
		TxHashSell:   fill.TxHashSell,
		Decimals:     amountDecimals,
		OrderID:      int64(fill.MakerOrderID),
		TakerOrderID: int64(fill.TakerOrderID),
	}
	log.Printf("[Executor] broadcasting trade update pair=%s tradeID=%d price=%s amount=%s side=%s", fill.PairID, fill.ID, trade.Price, trade.Amount, trade.Side)
	e.hub.BroadcastTradeUpdate(fill.PairID, trade)

	// Also broadcast order update for both maker and taker orders
	e.broadcastOrderUpdate(fill.MakerOrderID, fill.PairID)
	e.broadcastOrderUpdate(fill.TakerOrderID, fill.PairID)
}

// broadcastOrderUpdate sends an order update via websocket so frontend can update filled amounts
func (e *Executor) broadcastOrderUpdate(orderID uint, pairID string) {
	order, err := e.orderRepo.GetByID(context.Background(), orderID)
	if err != nil {
		return
	}

	msg := websocket.Message{
		Type:   "order_update",
		PairID: pairID,
		Payload: map[string]interface{}{
			"id":             order.ID,
			"filled_amount":  order.FilledAmount.String(),
			"status":         string(order.Status),
			"amount":         order.Amount.String(),
			"amount_in":      order.AmountIn.String(),
			"amount_out_min": order.AmountOutMin.String(),
		},
	}

	data, _ := json.Marshal(msg)
	e.hub.BroadcastToPair(pairID, data)
}

// broadcastTickerUpdate recomputes and broadcasts ticker stats after a fill is settled
func (e *Executor) broadcastTickerUpdate(ctx context.Context, fill *models.Fill) {
	stats, err := e.pairRepo.GetStats(ctx, fill.PairID)
	if err != nil {
		log.Printf("[Executor] failed to get stats for pair %s: %v", fill.PairID, err)
		return
	}

	pair, err := e.pairRepo.GetByID(ctx, fill.PairID)
	if err != nil {
		log.Printf("[Executor] failed to get pair %s: %v", fill.PairID, err)
		return
	}

	// Parse quote token info
	quoteTokenSymbol := ""
	quoteTokenDecimals := 18
	var quoteTokenData map[string]interface{}
	if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenData) == nil {
		if symbol, ok := quoteTokenData["symbol"].(string); ok {
			quoteTokenSymbol = symbol
		}
		if decimalsVal, ok := quoteTokenData["decimals"]; ok {
			switch v := decimalsVal.(type) {
			case float64:
				quoteTokenDecimals = int(v)
			case int:
				quoteTokenDecimals = v
			}
		}
	}

	normalizedVolume := e.convertFromWei(stats.Volume24h, quoteTokenDecimals)

	// Compute liquidity from orderbook
	ob, err := e.orderRepo.GetOrderBook(ctx, fill.PairID, fill.Network, 100)
	liquidity := decimal.Zero
	if err == nil {
		for _, level := range ob.Asks {
			liquidity = liquidity.Add(level.Amount.Mul(level.Price))
		}
		for _, level := range ob.Bids {
			liquidity = liquidity.Add(level.Amount.Mul(level.Price))
		}
	}
	normalizedLiquidity := e.convertFromWei(liquidity, quoteTokenDecimals)

	var volumeUSD, liquidityUSD, priceUSD decimal.Decimal
	if e.ethService != nil && quoteTokenSymbol != "" {
		usdPrice, err := e.ethService.GetTokenUSDPrice(ctx, string(fill.Network), pair.QuoteToken, quoteTokenSymbol)
		if err == nil {
			// Quote token USD price must be converted through the pair price to get the base token USD price
			priceUSD = stats.Price.Mul(usdPrice)
			volumeUSD = normalizedVolume.Mul(usdPrice)
			liquidityUSD = normalizedLiquidity.Mul(usdPrice)
		}
	}

	log.Printf("[Executor] broadcasting ticker update pair=%s price=%s volume24h=%s liquidity=%s", fill.PairID, stats.Price.String(), normalizedVolume.String(), normalizedLiquidity.String())
	e.hub.BroadcastTickerUpdate(websocket.TickerUpdate{
		PairID:         fill.PairID,
		LastPrice:      stats.Price.String(),
		PriceChange24h: stats.PriceChange24h.String(),
		Volume24h:      normalizedVolume.String(),
		Volume24hUSD:   volumeUSD.String(),
		PriceUSD:       priceUSD.String(),
		PriceHigh24h:   stats.PriceHigh24h.String(),
		PriceLow24h:    stats.PriceLow24h.String(),
		Liquidity:      normalizedLiquidity.String(),
		LiquidityUSD:   liquidityUSD.String(),
	})

	// Also broadcast dedicated liquidity update for real-time updates
	log.Printf("[Executor] broadcasting liquidity update pair=%s liquidity=%s liquidityUSD=%s", fill.PairID, normalizedLiquidity.String(), liquidityUSD.String())
	e.hub.BroadcastLiquidityUpdate(fill.PairID, normalizedLiquidity.String(), liquidityUSD.String())
}
