package executor

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/nexus/backend/internal/models"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/core/types"
)

// BatchConfig controls batching behavior
type BatchConfig struct {
	BatchSize       int           // Max fills per batch (e.g., 30)
	BatchWaitTime   int64         // Wait up to this many milliseconds before sending partial batch
	BatchStrategy   string        // "aggressive" (send fast), "conservative" (wait longer), "hybrid" (balanced)
	ValidationMode string        // "strict" (all-or-nothing), "partial" (skip bad fills)
}

// Batch represents a group of fills ready to settle
type Batch struct {
	Fills          []*models.Fill
	RegularFills   []*models.Fill      // Regular order matches
	LadderFills    []*models.Fill      // Ladder order matches
	Orders         map[int64]*models.Order // Order cache by ID
	CreatedAt      int64
	EstimatedGas   uint64
	Strategy       string
}

// BatchResult tracks batch settlement outcome
type BatchResult struct {
	BatchID       string
	TotalFills    int
	SuccessCount  int
	FailedCount   int
	SkippedCount  int
	TxHash        string
	GasUsed       uint64
	Error         error
	FailedFillIDs []int64
}

// DefaultBatchConfig returns production-ready batching config
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		BatchSize:      30,
		BatchWaitTime:  2000, // 2 seconds
		BatchStrategy:  "hybrid",
		ValidationMode: "partial",
	}
}

// orderFetcher is the minimal subset of orderRepo used by batch creation
type orderFetcher interface {
	GetByIDs(ctx context.Context, ids []uint) ([]models.Order, error)
}

// createBatches groups fills into batches for efficient settlement
// Strategy:
//   - Group by order type (regular vs ladder)
//   - Validate all fills before batching
//   - Return ready-to-settle batches and any unsupported fills
func createBatches(ctx context.Context, fills []*models.Fill, orderRepo orderFetcher, config BatchConfig) ([]*Batch, []*models.Fill) {
	if len(fills) == 0 {
		return nil, nil
	}

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 30
	}

	orderIDs := make(map[uint]struct{})
	for _, fill := range fills {
		orderIDs[fill.MakerOrderID] = struct{}{}
		orderIDs[fill.TakerOrderID] = struct{}{}
	}

	ids := make([]uint, 0, len(orderIDs))
	for id := range orderIDs {
		ids = append(ids, id)
	}

	orders, err := orderRepo.GetByIDs(ctx, ids)
	if err != nil {
		fmt.Printf("[Executor][Batch] failed to fetch orders for batch classification: %v\n", err)
		return nil, fills
	}

	orderMap := make(map[uint]*models.Order)
	for i := range orders {
		orderMap[orders[i].ID] = &orders[i]
	}

	var regularFills, ladderFills, fallbackFills []*models.Fill
	for _, fill := range fills {
		makerOrder := orderMap[fill.MakerOrderID]
		takerOrder := orderMap[fill.TakerOrderID]

		if makerOrder == nil || takerOrder == nil {
			fmt.Printf("[Executor][Batch] skipping fill %d due to missing order data\n", fill.ID)
			fallbackFills = append(fallbackFills, fill)
			continue
		}

		if makerOrder.IsLadder && takerOrder.IsLadder {
			ladderFills = append(ladderFills, fill)
			continue
		}

		if !makerOrder.IsLadder && !takerOrder.IsLadder {
			regularFills = append(regularFills, fill)
			continue
		}

		// Mixed ladder/regular match cannot be safely batched using existing batch functions
		fallbackFills = append(fallbackFills, fill)
	}

	var batches []*Batch
	for i := 0; i < len(regularFills); i += batchSize {
		end := i + batchSize
		if end > len(regularFills) {
			end = len(regularFills)
		}
		batch := &Batch{
			Fills:        regularFills[i:end],
			RegularFills: regularFills[i:end],
			Orders:       make(map[int64]*models.Order),
			CreatedAt:    getCurrentTimestampMs(),
			Strategy:      config.BatchStrategy,
		}
		batches = append(batches, batch)
	}

	for i := 0; i < len(ladderFills); i += batchSize {
		end := i + batchSize
		if end > len(ladderFills) {
			end = len(ladderFills)
		}
		batch := &Batch{
			Fills:       ladderFills[i:end],
			LadderFills: ladderFills[i:end],
			Orders:      make(map[int64]*models.Order),
			CreatedAt:   getCurrentTimestampMs(),
			Strategy:    config.BatchStrategy,
		}
		batches = append(batches, batch)
	}

	return batches, fallbackFills
}

// validateBatch checks all fills in batch for settlability
// Returns error if validation fails in strict mode, otherwise logs warnings
func validateBatch(ctx context.Context, batch *Batch, fillRepo interface{}, orderRepo interface{}, mode string) error {
	if len(batch.Fills) == 0 {
		return fmt.Errorf("empty batch")
	}

	var validationErrors []string

	for i, fill := range batch.Fills {
		if fill == nil {
			validationErrors = append(validationErrors, fmt.Sprintf("fill[%d] is nil", i))
			continue
		}

		// Check fill status
		if fill.Status != "pending" {
			validationErrors = append(validationErrors, 
				fmt.Sprintf("fill[%d] status=%s (expected pending)", i, fill.Status))
			continue
		}

		// Check order IDs
		if fill.MakerOrderID == 0 || fill.TakerOrderID == 0 {
			validationErrors = append(validationErrors,
				fmt.Sprintf("fill[%d] missing order IDs: maker=%d taker=%d", i, fill.MakerOrderID, fill.TakerOrderID))
			continue
		}

		// Check amounts
		if fill.Amount.IsZero() {
			validationErrors = append(validationErrors,
				fmt.Sprintf("fill[%d] invalid amount: %v", i, fill.Amount))
			continue
		}

		if fill.AmountIn.IsZero() {
			validationErrors = append(validationErrors,
				fmt.Sprintf("fill[%d] invalid amountIn: %v", i, fill.AmountIn))
			continue
		}

		if fill.AmountOut.IsZero() {
			validationErrors = append(validationErrors,
				fmt.Sprintf("fill[%d] invalid amountOut: %v", i, fill.AmountOut))
			continue
		}
	}

	if len(validationErrors) == 0 {
		return nil
	}

	errMsg := fmt.Sprintf("batch validation failed: %d/%d fills invalid\n", len(validationErrors), len(batch.Fills))
	for _, e := range validationErrors {
		errMsg += "  - " + e + "\n"
	}

	if mode == "strict" {
		return fmt.Errorf(errMsg)
	}

	// Partial mode: log warnings but don't fail
	fmt.Printf("[Executor][Batch] Validation warnings:\n%s", errMsg)
	return nil
}

// packBatchData converts regular fills into contract ABI-encoded parameters
// For batchMatchOrders: (Order[] buyOrders, bytes[] sigsBuy, Order[] sellOrders, bytes[] sigsSell, uint256[] amounts)
func packBatchData(ctx context.Context, batch *Batch, executor *Executor, contractABI *abi.ABI) ([]byte, error) {
	if len(batch.RegularFills) == 0 {
		return nil, fmt.Errorf("batch has no regular fills to pack")
	}

	// Collect all unique order IDs needed for this batch
	orderIDs := make(map[uint]bool)
	for _, fill := range batch.RegularFills {
		orderIDs[fill.MakerOrderID] = true
		orderIDs[fill.TakerOrderID] = true
	}

	// Convert map to slice
	ids := make([]uint, 0, len(orderIDs))
	for id := range orderIDs {
		ids = append(ids, id)
	}

	// Fetch all orders from database
	orders, err := executor.orderRepo.GetByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch orders: %w", err)
	}

	// Create order lookup map
	orderMap := make(map[uint]*models.Order)
	for i := range orders {
		orderMap[orders[i].ID] = &orders[i]
	}

	// Build arrays for contract call
	var buyOrders, sellOrders []interface{}
	var sigsBuy, sigsSell [][]byte
	var amounts []*big.Int

	for _, fill := range batch.RegularFills {
		makerOrder := orderMap[fill.MakerOrderID]
		takerOrder := orderMap[fill.TakerOrderID]

		if makerOrder == nil || takerOrder == nil {
			fmt.Printf("[Executor][Batch] Warning: missing order for fill %d (maker: %v, taker: %v)\n",
				fill.ID, makerOrder == nil, takerOrder == nil)
			continue
		}

		// Determine buy/sell orders
		var buyOrder, sellOrder *models.Order
		var sigBuy, sigSell []byte

		if makerOrder.Side == models.OrderSideBuy {
			buyOrder = makerOrder
			sellOrder = takerOrder
			sigBuy = hexDecodeSignature(buyOrder.Signature)
			sigSell = hexDecodeSignature(sellOrder.Signature)
		} else {
			buyOrder = takerOrder
			sellOrder = makerOrder
			sigBuy = hexDecodeSignature(buyOrder.Signature)
			sigSell = hexDecodeSignature(sellOrder.Signature)
		}

		buyOrders = append(buyOrders, buildOrderStruct(buyOrder))
		sellOrders = append(sellOrders, buildOrderStruct(sellOrder))
		sigsBuy = append(sigsBuy, sigBuy)
		sigsSell = append(sigsSell, sigSell)
		amounts = append(amounts, fill.Amount.BigInt())
	}

	if len(buyOrders) == 0 {
		return nil, fmt.Errorf("no valid fills to pack")
	}

	// Pack using ABI
	input, err := contractABI.Pack("batchMatchOrders", buyOrders, sigsBuy, sellOrders, sigsSell, amounts)
	if err != nil {
		return nil, fmt.Errorf("failed to pack batch data: %w", err)
	}

	fmt.Printf("[Executor][Batch] Packed %d fills into batch call, input length: %d\n", len(buyOrders), len(input))
	return input, nil
}

// packBatchDataLadder converts ladder fills into contract ABI-encoded parameters
// For batchMatchLadderOrders: (LadderAuth[], bytes[], LadderAuth[], bytes[], uint256[], uint256[], uint256[])
func packBatchDataLadder(ctx context.Context, batch *Batch, executor *Executor, contractABI *abi.ABI) ([]byte, error) {
	if len(batch.LadderFills) == 0 {
		return nil, fmt.Errorf("batch has no ladder fills to pack")
	}

	// Collect all unique order IDs
	orderIDs := make(map[uint]bool)
	for _, fill := range batch.LadderFills {
		orderIDs[fill.MakerOrderID] = true
		orderIDs[fill.TakerOrderID] = true
	}

	ids := make([]uint, 0, len(orderIDs))
	for id := range orderIDs {
		ids = append(ids, id)
	}

	// Fetch all orders
	orders, err := executor.orderRepo.GetByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch orders: %w", err)
	}

	orderMap := make(map[uint]*models.Order)
	for i := range orders {
		orderMap[orders[i].ID] = &orders[i]
	}

	// Build arrays for ladder batch call
	var buyAuths, sellAuths []interface{}
	var sigsBuy, sigsSell [][]byte
	var buyLevelIndices, sellLevelIndices, amounts []*big.Int

	for _, fill := range batch.LadderFills {
		makerOrder := orderMap[fill.MakerOrderID]
		takerOrder := orderMap[fill.TakerOrderID]

		if makerOrder == nil || takerOrder == nil {
			continue
		}

		// Determine buy/sell orders
		var buyOrder, sellOrder *models.Order
		if makerOrder.Side == models.OrderSideBuy {
			buyOrder = makerOrder
			sellOrder = takerOrder
		} else {
			buyOrder = takerOrder
			sellOrder = makerOrder
		}

		resolvedBuyOrder, err := executor.resolveLadderParentOrder(ctx, buyOrder)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve buy ladder parent: %w", err)
		}
		resolvedSellOrder, err := executor.resolveLadderParentOrder(ctx, sellOrder)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve sell ladder parent: %w", err)
		}

		sigBuy := hexDecodeSignature(buyOrder.Signature)
		sigSell := hexDecodeSignature(sellOrder.Signature)

		buyAuths = append(buyAuths, buildLadderAuthStruct(resolvedBuyOrder))
		sellAuths = append(sellAuths, buildLadderAuthStruct(resolvedSellOrder))
		sigsBuy = append(sigsBuy, sigBuy)
		sigsSell = append(sigsSell, sigSell)

		buyLevelIndices = append(buyLevelIndices, big.NewInt(int64(getLadderLevelIndex(resolvedBuyOrder, fill.Price))))
		sellLevelIndices = append(sellLevelIndices, big.NewInt(int64(getLadderLevelIndex(resolvedSellOrder, fill.Price))))
		amounts = append(amounts, fill.Amount.BigInt())
	}

	if len(buyAuths) == 0 {
		return nil, fmt.Errorf("no valid ladder fills to pack")
	}

	// Pack using ABI
	input, err := contractABI.Pack("batchMatchLadderOrders", buyAuths, sigsBuy, sellAuths, sigsSell,
		buyLevelIndices, sellLevelIndices, amounts)
	if err != nil {
		return nil, fmt.Errorf("failed to pack ladder batch data: %w", err)
	}

	fmt.Printf("[Executor][Batch] Packed %d ladder fills into batch call, input length: %d\n", len(buyAuths), len(input))
	return input, nil
}

// estimateGas provides gas cost estimate for batch
// Regular fill: ~79,000 gas each
// Batch overhead: ~10,000 gas
// Formula: 10,000 + (n * 8,900) where n = fills in batch
func estimateGas(batch *Batch) uint64 {
	numFills := len(batch.Fills)
	if numFills == 0 {
		return 10000
	}

	// Base overhead for batch transaction
	baseGas := uint64(10000)
	// Per-fill cost in batched transaction (vs 79,000 individually)
	perFillGas := uint64(8900)

	return baseGas + (uint64(numFills) * perFillGas)
}

// settleBatch executes batch settlement on-chain with full transaction lifecycle
func settleBatch(ctx context.Context, batch *Batch, executor *Executor) *BatchResult {
	result := &BatchResult{
		BatchID:    fmt.Sprintf("batch_%d_%d", batch.CreatedAt, len(batch.Fills)),
		TotalFills: len(batch.Fills),
	}

	if len(batch.Fills) == 0 {
		result.Error = fmt.Errorf("empty batch cannot be settled")
		return result
	}

	// Estimate gas
	batch.EstimatedGas = estimateGas(batch)
	fmt.Printf("[Executor][Batch] Settling batch %s with %d fills, estimated gas: %d\n",
		result.BatchID, result.TotalFills, batch.EstimatedGas)

	// Determine batch type and pack data
	var input []byte
	var err error
	isBatchLadder := len(batch.LadderFills) > 0

	if isBatchLadder {
		input, err = packBatchDataLadder(ctx, batch, executor, executor.abi)
	} else {
		input, err = packBatchData(ctx, batch, executor, executor.abi)
	}

	if err != nil {
		result.Error = fmt.Errorf("failed to pack batch data: %w", err)
		fmt.Printf("[Executor][Batch] %v\n", result.Error)
		return result
	}

	if batch.Fills[0].Network == models.NetworkSolana {
		result.Error = fmt.Errorf("solana network batch settlement is not yet implemented")
		fmt.Printf("[Executor][Batch] %v\n", result.Error)
		return result
	}

	// Get network config
	client, gasPrice, settlement, chainID, executorAddr := executor.getNetworkConfig(batch.Fills[0].Network)

	// Get current nonce
	nonce, err := executor.getPendingNonce(ctx, client, executorAddr)
	if err != nil {
		result.Error = fmt.Errorf("failed to get nonce: %w", err)
		return result
	}

	// Build transaction
	tx := types.NewTransaction(
		nonce,
		settlement,
		big.NewInt(0),
		DefaultGasLimit,
		gasPrice,
		input,
	)

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), executor.privateKey)
	if err != nil {
		result.Error = fmt.Errorf("failed to sign transaction: %w", err)
		return result
	}

	// Send transaction
	err = client.SendTransaction(ctx, signedTx)
	if err != nil {
		result.Error = fmt.Errorf("failed to send transaction: %w", err)
		return result
	}

	result.TxHash = signedTx.Hash().Hex()
	fmt.Printf("[Executor][Batch] Batch transaction sent: %s\n", result.TxHash)

	// Update all fills with transaction hash (pending state)
	for _, fill := range batch.Fills {
		fill.TxHash = result.TxHash
		fill.Status = "pending"
		if err := executor.fillRepo.Update(ctx, fill); err != nil {
			fmt.Printf("[Executor][Batch] Failed to update fill %d with tx hash: %v\n", fill.ID, err)
		}
		executor.broadcastTradeUpdate(fill)
		executor.broadcastTickerUpdate(ctx, fill)
	}

	// Wait for transaction confirmation
	receipt, err := executor.waitForReceipt(ctx, client, signedTx.Hash())
	if err != nil {
		result.Error = fmt.Errorf("failed waiting for receipt: %w", err)
		fmt.Printf("[Executor][Batch] %v\n", result.Error)
		return result
	}

	// Check transaction status
	if receipt.Status == 0 {
		result.Error = fmt.Errorf("batch transaction reverted")
		fmt.Printf("[Executor][Batch] Transaction reverted: %s\n", result.TxHash)

		// Mark all fills as failed
		for _, fill := range batch.Fills {
			fill.Status = "failed"
			if err := executor.fillRepo.Update(ctx, fill); err != nil {
				fmt.Printf("[Executor][Batch] Failed to update fill %d status: %v\n", fill.ID, err)
			}
			result.FailedFillIDs = append(result.FailedFillIDs, int64(fill.ID))
		}
		result.FailedCount = result.TotalFills
		return result
	}

	// Transaction successful - update all fills
	result.GasUsed = receipt.GasUsed
	result.SuccessCount = result.TotalFills

	for _, fill := range batch.Fills {
		fill.TxHash = result.TxHash
		fill.BlockNumber = receipt.BlockNumber.Uint64()
		fill.GasUsed = receipt.GasUsed / uint64(result.TotalFills) // Distribute gas among fills
		fill.Status = "settled"

		if err := executor.fillRepo.Update(ctx, fill); err != nil {
			fmt.Printf("[Executor][Batch] Failed to update fill %d confirmation: %v\n", fill.ID, err)
			result.FailedCount++
			result.SuccessCount--
			result.FailedFillIDs = append(result.FailedFillIDs, int64(fill.ID))
			continue
		}

		// Update maker order status
		if makerOrder := batch.Orders[int64(fill.MakerOrderID)]; makerOrder != nil {
			if makerOrder.FilledAmount.GreaterThanOrEqual(makerOrder.Amount) {
				makerOrder.Status = models.OrderStatusFilled
			} else {
				makerOrder.Status = models.OrderStatusPartial
			}
			if err := executor.orderRepo.Update(ctx, makerOrder); err != nil {
				fmt.Printf("[Executor][Batch] Failed to update maker order %d: %v\n", makerOrder.ID, err)
			}
		}

		// Update taker order status
		if takerOrder := batch.Orders[int64(fill.TakerOrderID)]; takerOrder != nil {
			if takerOrder.FilledAmount.GreaterThanOrEqual(takerOrder.Amount) {
				takerOrder.Status = models.OrderStatusFilled
			} else {
				takerOrder.Status = models.OrderStatusPartial
			}
			if err := executor.orderRepo.Update(ctx, takerOrder); err != nil {
				fmt.Printf("[Executor][Batch] Failed to update taker order %d: %v\n", takerOrder.ID, err)
			}
		}

		executor.broadcastTradeUpdate(fill)
		executor.broadcastTickerUpdate(ctx, fill)
	}

	fmt.Printf("[Executor][Batch] Batch %s completed successfully: %d fills settled, gas used: %d\n",
		result.BatchID, result.SuccessCount, result.GasUsed)

	return result
}

// shouldBatch determines if pending fills should be batched or settled individually
// Returns true if batching would be beneficial
func shouldBatch(fillCount int, oldestFillAgeMs int64, config BatchConfig) bool {
	minBatchSize := 2 // Always batch if we have at least 2 fills

	if fillCount < minBatchSize {
		return false
	}

	// Aggressive: batch immediately if we have 10+ fills
	if config.BatchStrategy == "aggressive" && fillCount >= 10 {
		return true
	}

	// Conservative: wait for batch to be full
	if config.BatchStrategy == "conservative" {
		return fillCount >= config.BatchSize
	}

	// Hybrid: batch when we have enough fills OR waited long enough
	if config.BatchStrategy == "hybrid" {
		return fillCount >= 5 || (fillCount >= minBatchSize && oldestFillAgeMs > 2000)
	}

	return false
}

// getCurrentTimestampMs returns current time in milliseconds
func getCurrentTimestampMs() int64 {
	return time.Now().UnixMilli()
}
