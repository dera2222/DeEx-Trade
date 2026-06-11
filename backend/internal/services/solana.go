package services

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/models"
	"github.com/nexus/backend/internal/repository"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/gagliardetto/solana-go/programs/token"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/shopspring/decimal"
)

const solanaMemoProgramID = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"

// SolanaError represents different types of Solana-related errors
type SolanaError struct {
	Type    string
	Message string
	Details interface{}
}

func (e *SolanaError) Error() string {
	return fmt.Sprintf("[SolanaSettlement] %s: %s", e.Type, e.Message)
}

// Error types
const (
	ErrorBlockhashExpired    = "BlockhashExpired"
	ErrorRPCFailure          = "RPCFailure"
	ErrorTransactionFailed   = "TransactionFailed"
	ErrorInvalidBlockhash    = "InvalidBlockhash"
	ErrorNetworkTimeout      = "NetworkTimeout"
	ErrorInsufficientFunds  = "InsufficientFunds"
)

// LogTransaction logs transaction details with structured logging
func (s *SolanaSettlementService) LogTransaction(ctx context.Context, operation string, details map[string]interface{}) {
	logData := map[string]interface{}{
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"operation":   operation,
		"network":     "solana",
	}
	
	for k, v := range details {
		logData[k] = v
	}
	
	// In a real implementation, you would use a proper logging library
	// For now, we'll use structured printf
	fmt.Printf("[SolanaSettlement] Transaction Log: %+v\n", logData)
}

// LogError logs errors with structured information
func (s *SolanaSettlementService) LogError(ctx context.Context, operation string, err error, details map[string]interface{}) {
	errorData := map[string]interface{}{
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"operation":   operation,
		"error":       err.Error(),
		"network":     "solana",
	}
	
	for k, v := range details {
		errorData[k] = v
	}
	
	// In a real implementation, you would use a proper logging library
	// For now, we'll use structured printf
	fmt.Printf("[SolanaSettlement] Error Log: %+v\n", errorData)
}

// IsBlockhashError checks if an error is related to blockhash issues
func (s *SolanaSettlementService) IsBlockhashError(err error) bool {
	return strings.Contains(err.Error(), "Blockhash not found") ||
		   strings.Contains(err.Error(), "BlockhashExpired") ||
		   strings.Contains(err.Error(), "invalid blockhash")
}

// IsRPCTimeout checks if an error is related to RPC timeout
func (s *SolanaSettlementService) IsRPCTimeout(err error) bool {
	return strings.Contains(err.Error(), "timeout") ||
		   strings.Contains(err.Error(), "context deadline exceeded")
}

// ShouldRetry determines if a transaction should be retried based on the error
func (s *SolanaSettlementService) ShouldRetry(err error, attempt int, maxRetries int) bool {
	if attempt >= maxRetries-1 {
		return false
	}
	
	// Always retry blockhash errors
	if s.IsBlockhashError(err) {
		return true
	}
	
	// Retry RPC timeouts
	if s.IsRPCTimeout(err) {
		return true
	}
	
	// Don't retry certain fatal errors
	if strings.Contains(err.Error(), "insufficient funds") ||
	   strings.Contains(err.Error(), "invalid account") {
		return false
	}
	
	return true
}

// Solana token mint addresses
const (
	NATIVE_SOL_MINT             = "11111111111111111111111111111111112"
	WRAPPED_SOL_MINT            = "So11111111111111111111111111111111111111112"
	SPL_TOKEN_PROGRAM_ID        = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvR6K6XQ4Ftr8A3HYVbGQGFWNky6fi3FDDKP"
	SYSTEM_PROGRAM_ID           = "11111111111111111111111111111111"
	SYSVAR_RENT_PUBKEY          = "SysvarRent111111111111111111111111111111111"
)

// rpcRequest is the generic JSON-RPC request envelope.
type rpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Error   *rpcError       `json:"error,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type signatureInfo struct {
	Signature string      `json:"signature"`
	Slot      uint64      `json:"slot"`
	Err       interface{} `json:"err"`
}

type rpcTransaction struct {
	Transaction rpcParsedTransaction `json:"transaction"`
	Meta        *rpcTransactionMeta  `json:"meta"`
}

type rpcParsedTransaction struct {
	Message rpcParsedMessage `json:"message"`
}

type rpcParsedMessage struct {
	AccountKeys  []rpcAccountKey  `json:"accountKeys"`
	Instructions []rpcInstruction `json:"instructions"`
}

type rpcAccountKey struct {
	Pubkey   string `json:"pubkey"`
	Signer   bool   `json:"signer"`
	Writable bool   `json:"writable"`
}

type rpcInstruction struct {
	Program   string          `json:"program"`
	ProgramID string          `json:"programId"`
	Parsed    json.RawMessage `json:"parsed,omitempty"`
	Data      string          `json:"data,omitempty"`
}

type rpcTransactionMeta struct {
	Err               interface{}           `json:"err"`
	PreBalances       []uint64              `json:"preBalances"`
	PostBalances      []uint64              `json:"postBalances"`
	PreTokenBalances  []rpcTokenBalance     `json:"preTokenBalances"`
	PostTokenBalances []rpcTokenBalance     `json:"postTokenBalances"`
	InnerInstructions []rpcInnerInstruction `json:"innerInstructions"`
	LogMessages       []string              `json:"logMessages,omitempty"`
}

type rpcInnerInstruction struct {
	Index        int              `json:"index"`
	Instructions []rpcInstruction `json:"instructions"`
}

type rpcTokenBalance struct {
	AccountIndex  int            `json:"accountIndex"`
	Mint          string         `json:"mint"`
	UiTokenAmount rpcTokenAmount `json:"uiTokenAmount"`
}

type rpcTokenAmount struct {
	Amount         string  `json:"amount"`
	Decimals       int     `json:"decimals"`
	UiAmount       float64 `json:"uiAmount"`
	UiAmountString string  `json:"uiAmountString"`
}

type memoParsed struct {
	Type string   `json:"type"`
	Info memoInfo `json:"info"`
}

type memoInfo struct {
	Memo string `json:"memo"`
}

// SolanaService watches Solana custody deposits and credits the internal ledger.
type SolanaService struct {
	cfg         *config.Config
	client      *http.Client
	depositRepo *repository.DepositRepository
	userRepo    *repository.UserRepository
	balanceRepo *repository.BalanceRepository
}

func NewSolanaService(cfg *config.Config, depositRepo *repository.DepositRepository, userRepo *repository.UserRepository, balanceRepo *repository.BalanceRepository) *SolanaService {
	return &SolanaService{
		cfg:         cfg,
		client:      &http.Client{Timeout: 15 * time.Second},
		depositRepo: depositRepo,
		userRepo:    userRepo,
		balanceRepo: balanceRepo,
	}
}

func (s *SolanaService) StartDepositWatcher(ctx context.Context, interval time.Duration) {
	if s.cfg.SolanaRPCURL == "" || s.cfg.SolanaCustodyAddr == "" {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.pollCustodyDeposits(ctx); err != nil {
				fmt.Printf("[SolanaService] deposit watcher error: %v\n", err)
			}
		}
	}
}

func (s *SolanaService) pollCustodyDeposits(ctx context.Context) error {
	watchAddresses, err := s.getWatchAddresses(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("[SolanaService] polling watch addresses: %v\n", watchAddresses)

	for _, watchAddress := range watchAddresses {
		addresses := []interface{}{watchAddress, map[string]interface{}{"limit": 50}}
		sigResp, err := s.callRPC(ctx, "getSignaturesForAddress", addresses)
		if err != nil {
			return fmt.Errorf("failed to get signatures for address %s: %w", watchAddress, err)
		}

		var signatures []signatureInfo
		if err := json.Unmarshal(sigResp.Result, &signatures); err != nil {
			return fmt.Errorf("failed to decode signatures response for address %s: %w", watchAddress, err)
		}

		for _, sig := range signatures {
			if sig.Err != nil {
				fmt.Printf("[SolanaService] skipped signature %s due rpc error\n", sig.Signature)
				continue
			}

			processed, err := s.depositRepo.ExistsByTxHash(ctx, sig.Signature)
			if err != nil {
				return fmt.Errorf("failed to check tx hash: %w", err)
			}
			if processed {
				fmt.Printf("[SolanaService] skipped signature %s because tx_hash already exists\n", sig.Signature)
				continue
			}

			fmt.Printf("[SolanaService] processing tx %s for watch address %s\n", sig.Signature, watchAddress)
			deposit, err := s.processTransaction(ctx, sig.Signature)
			if err != nil {
				fmt.Printf("[SolanaService] transaction processing error %s: %v\n", sig.Signature, err)
				continue
			}
			if deposit == nil {
				continue
			}

			if err := s.creditDeposit(ctx, deposit); err != nil {
				fmt.Printf("[SolanaService] failed to credit deposit: %v\n", err)
			}
		}
	}

	return nil
}

func (s *SolanaService) getWatchAddresses(ctx context.Context) ([]string, error) {
	addresses := []string{s.cfg.SolanaCustodyAddr}
	tokenAccounts, err := s.getCustodyTokenAccounts(ctx)
	if err != nil {
		fmt.Printf("[SolanaService] warning: failed to fetch custody token accounts: %v\n", err)
	} else {
		addresses = append(addresses, tokenAccounts...)
	}

	seen := make(map[string]struct{}, len(addresses))
	unique := make([]string, 0, len(addresses))
	for _, address := range addresses {
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		unique = append(unique, address)
	}

	return unique, nil
}

func (s *SolanaService) getCustodyTokenAccounts(ctx context.Context) ([]string, error) {
	params := []interface{}{
		s.cfg.SolanaCustodyAddr,
		map[string]string{"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
		map[string]string{"encoding": "jsonParsed"},
	}

	resp, err := s.callRPC(ctx, "getTokenAccountsByOwner", params)
	if err != nil {
		return nil, fmt.Errorf("failed to get token accounts by owner: %w", err)
	}

	type tokenAccountItem struct {
		Pubkey string `json:"pubkey"`
	}
	var result struct {
		Value []tokenAccountItem `json:"value"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, fmt.Errorf("failed to decode token accounts response: %w", err)
	}

	accounts := make([]string, 0, len(result.Value))
	for _, item := range result.Value {
		if item.Pubkey != "" {
			accounts = append(accounts, item.Pubkey)
		}
	}

	fmt.Printf("[SolanaService] found %d custody token accounts\n", len(accounts))
	return accounts, nil
}

func (s *SolanaService) processTransaction(ctx context.Context, signature string) (*models.SolanaDeposit, error) {
	params := []interface{}{signature, map[string]interface{}{"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}}
	txResp, err := s.callRPC(ctx, "getTransaction", params)
	if err != nil {
		return nil, err
	}

	var tx rpcTransaction
	if err := json.Unmarshal(txResp.Result, &tx); err != nil {
		return nil, fmt.Errorf("failed to decode transaction details: %w", err)
	}
	if tx.Meta == nil {
		fmt.Printf("[SolanaService] tx %s has no meta\n", signature)
		return nil, nil
	}
	if tx.Meta.Err != nil {
		fmt.Printf("[SolanaService] tx %s has rpc meta error: %v\n", signature, tx.Meta.Err)
		return nil, nil
	}

	memo, err := extractMemo(tx.Transaction.Message.Instructions, tx.Meta.InnerInstructions, tx.Meta.LogMessages)
	if err != nil || memo == "" {
		fmt.Printf("[SolanaService] tx %s skipped: memo not found, instructions=%d, innerInstructions=%d, logMessages=%d\n", signature, len(tx.Transaction.Message.Instructions), len(tx.Meta.InnerInstructions), len(tx.Meta.LogMessages))
		logInstructions(tx.Transaction.Message.Instructions, tx.Meta.InnerInstructions)
		if len(tx.Meta.LogMessages) > 0 {
			logMemoLogs(tx.Meta.LogMessages)
		}
		return nil, nil
	}

	userAddress, err := parseMemoWalletAddress(memo)
	if err != nil {
		fmt.Printf("[SolanaService] tx %s skipped: invalid memo format '%s' (%v)\n", signature, memo, err)
		return nil, nil
	}

	amount, tokenMint, err := findDepositAmountAndToken(tx, s.cfg.SolanaCustodyAddr)
	if err != nil {
		fmt.Printf("[SolanaService] tx %s skipped: unable to determine deposit amount/token (%v)\n", signature, err)
		return nil, nil
	}

	user, err := s.userRepo.GetOrCreate(ctx, userAddress)
	if err != nil {
		return nil, err
	}

	return &models.SolanaDeposit{
		UserID:      user.ID,
		UserAddress: userAddress,
		Network:     models.NetworkSolana,
		TokenMint:   tokenMint,
		Amount:      amount,
		Memo:        memo,
		TxHash:      signature,
		Status:      models.DepositStatusPending,
	}, nil
}

func (s *SolanaService) creditDeposit(ctx context.Context, deposit *models.SolanaDeposit) error {
	if err := s.depositRepo.Create(ctx, deposit); err != nil {
		return fmt.Errorf("failed to create deposit record: %w", err)
	}

	if err := s.balanceRepo.CreditAvailable(ctx, deposit.UserID, deposit.Network, deposit.TokenMint, deposit.Amount); err != nil {
		_ = s.depositRepo.MarkCredited(ctx, deposit.ID, deposit.TxHash, models.DepositStatusFailed)
		return fmt.Errorf("failed to credit user balance: %w", err)
	}

	if err := s.depositRepo.MarkCredited(ctx, deposit.ID, deposit.TxHash, models.DepositStatusCredited); err != nil {
		return fmt.Errorf("failed to mark deposit credited: %w", err)
	}

	return nil
}

func (s *SolanaService) callRPC(ctx context.Context, method string, params []interface{}) (*rpcResponse, error) {
	payload, err := json.Marshal(rpcRequest{
		Jsonrpc: "2.0",
		ID:      1,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.SolanaRPCURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var rpcResp rpcResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("failed to parse rpc response: %w", err)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error: %d %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return &rpcResp, nil
}

func extractMemo(instructions []rpcInstruction, innerInstructions []rpcInnerInstruction, logMessages []string) (string, error) {
	tryExtract := func(inst rpcInstruction) (string, bool) {
		programName := strings.ToLower(strings.TrimSpace(inst.Program))
		if inst.ProgramID != solanaMemoProgramID && !strings.EqualFold(programName, "spl-memo") && !strings.EqualFold(programName, "memo") && !strings.Contains(programName, "memo") {
			return "", false
		}

		if len(inst.Parsed) > 0 {
			var memoData memoParsed
			if err := json.Unmarshal(inst.Parsed, &memoData); err == nil {
				return memoData.Info.Memo, true
			}

			var parsedMap map[string]any
			if err := json.Unmarshal(inst.Parsed, &parsedMap); err == nil {
				if info, ok := parsedMap["info"].(map[string]any); ok {
					if memoValue, ok := info["memo"].(string); ok {
						return memoValue, true
					}
				}
			}
		}

		if inst.Data != "" {
			if strings.HasPrefix(inst.Data, "DEPOSIT-") {
				return inst.Data, true
			}
			decoded, err := base64.StdEncoding.DecodeString(inst.Data)
			if err == nil {
				decodedStr := string(decoded)
				if strings.HasPrefix(decodedStr, "DEPOSIT-") {
					return decodedStr, true
				}
			}
		}

		return "", false
	}

	for _, inst := range instructions {
		if memo, ok := tryExtract(inst); ok {
			return memo, nil
		}
	}

	for _, inner := range innerInstructions {
		for _, inst := range inner.Instructions {
			if memo, ok := tryExtract(inst); ok {
				return memo, nil
			}
		}
	}

	if memo, ok := extractMemoFromLogs(logMessages); ok {
		return memo, nil
	}

	return "", errors.New("memo not found")
}

func extractMemoFromLogs(logMessages []string) (string, bool) {
	for _, log := range logMessages {
		if idx := strings.Index(log, "Memo (len"); idx >= 0 {
			rest := strings.TrimSpace(log[idx:])
			if colon := strings.Index(rest, ":"); colon >= 0 {
				payload := strings.TrimSpace(rest[colon+1:])
				if len(payload) >= 2 {
					if (payload[0] == '"' && payload[len(payload)-1] == '"') || (payload[0] == '\'' && payload[len(payload)-1] == '\'') {
						memo := payload[1 : len(payload)-1]
						if strings.HasPrefix(memo, "DEPOSIT-") {
							return memo, true
						}
					}
				}
			}
		}
	}
	return "", false
}

func logMemoLogs(logMessages []string) {
	for idx, log := range logMessages {
		fmt.Printf("[SolanaService] memo log[%d]: %s\n", idx, log)
	}
}

func logInstructions(instructions []rpcInstruction, innerInstructions []rpcInnerInstruction) {
	for idx, inst := range instructions {
		data := inst.Data
		if len(data) > 100 {
			data = data[:100] + "..."
		}
		fmt.Printf("[SolanaService] instruction[%d]: program=%s programId=%s data=%s parsed=%s\n", idx, inst.Program, inst.ProgramID, data, summarizeParsed(inst.Parsed))
	}
	for idx, inner := range innerInstructions {
		fmt.Printf("[SolanaService] innerInstruction group[%d] count=%d\n", idx, len(inner.Instructions))
		for j, inst := range inner.Instructions {
			data := inst.Data
			if len(data) > 100 {
				data = data[:100] + "..."
			}
			fmt.Printf("[SolanaService] inner[%d][%d]: program=%s programId=%s data=%s parsed=%s\n", idx, j, inst.Program, inst.ProgramID, data, summarizeParsed(inst.Parsed))
		}
	}
}

func summarizeParsed(parsed json.RawMessage) string {
	if len(parsed) == 0 {
		return ""
	}
	if len(parsed) > 100 {
		return string(parsed[:100]) + "..."
	}
	return string(parsed)
}

func parseMemoWalletAddress(memo string) (string, error) {
	if !strings.HasPrefix(memo, "DEPOSIT-") {
		return "", errors.New("invalid deposit memo")
	}
	parts := strings.SplitN(memo, "-", 4)
	if len(parts) < 4 {
		return "", errors.New("invalid deposit memo format")
	}
	wallet := parts[1]
	if wallet == "" {
		return "", errors.New("missing wallet address in memo")
	}
	return wallet, nil
}

func findDepositAmountAndToken(tx rpcTransaction, custodyAddress string) (decimal.Decimal, string, error) {
	if tx.Meta == nil {
		return decimal.Zero, "", errors.New("missing transaction metadata")
	}

	custodyIndex := -1
	for idx, account := range tx.Transaction.Message.AccountKeys {
		if account.Pubkey == custodyAddress {
			custodyIndex = idx
			break
		}
	}

	if custodyIndex >= 0 && len(tx.Meta.PostBalances) == len(tx.Meta.PreBalances) && custodyIndex < len(tx.Meta.PostBalances) {
		nativeDelta := int64(tx.Meta.PostBalances[custodyIndex]) - int64(tx.Meta.PreBalances[custodyIndex])
		if nativeDelta > 0 {
			amount := decimal.NewFromInt(nativeDelta).Div(decimal.NewFromInt(1_000_000_000))
			return amount, NATIVE_SOL_MINT, nil
		}
	}

	for _, postToken := range tx.Meta.PostTokenBalances {
		preAmount := decimal.Zero
		for _, preToken := range tx.Meta.PreTokenBalances {
			if preToken.AccountIndex == postToken.AccountIndex && preToken.Mint == postToken.Mint {
				if preToken.UiTokenAmount.UiAmountString != "" {
					preAmount, _ = decimal.NewFromString(preToken.UiTokenAmount.UiAmountString)
				}
				break
			}
		}

		postAmount := decimal.Zero
		if postToken.UiTokenAmount.UiAmountString != "" {
			postAmount, _ = decimal.NewFromString(postToken.UiTokenAmount.UiAmountString)
		}
		if postAmount.GreaterThan(preAmount) {
			return postAmount.Sub(preAmount), postToken.Mint, nil
		}
	}

	return decimal.Zero, "", errors.New("no positive deposit amount found")
}

// SolanaSettlementService handles settlement of Solana fills by transferring tokens from custody wallet
type SolanaSettlementService struct {
	cfg           *config.Config
	blockhashCache *BlockhashCache
}

// SolanaSettlementResult captures all confirmed transaction signatures for a Solana fill
type SolanaSettlementResult struct {
	TxHashes []string
}

// NewSolanaSettlementService creates a new Solana settlement service
func NewSolanaSettlementService(cfg *config.Config) *SolanaSettlementService {
	return &SolanaSettlementService{
		cfg:           cfg,
		blockhashCache: NewBlockhashCache(30 * time.Second), // Cache blockhashes for 30 seconds with buffer
	}
}

// SettleFill transfers tokens from the custody wallet to the user for a fill
func (s *SolanaSettlementService) SettleFill(ctx context.Context, fill *models.Fill, maker, taker *models.Order) (*SolanaSettlementResult, error) {
	if fill.Network != models.NetworkSolana {
		return nil, fmt.Errorf("settlement service only supports Solana network")
	}

	fmt.Printf("[SolanaSettlement] Processing fill ID=%d amount=%s amountIn=%s amountOut=%s\n",
		fill.ID, fill.Amount.String(), fill.AmountIn.String(), fill.AmountOut.String())

	// Determine which order is buy and which is sell
	var buyOrder, sellOrder *models.Order
	if maker.Side == models.OrderSideBuy {
		buyOrder = maker
		sellOrder = taker
	} else {
		buyOrder = taker
		sellOrder = maker
	}

	// In settlement:
	// - Buyer receives Amount (base token) of buyOrder.tokenOut
	// - Seller receives AmountIn (quote token) of buyOrder.tokenIn
	// Since we use custody model, we transfer from custody wallet to users
	//
	// If the order specified a receiver address, send to that address instead of maker.
	buyRecipient := buyOrder.Maker
	if buyOrder.Receiver != "" {
		buyRecipient = buyOrder.Receiver
		fmt.Printf("[SolanaSettlement] Buy order has custom receiver: %s (maker: %s)\n", buyRecipient, buyOrder.Maker)
	}

	sellRecipient := sellOrder.Maker
	if sellOrder.Receiver != "" {
		sellRecipient = sellOrder.Receiver
		fmt.Printf("[SolanaSettlement] Sell order has custom receiver: %s (maker: %s)\n", sellRecipient, sellOrder.Maker)
	}

	// Transfer base token (Amount) to buyer (or buyer's receiver)
	baseTx, err := s.transferTokenToUser(ctx, buyOrder.TokenOut, fill.Amount, buyRecipient)
	if err != nil {
		return nil, fmt.Errorf("failed to transfer base token to buyer: %w", err)
	}

	// Transfer quote token (AmountIn) to seller (or seller's receiver)
	quoteTx, err := s.transferTokenToUser(ctx, buyOrder.TokenIn, fill.AmountIn, sellRecipient)
	if err != nil {
		return nil, fmt.Errorf("failed to transfer quote token to seller: %w", err)
	}

	fmt.Printf("[SolanaSettlement] Successfully settled fill ID=%d, txs=%s,%s\n", fill.ID, baseTx, quoteTx)
	return &SolanaSettlementResult{TxHashes: []string{baseTx, quoteTx}}, nil
}

// transferTokenToUser transfers tokens from custody wallet to user
func (s *SolanaSettlementService) transferTokenToUser(ctx context.Context, tokenMint string, amount decimal.Decimal, userAddress string) (string, error) {
	fmt.Printf("[SolanaSettlement] Transferring %s of token %s to user %s\n", amount.String(), tokenMint, userAddress)

	if s.cfg.SolanaCustodyPrivateKey == "" {
		return "", fmt.Errorf("SOLANA_CUSTODY_PRIVATE_KEY not configured")
	}

	// Decode custody private key from base58
	custodyPrivateKeyBytes := base58Decode(s.cfg.SolanaCustodyPrivateKey)
	var privateKey ed25519.PrivateKey
	if len(custodyPrivateKeyBytes) == 32 {
		privateKey = ed25519.NewKeyFromSeed(custodyPrivateKeyBytes)
	} else if len(custodyPrivateKeyBytes) == 64 {
		privateKey = ed25519.PrivateKey(custodyPrivateKeyBytes)
	} else {
		return "", fmt.Errorf("invalid private key length: expected 32 or 64, got %d", len(custodyPrivateKeyBytes))
	}

	publicKey := privateKey.Public().(ed25519.PublicKey)
	custodyPubkeyStr := base58Encode(publicKey)

	fmt.Printf("[SolanaSettlement] Custody address: %s\n", custodyPubkeyStr)

	if tokenMint == NATIVE_SOL_MINT || tokenMint == WRAPPED_SOL_MINT {
		// Convert amount to lamports
		solLamports := uint64(amount.IntPart())
		
		// Check custody wallet balance before attempting transfer
		balance, err := s.getSOLBalance(ctx, custodyPubkeyStr)
		if err != nil {
			return "", fmt.Errorf("failed to check custody wallet balance: %w", err)
		}
		
		// Account for transaction fees (estimate 5000 lamports)
		requiredLamports := solLamports + 5000
		if balance < requiredLamports {
			return "", fmt.Errorf("insufficient balance: custody wallet has %d lamports, need %d (including fees)", balance, requiredLamports)
		}

		blockhashData, err := s.getRecentBlockhash(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get recent blockhash: %w", err)
		}

		fmt.Printf("[SolanaSettlement] SOL transfer: %d lamports to %s (valid until block %d)\n", solLamports, userAddress, blockhashData.LastValidBlockHeight)

		txSig, err := s.sendSystemTransfer(ctx, custodyPubkeyStr, userAddress, solLamports, blockhashData.Blockhash, blockhashData.LastValidBlockHeight, privateKey)
		if err != nil {
			return "", fmt.Errorf("failed to send SOL transfer: %w", err)
		}

		fmt.Printf("[SolanaSettlement] SOL transfer confirmed: %s\n", txSig)
		return txSig, nil
	}

	sourceAccount, err := s.getTokenAccountForMint(ctx, custodyPubkeyStr, tokenMint)
	if err != nil {
		return "", fmt.Errorf("failed to find custody token account for mint %s: %w", tokenMint, err)
	}

	destAccount, err := s.getTokenAccountForMint(ctx, userAddress, tokenMint)
	if err != nil {
		return "", fmt.Errorf("failed to find destination token account for user %s mint %s: %w", userAddress, tokenMint, err)
	}

	blockhashData, err := s.getRecentBlockhash(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get recent blockhash: %w", err)
	}

	fmt.Printf("[SolanaSettlement] SPL transfer: token %s, valid until block %d\n", tokenMint, blockhashData.LastValidBlockHeight)
	txSig, err := s.sendSplTransfer(ctx, sourceAccount, destAccount, custodyPubkeyStr, tokenMint, amount, blockhashData.Blockhash, blockhashData.LastValidBlockHeight, privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to send SPL token transfer: %w", err)
	}

	fmt.Printf("[SolanaSettlement] SPL transfer confirmed: %s\n", txSig)
	return txSig, nil
}

// Helper RPC functions
type rpcResponseGetRecentBlockhash struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		Context struct {
			Slot uint64 `json:"slot"`
		} `json:"context"`
		Value struct {
			Blockhash            string `json:"blockhash"`
			LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
			FeeCalculator        struct {
				LamportsPerSignature uint64 `json:"lamportsPerSignature"`
			} `json:"feeCalculator"`
		} `json:"value"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// BlockhashCache manages cached blockhashes with expiration tracking
type BlockhashCache struct {
	mu             sync.RWMutex
	current        *BlockhashWithHeight
	expiresAt      time.Time
	bufferDuration time.Duration
}

// NewBlockhashCache creates a new blockhash cache
func NewBlockhashCache(bufferDuration time.Duration) *BlockhashCache {
	return &BlockhashCache{
		bufferDuration: bufferDuration,
	}
}

// Get returns the current blockhash if valid, otherwise fetches a new one
func (bc *BlockhashCache) Get(ctx context.Context, fetchFunc func() (*BlockhashWithHeight, error)) (*BlockhashWithHeight, error) {
	bc.mu.RLock()
	if bc.current != nil && time.Now().Before(bc.expiresAt) {
		bc.mu.RUnlock()
		return bc.current, nil
	}
	bc.mu.RUnlock()

	// Need to fetch a new blockhash
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Double-check after acquiring write lock
	if bc.current != nil && time.Now().Before(bc.expiresAt) {
		return bc.current, nil
	}

	// Fetch fresh blockhash
	newBlockhash, err := fetchFunc()
	if err != nil {
		return nil, err
	}

	bc.current = newBlockhash
	bc.expiresAt = time.Now().Add(bc.bufferDuration)

	return bc.current, nil
}

// Invalidate clears the cached blockhash, forcing the next Get to fetch a new one
func (bc *BlockhashCache) Invalidate() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.current = nil
	bc.expiresAt = time.Time{}
}

// BlockhashWithHeight contains blockhash and its validity information
type BlockhashWithHeight struct {
	Blockhash            string
	LastValidBlockHeight uint64
	FetchedAt           time.Time
}

// TransactionResult represents the result of a transaction submission
type TransactionResult struct {
	Signature string
	Error     error
}

// submitTransactionWithRetry handles transaction submission with enhanced retry logic
func (s *SolanaSettlementService) submitTransactionWithRetry(ctx context.Context, tx *solana.Transaction, maxRetries int, baseDelay time.Duration) (string, error) {
	client := rpc.New(s.cfg.SolanaRPCURL)
	
	// Use SendTransactionWithOpts to skip preflight and match commitment level.
	// Default SendTransaction uses "finalized" commitment for preflight simulation,
	// but our blockhash is fetched with "confirmed" commitment. A confirmed-but-not-finalized
	// blockhash gets rejected during preflight → "Blockhash not found".
	// Skipping preflight is the standard practice for server-side transaction submission.
	opts := rpc.TransactionOpts{
		SkipPreflight:       true,
		PreflightCommitment: rpc.CommitmentConfirmed,
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		sig, err := client.SendTransactionWithOpts(ctx, tx, opts)
		if err != nil {
			if strings.Contains(err.Error(), "Blockhash not found") {
				fmt.Printf("[SolanaSettlement] Attempt %d: Blockhash not found; will retry with exponential backoff\n", attempt+1)
				if attempt < maxRetries-1 {
					delay := baseDelay * time.Duration(1<<attempt) // Exponential backoff
					fmt.Printf("[SolanaSettlement] Waiting %v before retry...\n", delay)
					time.Sleep(delay)
					continue
				}
			}
			return "", fmt.Errorf("failed to send transaction (attempt %d): %w", attempt+1, err)
		}

		sigStr := sig.String()
		fmt.Printf("[SolanaSettlement] Transaction sent: %s\n", sigStr)

		if err := s.waitForConfirmation(ctx, sigStr); err != nil {
			return "", fmt.Errorf("transaction confirmation failed: %w", err)
		}

		return sigStr, nil
	}

	return "", fmt.Errorf("failed to send transaction after %d attempts", maxRetries)
}

// GetTokenDecimals returns the number of decimals for a given token mint
func (s *SolanaSettlementService) GetTokenDecimals(ctx context.Context, tokenMint string) (int, error) {
	// Native SOL uses 9 decimals
	if tokenMint == NATIVE_SOL_MINT || tokenMint == WRAPPED_SOL_MINT {
		return 9, nil
	}

	// For SPL tokens, query the mint account to get decimals
	params := []interface{}{
		tokenMint,
		map[string]string{"encoding": "jsonParsed"},
	}

	resp, err := s.callRPC(ctx, "getAccountInfo", params)
	if err != nil {
		return 0, fmt.Errorf("failed to get mint account info: %w", err)
	}

	var result struct {
		Value *struct {
			Data struct {
				Parsed struct {
					Info struct {
						Decimals int `json:"decimals"`
					} `json:"info"`
				} `json:"parsed"`
			} `json:"data"`
		} `json:"value"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return 0, fmt.Errorf("failed to decode mint account info: %w", err)
	}

	if result.Value == nil {
		return 0, fmt.Errorf("mint account not found: %s", tokenMint)
	}

	return result.Value.Data.Parsed.Info.Decimals, nil
}

func (s *SolanaSettlementService) getRecentBlockhash(ctx context.Context) (*BlockhashWithHeight, error) {
	return s.blockhashCache.Get(ctx, func() (*BlockhashWithHeight, error) {
		client := &http.Client{Timeout: 15 * time.Second}

		reqBody := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getLatestBlockhash",
			"params":  []interface{}{map[string]string{"commitment": "finalized"}},
		}

		body, _ := json.Marshal(reqBody)
		req, err := http.NewRequestWithContext(ctx, "POST", s.cfg.SolanaRPCURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var result rpcResponseGetRecentBlockhash
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, err
		}

		if result.Error != nil {
			return nil, fmt.Errorf("rpc error: %s", result.Error.Message)
		}

		return &BlockhashWithHeight{
			Blockhash:            result.Result.Value.Blockhash,
			LastValidBlockHeight: result.Result.Value.LastValidBlockHeight,
			FetchedAt:           time.Now(),
		}, nil
	})
}

func (s *SolanaSettlementService) getSOLBalance(ctx context.Context, address string) (uint64, error) {
	client := &http.Client{Timeout: 15 * time.Second}

	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getBalance",
		"params":  []interface{}{address},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.cfg.SolanaRPCURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Jsonrpc string `json:"jsonrpc"`
		Result  struct {
			Context struct {
				Slot uint64 `json:"slot"`
			} `json:"context"`
			Value uint64 `json:"value"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	if result.Error != nil {
		return 0, fmt.Errorf("rpc error: %s", result.Error.Message)
	}

	return result.Result.Value, nil
}

func (s *SolanaSettlementService) sendSystemTransfer(ctx context.Context, fromStr, toStr string, lamports uint64, blockhash string, lastValidBlockHeight uint64, privateKey ed25519.PrivateKey) (string, error) {
	fromPub, err := solana.PublicKeyFromBase58(fromStr)
	if err != nil {
		return "", fmt.Errorf("invalid from address: %w", err)
	}

	toPub, err := solana.PublicKeyFromBase58(toStr)
	if err != nil {
		return "", fmt.Errorf("invalid to address: %w", err)
	}

	recentBlockhash, err := solana.HashFromBase58(blockhash)
	if err != nil {
		return "", fmt.Errorf("invalid recent blockhash: %w", err)
	}

	transfer := system.NewTransferInstructionBuilder().
		SetLamports(lamports).
		SetFundingAccount(fromPub).
		SetRecipientAccount(toPub)

	instruction := transfer.Build()
	tx, err := solana.NewTransaction(
		[]solana.Instruction{instruction},
		recentBlockhash,
		solana.TransactionPayer(fromPub),
	)
	if err != nil {
		return "", fmt.Errorf("failed to build system transfer transaction: %w", err)
	}

	solPrivateKey := solana.PrivateKey(privateKey)
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(fromPub) {
			return &solPrivateKey
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign system transfer transaction: %w", err)
	}

	const maxRetries = 5
	baseDelay := 200 * time.Millisecond
	
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Get fresh blockhash for each attempt to prevent expiration
		if attempt > 0 {
			fmt.Printf("[SolanaSettlement] Attempt %d: refreshing blockhash to prevent expiration\n", attempt+1)
			s.blockhashCache.Invalidate()
			newBlockhashData, err := s.getRecentBlockhash(ctx)
			if err != nil {
				lastErr = fmt.Errorf("failed to refresh blockhash on attempt %d: %w", attempt+1, err)
				continue // Continue to next attempt instead of failing immediately
			}
			
			// Rebuild transaction with new blockhash
			recentBlockhash, err := solana.HashFromBase58(newBlockhashData.Blockhash)
			if err != nil {
				lastErr = fmt.Errorf("invalid refreshed blockhash: %w", err)
				continue
			}
			
			tx, err = solana.NewTransaction(
				[]solana.Instruction{instruction},
				recentBlockhash,
				solana.TransactionPayer(fromPub),
			)
			if err != nil {
				lastErr = fmt.Errorf("failed to rebuild system transfer transaction: %w", err)
				continue
			}
			
			// Resign the transaction
			_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
				if key.Equals(fromPub) {
					return &solPrivateKey
				}
				return nil
			})
			if err != nil {
				lastErr = fmt.Errorf("failed to resign system transfer transaction: %w", err)
				continue
			}
			
			fmt.Printf("[SolanaSettlement] Transaction rebuilt with fresh blockhash valid until block %d\n", newBlockhashData.LastValidBlockHeight)
		}

		sig, err := s.submitTransactionWithRetry(ctx, tx, 1, baseDelay)
		if err == nil {
			return sig, nil
		}
		lastErr = err
		fmt.Printf("[SolanaSettlement] Attempt %d failed: %v\n", attempt+1, err)
	}

	if lastErr != nil {
		return "", fmt.Errorf("failed to send system transfer after %d attempts: %w", maxRetries, lastErr)
	}
	return "", fmt.Errorf("failed to send system transfer after %d attempts: unknown error", maxRetries)
}

func (s *SolanaSettlementService) callRPC(ctx context.Context, method string, params []interface{}) (*rpcResponse, error) {
	payload, err := json.Marshal(rpcRequest{
		Jsonrpc: "2.0",
		ID:      1,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.SolanaRPCURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var rpcResp rpcResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("failed to parse rpc response: %w", err)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error: %d %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return &rpcResp, nil
}

func (s *SolanaSettlementService) getTokenAccountForMint(ctx context.Context, owner string, tokenMint string) (string, error) {
	params := []interface{}{
		owner,
		map[string]string{"mint": tokenMint},
		map[string]string{"encoding": "jsonParsed"},
	}

	resp, err := s.callRPC(ctx, "getTokenAccountsByOwner", params)
	if err != nil {
		return "", err
	}

	type tokenAccountItem struct {
		Pubkey string `json:"pubkey"`
	}
	var result struct {
		Value []tokenAccountItem `json:"value"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return "", err
	}

	if len(result.Value) == 0 {
		return "", fmt.Errorf("no token account found for owner %s mint %s", owner, tokenMint)
	}

	return result.Value[0].Pubkey, nil
}

func (s *SolanaSettlementService) sendSplTransfer(ctx context.Context, sourceAta, destAta, owner, tokenMint string, amount decimal.Decimal, blockhash string, lastValidBlockHeight uint64, privateKey ed25519.PrivateKey) (string, error) {
	sourcePub, err := solana.PublicKeyFromBase58(sourceAta)
	if err != nil {
		return "", fmt.Errorf("invalid source ATA: %w", err)
	}

	destPub, err := solana.PublicKeyFromBase58(destAta)
	if err != nil {
		return "", fmt.Errorf("invalid destination ATA: %w", err)
	}

	ownerPub, err := solana.PublicKeyFromBase58(owner)
	if err != nil {
		return "", fmt.Errorf("invalid owner address: %w", err)
	}

	recentBlockhash, err := solana.HashFromBase58(blockhash)
	if err != nil {
		return "", fmt.Errorf("invalid recent blockhash: %w", err)
	}

	amountU64 := uint64(amount.IntPart())

	transfer := token.NewTransferInstructionBuilder().
		SetAmount(amountU64).
		SetSourceAccount(sourcePub).
		SetDestinationAccount(destPub).
		SetOwnerAccount(ownerPub)

	instruction := transfer.Build()
	tx, err := solana.NewTransaction(
		[]solana.Instruction{instruction},
		recentBlockhash,
		solana.TransactionPayer(ownerPub),
	)
	if err != nil {
		return "", fmt.Errorf("failed to build SPL transfer transaction: %w", err)
	}

	solPrivateKey := solana.PrivateKey(privateKey)
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(ownerPub) {
			return &solPrivateKey
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign SPL transfer transaction: %w", err)
	}

	const maxRetries = 5
	baseDelay := 200 * time.Millisecond
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Get fresh blockhash for each attempt to prevent expiration
		if attempt > 0 {
			fmt.Printf("[SolanaSettlement] Attempt %d: refreshing blockhash to prevent expiration\n", attempt+1)
			s.blockhashCache.Invalidate()
			newBlockhashData, err := s.getRecentBlockhash(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to refresh blockhash on attempt %d: %w", attempt+1, err)
			}
			
			// Rebuild transaction with new blockhash
			recentBlockhash, err := solana.HashFromBase58(newBlockhashData.Blockhash)
			if err != nil {
				return "", fmt.Errorf("invalid refreshed blockhash: %w", err)
			}
			
			tx, err = solana.NewTransaction(
				[]solana.Instruction{instruction},
				recentBlockhash,
				solana.TransactionPayer(ownerPub),
			)
			if err != nil {
				return "", fmt.Errorf("failed to rebuild SPL transfer transaction: %w", err)
			}
			
			// Resign the transaction
			_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
				if key.Equals(ownerPub) {
					return &solPrivateKey
				}
				return nil
			})
			if err != nil {
				return "", fmt.Errorf("failed to resign SPL transfer transaction: %w", err)
			}
			
			fmt.Printf("[SolanaSettlement] Transaction rebuilt with fresh blockhash valid until block %d\n", newBlockhashData.LastValidBlockHeight)
		}

		sig, err := s.submitTransactionWithRetry(ctx, tx, 1, baseDelay)
		if err == nil {
			return sig, nil
		}
	}

	return "", fmt.Errorf("failed to send SPL transfer after %d attempts: %w", maxRetries, err)
}

type rpcResponseGetSignatureStatus struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		Context struct {
			Slot uint64 `json:"slot"`
		} `json:"context"`
		Value []struct {
			Slot               uint64      `json:"slot"`
			Confirmations      uint64      `json:"confirmations"`
			Err                interface{} `json:"err"`
			ConfirmationStatus string      `json:"confirmationStatus"`
		} `json:"value"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (s *SolanaSettlementService) waitForConfirmation(ctx context.Context, txSig string) error {
	client := &http.Client{Timeout: 15 * time.Second}
	deadline := time.Now().Add(30 * time.Second)

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("confirmation timeout")
		}

		reqBody := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "getSignatureStatuses",
			"params":  []interface{}{[]string{txSig}, map[string]interface{}{"searchTransactionHistory": true}},
		}

		body, _ := json.Marshal(reqBody)
		req, err := http.NewRequestWithContext(ctx, "POST", s.cfg.SolanaRPCURL, bytes.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		var result rpcResponseGetSignatureStatus
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		if len(result.Result.Value) > 0 {
			status := result.Result.Value[0]
			if status.Err == nil {
				fmt.Printf("[SolanaSettlement] Transaction confirmed with %d confirmations\n", status.Confirmations)
				return nil
			} else {
				return fmt.Errorf("transaction failed: %v", status.Err)
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// Base58 encoding/decoding helpers
const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

func base58Decode(s string) []byte {
	decoded := big.NewInt(0)
	multi := big.NewInt(1)

	for i := len(s) - 1; i >= 0; i-- {
		digit := big.NewInt(int64(strings.IndexByte(base58Alphabet, s[i])))
		decoded.Add(decoded, new(big.Int).Mul(digit, multi))
		multi.Mul(multi, big.NewInt(58))
	}

	h := "0"
	if decoded.String() != "0" {
		h = fmt.Sprintf("%x", decoded)
		if len(h)%2 == 1 {
			h = "0" + h
		}
	}

	res := make([]byte, 0)
	for i := 0; i < len(h); i += 2 {
		c := h[i : i+2]
		v := 0
		fmt.Sscanf(c, "%x", &v)
		res = append(res, byte(v))
	}

	// Handle leading zeros
	nz := 0
	for i := 0; i < len(s); i++ {
		if s[i] != '1' {
			break
		}
		nz++
	}

	return append(make([]byte, nz), res...)
}

func base58Encode(b []byte) string {
	encoded := big.NewInt(0)
	multi := big.NewInt(1)

	for i := len(b) - 1; i >= 0; i-- {
		encoded.Add(encoded, new(big.Int).Mul(big.NewInt(int64(b[i])), multi))
		multi.Mul(multi, big.NewInt(256))
	}

	encoded_str := ""
	if encoded.String() == "0" {
		encoded_str = "0"
	} else {
		for encoded.Cmp(big.NewInt(0)) > 0 {
			remainder := big.NewInt(0)
			encoded.DivMod(encoded, big.NewInt(58), remainder)
			encoded_str = string(base58Alphabet[remainder.Int64()]) + encoded_str
		}
	}

	nz := 0
	for i := 0; i < len(b); i++ {
		if b[i] != 0 {
			break
		}
		nz++
	}

	return strings.Repeat("1", nz) + encoded_str
}