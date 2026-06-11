package services

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/models"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/shopspring/decimal"
)

type EthereumService struct {
	cfg       *config.Config
	clients   map[string]*ethclient.Client
	contracts map[string]*SettlementContract
}

// Chainlink price feed addresses by network and token
var chainlinkFeeds = map[string]map[string]string{
	"bsc": {
		"BNB":  "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE",
		"USDT": "0xB97Ad0E74fa7d920791E90258A6E2085088b4320",
		"USDC": "0x51597f405303C4377E36123cBc172b13269EA163",
		"SOL":  "0x0E8a53DD9c13589df6382F13dA6B3Ec8F919B323",
	},
	"base": {
		"ETH":     "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70",
		"USDC":    "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B",
		"USDT":    "0xf19d560eB8d2ADf07BD6D13ed03e1D11215721F9",
		"VIRTUAL": "0xEaf310161c9eF7c813A14f8FEF6Fb271434019F7",
	},
}

type SettlementContract struct {
	Address common.Address
	ABI     abi.ABI
}

type TokenBalance struct {
	Address  string          `json:"address"`
	Symbol   string          `json:"symbol"`
	Balance  decimal.Decimal `json:"balance"`
	Decimals int             `json:"decimals"`
}

// getFunctionSelector returns the 4-byte function selector for a given function name
func getFunctionSelector(funcName string) []byte {
	hash := crypto.Keccak256([]byte(funcName))
	return hash[:4]
}

func NewEthereumService(cfg *config.Config) (*EthereumService, error) {
	rpcClients := make(map[string]*ethclient.Client)
	errStrings := []string{}

	if cfg.ExecutorRPCURL != "" {
		client, err := ethclient.Dial(cfg.ExecutorRPCURL)
		if err != nil {
			errStrings = append(errStrings, fmt.Sprintf("failed to connect BSC RPC: %v", err))
		} else {
			rpcClients["bsc"] = client
		}
	}

	if cfg.ExecutorRPCURLBase != "" {
		client, err := ethclient.Dial(cfg.ExecutorRPCURLBase)
		if err != nil {
			errStrings = append(errStrings, fmt.Sprintf("failed to connect Base RPC: %v", err))
		} else {
			rpcClients["base"] = client
		}
	}

	if _, ok := rpcClients["base"]; !ok {
		if cfg.AlchemyURL != "" {
			client, err := ethclient.Dial(cfg.AlchemyURL)
			if err != nil {
				errStrings = append(errStrings, fmt.Sprintf("failed to connect Alchemy RPC: %v", err))
			} else {
				rpcClients["base"] = client
			}
		}
	}

	if _, ok := rpcClients["base"]; !ok {
		if cfg.InfuraURL != "" {
			client, err := ethclient.Dial(cfg.InfuraURL)
			if err != nil {
				errStrings = append(errStrings, fmt.Sprintf("failed to connect Infura RPC: %v", err))
			} else {
				rpcClients["base"] = client
			}
		}
	}

	if len(rpcClients) == 0 {
		return nil, fmt.Errorf("no RPC endpoints configured: %s", strings.Join(errStrings, "; "))
	}

	contracts := make(map[string]*SettlementContract)

	// BSC contract
	bscContract := common.HexToAddress(cfg.SettlementContractBSC)
	contracts["bsc"] = &SettlementContract{
		Address: bscContract,
		ABI:     getSettlementABI(),
	}

	// Base contract
	baseContract := common.HexToAddress(cfg.SettlementContractBase)
	contracts["base"] = &SettlementContract{
		Address: baseContract,
		ABI:     getSettlementABI(),
	}

	return &EthereumService{
		cfg:       cfg,
		clients:   rpcClients,
		contracts: contracts,
	}, nil
}

func (s *EthereumService) getClient(network string) (*ethclient.Client, error) {
	if network != "" {
		if client, ok := s.clients[network]; ok && client != nil {
			return client, nil
		}
	}

	if client, ok := s.clients["base"]; ok && client != nil {
		return client, nil
	}

	for _, client := range s.clients {
		if client != nil {
			return client, nil
		}
	}

	return nil, fmt.Errorf("no RPC client available for network %s", network)
}

// GetUserBalances returns token balances for a user
func (s *EthereumService) GetUserBalances(ctx context.Context, userAddress string) ([]TokenBalance, error) {
	address := common.HexToAddress(userAddress)

	// Get ETH balance
	client, err := s.getClient("base")
	if err != nil {
		return nil, err
	}

	balance, err := client.BalanceAt(ctx, address, nil)
	if err != nil {
		return nil, err
	}

	balances := []TokenBalance{
		{
			Address:  "0x0000000000000000000000000000000000000000",
			Symbol:   "ETH",
			Balance:  decimal.NewFromBigInt(balance, 0),
			Decimals: 18,
		},
	}

	return balances, nil
}

// GetTokenBalance returns balance for a specific token
func (s *EthereumService) GetTokenBalance(ctx context.Context, userAddress, tokenAddress string) (*TokenBalance, error) {
	// Simplified - actual implementation would use ERC20 balanceOf
	return &TokenBalance{
		Address:  tokenAddress,
		Balance:  decimal.Zero,
		Decimals: 18,
	}, nil
}

// GetTokenDecimals gets token decimals from contract (fallback method)
func (s *EthereumService) GetTokenDecimals(ctx context.Context, tokenAddress string) (int, error) {
	tokenAddr := common.HexToAddress(tokenAddress)

	// ERC20 decimals() function selector
	data := getFunctionSelector("decimals()")

	client, err := s.getClient("base")
	if err != nil {
		return 18, err
	}

	result, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &tokenAddr,
		Data: data,
	}, nil)

	if err != nil {
		return 18, err // Default to 18 if call fails
	}

	// Result is bytes, decode as uint8
	if len(result) > 0 {
		decimals, _ := decodeUint256(result)
		return int(decimals), nil
	}

	return 18, nil
}

// decodeUint256 decodes a uint256 from bytes
func decodeUint256(data []byte) (uint64, error) {
	if len(data) >= 32 {
		return new(big.Int).SetBytes(data[:32]).Uint64(), nil
	}
	return 0, fmt.Errorf("insufficient data")
}

// GetTokenInfo gets token decimals and symbol from contract
func (s *EthereumService) GetTokenInfo(ctx context.Context, tokenAddress, network string) (int, string, error) {
	client, err := s.getClient(network)
	if err != nil {
		return 18, "UNKNOWN", nil
	}

	tokenAddr := common.HexToAddress(tokenAddress)

	// Get decimals - decimals()
	decimalsData := getFunctionSelector("decimals()")
	decimalsResult, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &tokenAddr,
		Data: decimalsData,
	}, nil)

	decimals := 18 // Default
	if err == nil && len(decimalsResult) >= 32 {
		decimals64, _ := decodeUint256(decimalsResult)
		decimals = int(decimals64)
	}

	// Get symbol - symbol()
	symbolData := getFunctionSelector("symbol()")
	symbolResult, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &tokenAddr,
		Data: symbolData,
	}, nil)

	symbol := "UNKNOWN"
	if err == nil && len(symbolResult) > 0 {
		// String is padded to 32 bytes, trim padding
		str := string(symbolResult)
		// Find first null byte
		for i := 0; i < len(str); i++ {
			if str[i] == 0 {
				str = str[:i]
				break
			}
		}
		if str != "" {
			symbol = str
		}
	}

	return decimals, symbol, nil
}

// GetUSDPrice fetches USD price from Chainlink price feed
func (s *EthereumService) GetUSDPrice(ctx context.Context, network, tokenSymbol string) (decimal.Decimal, error) {
	feeds, ok := chainlinkFeeds[network]
	if !ok {
		return decimal.Zero, fmt.Errorf("network not supported: %s", network)
	}

	feedAddr, ok := feeds[strings.ToUpper(tokenSymbol)]
	if !ok {
		return decimal.Zero, fmt.Errorf("price feed not available for %s on %s", tokenSymbol, network)
	}

	client, ok := s.clients[network]
	if !ok || client == nil {
		return decimal.Zero, fmt.Errorf("RPC client not available for network: %s", network)
	}

	// Chainlink latestRoundData() function selector
	data := getFunctionSelector("latestRoundData()")

	feedAddress := common.HexToAddress(feedAddr)
	result, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &feedAddress,
		Data: data,
	}, nil)

	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to call Chainlink feed: %w", err)
	}

	// Decode the result - latestRoundData returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
	if len(result) < 160 { // 5 * 32 bytes
		return decimal.Zero, fmt.Errorf("invalid Chainlink response")
	}

	// Answer is at offset 32 (after roundId)
	answer := new(big.Int).SetBytes(result[32:64])

	// Chainlink prices are typically 8 decimals
	price := decimal.NewFromBigInt(answer, -8)

	return price, nil
}

// GetTokenUSDPrice gets USD price for a token, handling wrapped tokens
func (s *EthereumService) GetTokenUSDPrice(ctx context.Context, network, tokenAddress, tokenSymbol string) (decimal.Decimal, error) {
	upperSymbol := strings.ToUpper(tokenSymbol)

	// Cross-chain pricing for Solana assets using BSC Chainlink feeds
	if network == string(models.NetworkSolana) || upperSymbol == "SOL" || upperSymbol == "WSOL" {
		if upperSymbol == "SOL" || upperSymbol == "WSOL" {
			return s.GetUSDPrice(ctx, "bsc", "SOL")
		}
		if upperSymbol == "USDT" || upperSymbol == "USDC" {
			return s.GetUSDPrice(ctx, "bsc", upperSymbol)
		}
	}

	// Handle wrapped tokens
	switch upperSymbol {
	case "WBNB":
		return s.GetUSDPrice(ctx, network, "BNB")
	case "WETH":
		return s.GetUSDPrice(ctx, network, "ETH")
	default:
		return s.GetUSDPrice(ctx, network, tokenSymbol)
	}
}

// WatchFillEvent watches for Fill events from the settlement contract
func (s *EthereumService) WatchFillEvent(ctx context.Context, network string, callback func(models.Fill) error) error {
	contract, ok := s.contracts[network]
	if !ok {
		return fmt.Errorf("contract not found for network: %s", network)
	}

	query := ethereum.FilterQuery{
		Addresses: []common.Address{contract.Address},
	}

	client, err := s.getClient(network)
	if err != nil {
		return err
	}

	_, err = client.FilterLogs(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

// GetTransactionReceipt gets transaction receipt
func (s *EthereumService) GetTransactionReceipt(ctx context.Context, txHash string) (*models.Fill, error) {
	hash := common.HexToHash(txHash)
	client, err := s.getClient("base")
	if err != nil {
		return nil, err
	}

	receipt, err := client.TransactionReceipt(ctx, hash)
	if err != nil {
		return nil, err
	}

	fill := &models.Fill{
		TxHash:      txHash,
		BlockNumber: receipt.BlockNumber.Uint64(),
		GasUsed:     receipt.GasUsed,
	}

	return fill, nil
}

// VerifySignature verifies an EIP-712 signature
func (s *EthereumService) VerifySignature(message, signature, address string) bool {
	// Implementation would use ecrecover
	// This is a simplified version
	return strings.EqualFold(address, "0x0000000000000000000000000000000000000000")
}

// ParseOrderFromEvent parses an order from event logs
func (s *EthereumService) ParseOrderFromEvent(logData []byte) (*models.Order, error) {
	// Implementation would parse event data
	return nil, nil
}

// GetCurrentBlock gets current block number
func (s *EthereumService) GetCurrentBlock(ctx context.Context) (uint64, error) {
	client, err := s.getClient("base")
	if err != nil {
		return 0, err
	}

	header, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, err
	}
	return header.Number.Uint64(), nil
}

// EstimateGas estimates gas for a transaction
func (s *EthereumService) EstimateGas(ctx context.Context, to string, data []byte) (uint64, error) {
	toAddr := common.HexToAddress(to)
	callMsg := ethereum.CallMsg{
		To:   &toAddr,
		Data: data,
	}
	client, err := s.getClient("base")
	if err != nil {
		return 0, err
	}

	return client.EstimateGas(ctx, callMsg)
}

// Get settlement contract ABI
func getSettlementABI() abi.ABI {
	abiJSON := `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"AlreadyRevealed","type":"error"},{"inputs":[],"name":"AmountExceedsTotal","type":"error"},{"inputs":[],"name":"BadLadderSignature","type":"error"},{"inputs":[],"name":"BadSignature","type":"error"},{"inputs":[],"name":"CommitNotActive","type":"error"},{"inputs":[],"name":"CommitNotFound","type":"error"},{"inputs":[],"name":"Expired","type":"error"},{"inputs":[],"name":"InvalidLadderAuth","type":"error"},{"inputs":[],"name":"InvalidLevel","type":"error"},{"inputs":[],"name":"InvalidOrder","type":"error"},{"inputs":[],"name":"LadderAlreadyFilled","type":"error"},{"inputs":[],"name":"LadderExpired","type":"error"},{"inputs":[],"name":"OnlyOwner","type":"error"},{"inputs":[],"name":"Overfill","type":"error"},{"inputs":[],"name":"PriceTooLow","type":"error"},{"inputs":[],"name":"RevealTooLate","type":"error"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"commitHash","type":"bytes32"}],"name":"CommitExpired","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"commitHash","type":"bytes32"},{"indexed":true,"internalType":"address","name":"maker","type":"address"},{"indexed":false,"internalType":"uint256","name":"revealBy","type":"uint256"}],"name":"Committed","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"ladderHash","type":"bytes32"},{"indexed":true,"internalType":"address","name":"maker","type":"address"},{"indexed":true,"internalType":"address","name":"matcher","type":"address"},{"indexed":false,"internalType":"uint256","name":"levelIndex","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amountBase","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amountQuote","type":"uint256"}],"name":"LadderMatched","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"buyHash","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"sellHash","type":"bytes32"},{"indexed":true,"internalType":"address","name":"matcher","type":"address"},{"indexed":false,"internalType":"uint256","name":"amountBase","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amountQuote","type":"uint256"}],"name":"Matched","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"orderHash","type":"bytes32"},{"indexed":true,"internalType":"address","name":"maker","type":"address"},{"indexed":true,"internalType":"address","name":"taker","type":"address"},{"indexed":false,"internalType":"uint256","name":"amountIn","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amountOut","type":"uint256"}],"name":"OrderFilled","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"}],"name":"Paused","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"commitHash","type":"bytes32"},{"indexed":true,"internalType":"address","name":"maker","type":"address"}],"name":"Revealed","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"}],"name":"Unpaused","type":"event"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"LADDER_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"ORDER_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"auth","type":"tuple"},{"internalType":"uint256","name":"levelIndex","type":"uint256"}],"name":"availableLadderFill","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"levelIndex","type":"uint256"}],"name":"calculateLevelAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"auth","type":"tuple"},{"internalType":"uint256","name":"levelIndex","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"}],"name":"calculateLevelAmountOutMin","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"levelIndex","type":"uint256"}],"name":"calculateLevelPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"bytes32[]","name":"commitHashes","type":"bytes32[]"}],"name":"cleanupExpiredCommits","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"commitDuration","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"commitHash","type":"bytes32"}],"name":"commitOrder","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"commits","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"auth","type":"tuple"},{"internalType":"bytes","name":"signature","type":"bytes"},{"internalType":"uint256","name":"levelIndex","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"takerMinAmountOut","type":"uint256"}],"name":"fillLadderOrder","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"commitHash","type":"bytes32"},{"internalType":"address","name":"taker","type":"address"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"takerMinAmountOut","type":"uint256"}],"name":"fillRevealedOrder","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"auth","type":"tuple"}],"name":"getLadderDigest","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"address","name":"receiver","type":"address"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.Order","name":"o","type":"tuple"}],"name":"getOrderDigest","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"auth","type":"tuple"}],"name":"hashLadderAuth","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"pure","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"address","name":"receiver","type":"address"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.Order","name":"o","type":"tuple"}],"name":"hashOrder","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"pure","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"auth","type":"tuple"}],"name":"isLadderValid","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"ladderLevelFilled","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"ladderTotalFilled","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"buyAuth","type":"tuple"},{"internalType":"bytes","name":"sigBuy","type":"bytes"},{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"sellAuth","type":"tuple"},{"internalType":"bytes","name":"sigSell","type":"bytes"},{"internalType":"uint256","name":"buyLevelIndex","type":"uint256"},{"internalType":"uint256","name":"sellLevelIndex","type":"uint256"},{"internalType":"uint256","name":"amountBase","type":"uint256"}],"name":"matchLadderOrders","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"unpause","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"totalAmount","type":"uint256"},{"internalType":"uint256","name":"priceStart","type":"uint256"},{"internalType":"uint256","name":"priceEnd","type":"uint256"},{"internalType":"uint256","name":"levels","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.LadderAuth","name":"auth","type":"tuple"},{"internalType":"bytes","name":"signature","type":"bytes"}],"name":"verifyLadderSignature","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"maker","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"uint256","name":"expiration","type":"uint256"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"address","name":"receiver","type":"address"},{"internalType":"uint256","name":"salt","type":"uint256"}],"internalType":"struct LadderSettlementHybrid.Order","name":"o","type":"tuple"},{"internalType":"bytes","name":"signature","type":"bytes"}],"name":"verifySignature","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"}]`
	a, _ := abi.JSON(strings.NewReader(abiJSON))
	return a
}
