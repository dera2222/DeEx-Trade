package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/nexus/backend/internal/cache"
	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/engine"
	"github.com/nexus/backend/internal/models"
	"github.com/nexus/backend/internal/repository"
	"github.com/nexus/backend/internal/services"
	"github.com/nexus/backend/internal/websocket"
	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// convertWeiToHuman converts a decimal value from Wei to human readable format
func convertWeiToHuman(value decimal.Decimal, decimals int) string {
	if decimals == 0 {
		decimals = 18
	}
	divisor := decimal.NewFromFloat(math.Pow10(decimals))
	human := value.Div(divisor)
	// Format to max 6 decimal places, removing trailing zeros
	str := human.String()
	// If contains decimal point, truncate to reasonable precision
	if strings.Contains(str, ".") {
		// Round to 6 decimal places
		rounded := human.Round(6)
		str = rounded.String()
		// Remove trailing zeros after decimal point
		str = strings.TrimRight(str, "0")
		str = strings.TrimRight(str, ".")
	}
	return str
}

// convertFromWei converts a decimal value from raw token format (with decimals) to human-readable
func convertFromWei(amount decimal.Decimal, decimals int) decimal.Decimal {
	if amount.IsZero() {
		return decimal.Zero
	}
	if decimals == 0 {
		return amount
	}
	// Create divisor = 10^decimals
	divisor := decimal.NewFromFloat(math.Pow10(decimals))
	return amount.Div(divisor)
}

func (h *Handler) isCacheEnabled() bool {
	return h.cache != nil && h.cache.IsEnabled()
}

func (h *Handler) buildPairResponseJSON(ctx context.Context, pair *models.Pair) ([]byte, error) {
	resp, err := h.buildPairResponse(ctx, pair)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

func (h *Handler) unmarshalCachedPairResponses(cached []byte) ([]*PairResponse, error) {
	var payloads [][]byte
	if err := json.Unmarshal(cached, &payloads); err != nil {
		return nil, err
	}
	responses := make([]*PairResponse, 0, len(payloads))
	for _, payload := range payloads {
		var resp PairResponse
		if err := json.Unmarshal(payload, &resp); err != nil {
			return nil, err
		}
		responses = append(responses, &resp)
	}
	return responses, nil
}

func (h *Handler) setXCacheHeader(c *gin.Context, status string) {
	c.Header("X-Cache", status)
}

func (h *Handler) StartCacheWorker(ctx context.Context) {
	if h.cache == nil {
		return
	}
	h.cache.Start(ctx, h.buildPairResponseJSON)
}

func (h *Handler) refreshPairCache(ctx context.Context, pairID string) error {
	if h.cache == nil {
		return nil
	}
	return h.cache.RefreshPair(ctx, pairID, h.buildPairResponseJSON)
}

// NOTE: Market cap is now fetched from the Node.js server
// The server syncs pool data from Gecko Terminal API every 5 minutes
// and includes market cap in the pair response. No worker needed here.

// convertOrderToWithPair converts a database Order to OrderWithPair with pair info
func (h *Handler) convertOrderToWithPair(ctx context.Context, order models.Order) models.OrderWithPair {
	// Get pair info - try by ID first, then by token addresses
	var pairInfo *models.PairInfo
	pair, err := h.pairRepo.GetByID(ctx, order.PairID)
	if err != nil || pair == nil {
		// Try by token addresses
		pair, err = h.pairRepo.GetByTokens(ctx, order.TokenIn, order.TokenOut)
	}
	if err == nil && pair != nil {
		baseSymbol := pair.BaseSymbol
		quoteSymbol := pair.QuoteSymbol

		// If symbols are empty, try to extract from JSON in base_token/quote_token
		if baseSymbol == "" {
			var tokenData map[string]interface{}
			if json.Unmarshal([]byte(pair.BaseToken), &tokenData) == nil {
				if s, ok := tokenData["symbol"].(string); ok {
					baseSymbol = s
				}
			}
		}
		if quoteSymbol == "" {
			var tokenData map[string]interface{}
			if json.Unmarshal([]byte(pair.QuoteToken), &tokenData) == nil {
				if s, ok := tokenData["symbol"].(string); ok {
					quoteSymbol = s
				}
			}
		}

		// Extract logos
		baseLogo := ""
		quoteLogo := ""
		var baseTokenData map[string]interface{}
		if json.Unmarshal([]byte(pair.BaseToken), &baseTokenData) == nil {
			if logo, ok := baseTokenData["logo"].(string); ok {
				baseLogo = logo
			}
		}
		var quoteTokenData map[string]interface{}
		if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenData) == nil {
			if logo, ok := quoteTokenData["logo"].(string); ok {
				quoteLogo = logo
			}
		}

		pairInfo = &models.PairInfo{
			ID:          pair.ID,
			BaseSymbol:  baseSymbol,
			QuoteSymbol: quoteSymbol,
			BaseLogo:    baseLogo,
			QuoteLogo:   quoteLogo,
		}
	}

	// Get token info - try to get from tokens table or extract from pair
	tokenInDecimals := order.AmountInDecimals
	tokenInSymbol := ""
	tokenOutDecimals := order.AmountOutDecimals
	tokenOutSymbol := ""
	if tokenInDecimals == 0 {
		tokenInDecimals = 18
	}
	if tokenOutDecimals == 0 {
		tokenOutDecimals = 18
	}

	// If pair exists, extract token symbols based on order side
	// For BUY: token_in = quote (WBNB), token_out = base (Gift)
	// For SELL: token_in = base (Gift), token_out = quote (WBNB)
	if pair != nil {
		var baseTokenData, quoteTokenData map[string]interface{}
		if json.Unmarshal([]byte(pair.BaseToken), &baseTokenData) == nil {
			if s, ok := baseTokenData["symbol"].(string); ok {
				// For SELL orders, base token is token_in
				if order.Side == models.OrderSideSell {
					tokenInSymbol = s
				} else {
					tokenOutSymbol = s
				}
			}
		}
		if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenData) == nil {
			if s, ok := quoteTokenData["symbol"].(string); ok {
				// For BUY orders, quote token is token_in
				if order.Side == models.OrderSideBuy {
					tokenInSymbol = s
				} else {
					tokenOutSymbol = s
				}
			}
		}
	}

	// Try to get token symbols from tokens table (overrides pair data if available)
	if token, err := h.pairRepo.GetTokenByAddress(ctx, order.TokenIn); err == nil && token != nil {
		tokenInSymbol = token.Symbol
		if token.Decimals > 0 {
			tokenInDecimals = token.Decimals
		}
	}
	if token, err := h.pairRepo.GetTokenByAddress(ctx, order.TokenOut); err == nil && token != nil {
		tokenOutSymbol = token.Symbol
		if token.Decimals > 0 {
			tokenOutDecimals = token.Decimals
		}
	}

	tokenInInfo := &models.TokenInfo{
		Symbol:   tokenInSymbol,
		Decimals: tokenInDecimals,
	}
	tokenOutInfo := &models.TokenInfo{
		Symbol:   tokenOutSymbol,
		Decimals: tokenOutDecimals,
	}

	orderDTO := models.OrderDTO{
		ID:                      order.ID,
		OrderHash:               order.OrderHash,
		UserID:                  order.UserID,
		Network:                 string(order.Network),
		PairID:                  order.PairID,
		Side:                    string(order.Side),
		OrderType:               string(order.OrderType),
		Price:                   order.Price.String(),
		Amount:                  order.Amount.String(),
		FilledAmount:            order.FilledAmount.String(),
		AmountIn:                order.AmountIn.String(),
		AmountOutMin:            order.AmountOutMin.String(),
		TokenIn:                 order.TokenIn,
		TokenOut:                order.TokenOut,
		TokenInDecimals:         tokenInDecimals,
		TokenOutDecimals:        tokenOutDecimals,
		Maker:                   order.Maker,
		Nonce:                   order.Nonce,
		Salt:                    order.Salt,
		Status:                  string(order.Status),
		IsLadder:                order.IsLadder,
		TriggerPrice:            order.TriggerPrice.String(),
		IsPostOnly:              order.IsPostOnly,
		ReduceOnly:              order.ReduceOnly,
		TimeInForce:             order.TimeInForce,
		StopLossType:            order.StopLossType,
		Expiration:              order.Expiration.Format(time.RFC3339),
		CreatedAt:               order.CreatedAt.Format(time.RFC3339),
		UpdatedAt:               order.UpdatedAt.Format(time.RFC3339),
		LadderTotalAmountIn:     order.LadderTotalAmountIn.String(),
		LadderTotalAmountOutMin: order.LadderTotalAmountOutMin.String(),
	}

	// Set ladder fields if applicable
	if order.IsLadder {
		if order.LadderLevels > 0 {
			levels := order.LadderLevels
			orderDTO.LadderLevels = &levels
		}
		orderDTO.LadderPriceStart = order.LadderPriceStart.String()
		orderDTO.LadderPriceEnd = order.LadderPriceEnd.String()
		orderDTO.LadderParentID = order.LadderParentID

		// For ladder parent orders, use the total amounts for display instead of zero
		if order.LadderParentID == nil && order.LadderTotalAmountIn.GreaterThan(decimal.Zero) {
			orderDTO.AmountIn = order.LadderTotalAmountIn.String()
			orderDTO.AmountOutMin = order.LadderTotalAmountOutMin.String()
		}
	}

	// Calculate human-readable amounts
	// For ladder parent orders, use the ladder total amounts if available
	var amountInDisplay, amountOutMinDisplay decimal.Decimal
	if order.IsLadder && order.LadderParentID == nil && order.LadderTotalAmountIn.GreaterThan(decimal.Zero) {
		amountInDisplay = order.LadderTotalAmountIn
		amountOutMinDisplay = order.LadderTotalAmountOutMin
	} else {
		amountInDisplay = order.AmountIn
		amountOutMinDisplay = order.AmountOutMin
	}

	return models.OrderWithPair{
		Order:             orderDTO,
		Pair:              pairInfo,
		TokenInInfo:       tokenInInfo,
		TokenOutInfo:      tokenOutInfo,
		AmountInHuman:     convertWeiToHuman(amountInDisplay, tokenInDecimals),
		AmountOutMinHuman: convertWeiToHuman(amountOutMinDisplay, tokenOutDecimals),
	}
}

// convertOrdersToWithPairBatch efficiently converts multiple orders to OrderWithPair by batch loading pairs and tokens
func (h *Handler) convertOrdersToWithPairBatch(ctx context.Context, orders []models.Order) []models.OrderWithPair {
	if len(orders) == 0 {
		return []models.OrderWithPair{}
	}

	// Collect all unique pair IDs and token addresses
	pairIDs := make([]string, 0, len(orders))
	tokenAddresses := make([]string, 0, len(orders)*2)
	pairIDSet := make(map[string]bool)
	tokenAddrSet := make(map[string]bool)

	for _, order := range orders {
		if order.PairID != "" && !pairIDSet[order.PairID] {
			pairIDSet[order.PairID] = true
			pairIDs = append(pairIDs, order.PairID)
		}
		if order.TokenIn != "" && !tokenAddrSet[order.TokenIn] {
			tokenAddrSet[order.TokenIn] = true
			tokenAddresses = append(tokenAddresses, order.TokenIn)
		}
		if order.TokenOut != "" && !tokenAddrSet[order.TokenOut] {
			tokenAddrSet[order.TokenOut] = true
			tokenAddresses = append(tokenAddresses, order.TokenOut)
		}
	}

	// Batch load pairs
	pairs := make(map[string]*models.Pair)
	if len(pairIDs) > 0 {
		loadedPairs, err := h.pairRepo.GetByIDs(ctx, pairIDs)
		if err == nil {
			for i := range loadedPairs {
				pairs[loadedPairs[i].ID] = &loadedPairs[i]
			}
		}
	}

	// Batch load tokens
	tokens := make(map[string]*models.Token)
	if len(tokenAddresses) > 0 {
		loadedTokens, err := h.pairRepo.GetTokensByAddresses(ctx, tokenAddresses)
		if err == nil {
			for i := range loadedTokens {
				tokens[loadedTokens[i].Address] = &loadedTokens[i]
			}
		}
	}

	// Convert each order using preloaded data
	response := make([]models.OrderWithPair, len(orders))
	for i, order := range orders {
		response[i] = h.convertOrderToWithPairBatch(ctx, order, pairs, tokens)
	}

	return response
}

// convertOrderToWithPairBatch converts a single order using preloaded pair and token data
func (h *Handler) convertOrderToWithPairBatch(ctx context.Context, order models.Order, pairs map[string]*models.Pair, tokens map[string]*models.Token) models.OrderWithPair {
	// Get pair info from preloaded data
	var pairInfo *models.PairInfo
	pair := pairs[order.PairID]
	if pair != nil {
		baseSymbol := pair.BaseSymbol
		quoteSymbol := pair.QuoteSymbol

		// If symbols are empty, try to extract from JSON in base_token/quote_token
		if baseSymbol == "" {
			var tokenData map[string]interface{}
			if json.Unmarshal([]byte(pair.BaseToken), &tokenData) == nil {
				if s, ok := tokenData["symbol"].(string); ok {
					baseSymbol = s
				}
			}
		}
		if quoteSymbol == "" {
			var tokenData map[string]interface{}
			if json.Unmarshal([]byte(pair.QuoteToken), &tokenData) == nil {
				if s, ok := tokenData["symbol"].(string); ok {
					quoteSymbol = s
				}
			}
		}

		// Extract logos
		baseLogo := ""
		quoteLogo := ""
		var baseTokenData map[string]interface{}
		if json.Unmarshal([]byte(pair.BaseToken), &baseTokenData) == nil {
			if logo, ok := baseTokenData["logo"].(string); ok {
				baseLogo = logo
			}
		}
		var quoteTokenData map[string]interface{}
		if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenData) == nil {
			if logo, ok := quoteTokenData["logo"].(string); ok {
				quoteLogo = logo
			}
		}

		pairInfo = &models.PairInfo{
			ID:          pair.ID,
			BaseSymbol:  baseSymbol,
			QuoteSymbol: quoteSymbol,
			BaseLogo:    baseLogo,
			QuoteLogo:   quoteLogo,
		}
	}

	// Get token info from preloaded data
	tokenInDecimals := order.AmountInDecimals
	tokenInSymbol := ""
	tokenOutDecimals := order.AmountOutDecimals
	tokenOutSymbol := ""
	if tokenInDecimals == 0 {
		tokenInDecimals = 18
	}
	if tokenOutDecimals == 0 {
		tokenOutDecimals = 18
	}

	// If pair exists, extract token symbols based on order side
	// For BUY: token_in = quote (WBNB), token_out = base (Gift)
	// For SELL: token_in = base (Gift), token_out = quote (WBNB)
	if pair != nil {
		var baseTokenData, quoteTokenData map[string]interface{}
		if json.Unmarshal([]byte(pair.BaseToken), &baseTokenData) == nil {
			if s, ok := baseTokenData["symbol"].(string); ok {
				// For SELL orders, base token is token_in
				if order.Side == models.OrderSideSell {
					tokenInSymbol = s
				} else {
					tokenOutSymbol = s
				}
			}
		}
		if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenData) == nil {
			if s, ok := quoteTokenData["symbol"].(string); ok {
				// For BUY orders, quote token is token_in
				if order.Side == models.OrderSideBuy {
					tokenInSymbol = s
				} else {
					tokenOutSymbol = s
				}
			}
		}
	}

	// Try to get token symbols from preloaded tokens table (overrides pair data if available)
	if token := tokens[order.TokenIn]; token != nil {
		tokenInSymbol = token.Symbol
		if token.Decimals > 0 {
			tokenInDecimals = token.Decimals
		}
	}
	if token := tokens[order.TokenOut]; token != nil {
		tokenOutSymbol = token.Symbol
		if token.Decimals > 0 {
			tokenOutDecimals = token.Decimals
		}
	}

	tokenInInfo := &models.TokenInfo{
		Symbol:   tokenInSymbol,
		Decimals: tokenInDecimals,
	}
	tokenOutInfo := &models.TokenInfo{
		Symbol:   tokenOutSymbol,
		Decimals: tokenOutDecimals,
	}

	orderDTO := models.OrderDTO{
		ID:                      order.ID,
		OrderHash:               order.OrderHash,
		UserID:                  order.UserID,
		Network:                 string(order.Network),
		PairID:                  order.PairID,
		Side:                    string(order.Side),
		OrderType:               string(order.OrderType),
		Price:                   order.Price.String(),
		Amount:                  order.Amount.String(),
		FilledAmount:            order.FilledAmount.String(),
		AmountIn:                order.AmountIn.String(),
		AmountOutMin:            order.AmountOutMin.String(),
		TokenIn:                 order.TokenIn,
		TokenOut:                order.TokenOut,
		TokenInDecimals:         tokenInDecimals,
		TokenOutDecimals:        tokenOutDecimals,
		Maker:                   order.Maker,
		Nonce:                   order.Nonce,
		Salt:                    order.Salt,
		Status:                  string(order.Status),
		IsLadder:                order.IsLadder,
		TriggerPrice:            order.TriggerPrice.String(),
		IsPostOnly:              order.IsPostOnly,
		ReduceOnly:              order.ReduceOnly,
		TimeInForce:             order.TimeInForce,
		StopLossType:            order.StopLossType,
		Expiration:              order.Expiration.Format(time.RFC3339),
		CreatedAt:               order.CreatedAt.Format(time.RFC3339),
		UpdatedAt:               order.UpdatedAt.Format(time.RFC3339),
		LadderTotalAmountIn:     order.LadderTotalAmountIn.String(),
		LadderTotalAmountOutMin: order.LadderTotalAmountOutMin.String(),
	}

	// Set ladder fields if applicable
	if order.IsLadder {
		if order.LadderLevels > 0 {
			levels := order.LadderLevels
			orderDTO.LadderLevels = &levels
		}
		orderDTO.LadderPriceStart = order.LadderPriceStart.String()
		orderDTO.LadderPriceEnd = order.LadderPriceEnd.String()
		orderDTO.LadderParentID = order.LadderParentID

		// For ladder parent orders, use the total amounts for display instead of zero
		if order.LadderParentID == nil && order.LadderTotalAmountIn.GreaterThan(decimal.Zero) {
			orderDTO.AmountIn = order.LadderTotalAmountIn.String()
			orderDTO.AmountOutMin = order.LadderTotalAmountOutMin.String()
		}
	}

	// Calculate human-readable amounts
	// For ladder parent orders, use the ladder total amounts if available
	var amountInDisplay, amountOutMinDisplay decimal.Decimal
	if order.IsLadder && order.LadderParentID == nil && order.LadderTotalAmountIn.GreaterThan(decimal.Zero) {
		amountInDisplay = order.LadderTotalAmountIn
		amountOutMinDisplay = order.LadderTotalAmountOutMin
	} else {
		amountInDisplay = order.AmountIn
		amountOutMinDisplay = order.AmountOutMin
	}

	return models.OrderWithPair{
		Order:             orderDTO,
		Pair:              pairInfo,
		TokenInInfo:       tokenInInfo,
		TokenOutInfo:      tokenOutInfo,
		AmountInHuman:     convertWeiToHuman(amountInDisplay, tokenInDecimals),
		AmountOutMinHuman: convertWeiToHuman(amountOutMinDisplay, tokenOutDecimals),
	}
}

type PairResponse struct {
	models.Pair
	Price          string     `json:"price"`
	PriceUSD       string     `json:"price_usd"`
	PriceChange24h string     `json:"price_change_24h"`
	PriceHigh24h   string     `json:"price_high_24h"`
	PriceLow24h    string     `json:"price_low_24h"`
	Volume24h      string     `json:"volume_24h"`
	Volume24hUSD   string     `json:"volume_24h_usd"`
	Liquidity      string     `json:"liquidity"`
	LiquidityUSD   string     `json:"liquidity_usd"`
	MarketCap      string     `json:"market_cap"`
	MarketCapUSD   string     `json:"market_cap_usd"`
	LastTradeAt    *time.Time `json:"last_trade_at"`
	// Explicit string override so the frontend always receives a plain decimal string.
	// The embedded models.Pair.LastTradePrice (*decimal.Decimal) would serialize as a
	// nested object; this field shadows it with a clean "0.000123" string.
	LastTradePrice string `json:"last_trade_price,omitempty"`
	// GeckoPrice is the price stored by the price-worker from GeckoTerminal.
	// It is always the market reference price, independent of whether fills exist.
	// The frontend uses this as the fallback price after a trade goes stale (>5 min).
	GeckoPrice    string `json:"gecko_price,omitempty"`
	GeckoPriceUSD string `json:"gecko_price_usd,omitempty"`
	// GeckoPriceChange24h is the 24h % change from GeckoTerminal (price-worker).
	// The frontend uses it as fallback when the last fill is stale (>5 min).
	GeckoPriceChange24h string `json:"gecko_price_change_24h,omitempty"`
}

// pow10 returns 10^n as a big.Int.
func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

type Handler struct {
	config               *config.Config
	orderRepo            *repository.OrderRepository
	fillRepo             *repository.FillRepository
	pairRepo             *repository.PairRepository
	candleRepo           *repository.CandleRepository
	userRepo             *repository.UserRepository
	depositRepo          *repository.DepositRepository
	engine               *engine.MatchingEngine
	hub                  *websocket.Hub
	ethService           *services.EthereumService
	authService          *services.AuthService
	geckoTerminalService *services.GeckoTerminalService
	cache                *cache.CacheManager
	refundService        *services.RefundService
}

func NewHandler(
	cfg *config.Config,
	orderRepo *repository.OrderRepository,
	fillRepo *repository.FillRepository,
	pairRepo *repository.PairRepository,
	userRepo *repository.UserRepository,
	depositRepo *repository.DepositRepository,
	eng *engine.MatchingEngine,
	hub *websocket.Hub,
	ethService *services.EthereumService,
	authService *services.AuthService,
	refundService *services.RefundService,
	redis *db.RedisClient,
	database *db.DB,
) *Handler {
	return &Handler{
		config:               cfg,
		orderRepo:            orderRepo,
		fillRepo:             fillRepo,
		pairRepo:             pairRepo,
		candleRepo:           repository.NewCandleRepository(database),
		userRepo:             userRepo,
		depositRepo:          depositRepo,
		engine:               eng,
		hub:                  hub,
		ethService:           ethService,
		authService:          authService,
		refundService:        refundService,
		geckoTerminalService: services.NewGeckoTerminalService(cfg),
		cache:                cache.NewManager(redis, pairRepo, orderRepo),
	}
}

// Health check
func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"time":   time.Now().Unix(),
	})
}

// CacheHealth returns Redis health status for the cache layer
func (h *Handler) CacheHealth(c *gin.Context) {
	if h.cache == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "redis disabled"})
		return
	}

	if err := h.cache.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "redis error", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "redis ok"})
}

// ClearCache clears all cached pairs from Redis
func (h *Handler) ClearCache(c *gin.Context) {
	if h.cache == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "redis disabled"})
		return
	}

	if err := h.cache.ClearAllPairs(c.Request.Context()); err != nil {
		log.Printf("[ClearCache] failed to clear cache: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to clear cache", "details": err.Error()})
		return
	}

	log.Printf("[ClearCache] successfully cleared all pair cache")
	c.JSON(http.StatusOK, gin.H{"status": "cache cleared"})
}

func (h *Handler) GetSolanaDepositByMemo(c *gin.Context) {
	memo := c.Param("memo")
	if memo == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "memo is required"})
		return
	}

	deposit, err := h.depositRepo.GetByMemo(c.Request.Context(), memo)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "deposit not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch deposit", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, deposit)
}

func (h *Handler) GetSolanaDepositByTxHash(c *gin.Context) {
	txHash := c.Param("txHash")
	if txHash == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "txHash is required"})
		return
	}

	deposit, err := h.depositRepo.GetByTxHash(c.Request.Context(), txHash)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "deposit not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch deposit", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, deposit)
}

// RegisterRoutes sets up all API routes
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	// Public routes
	r.GET("/health", h.Health)
	r.GET("/api/v1/cache/health", h.CacheHealth)
	r.POST("/api/v1/cache/clear", h.ClearCache)
	r.GET("/api/v1/pairs", h.GetPairs)
	r.GET("/api/v1/pairs/trending", h.GetTrendingPairs)
	r.GET("/api/v1/pairs/:id", h.GetPair)
	r.GET("/api/v1/pairs/:id/orderbook", h.GetOrderBook)
	r.GET("/api/v1/pairs/:id/trades", h.GetTrades)
	r.GET("/api/v1/pairs/:id/candles", h.GetPairCandles)
	r.GET("/api/v1/debug/candles/:id", h.GetDebugCandles)
	r.GET("/api/v1/pairs/:id/ticker", h.GetTicker)
	r.GET("/api/v1/tokens/:address", h.GetToken)
	r.GET("/api/v1/search", h.Search)

	// Order routes - public (for unauthenticated access)
	r.GET("/api/v1/orders", h.GetOrders)
	r.GET("/api/v1/orders/open", h.GetOpenOrders)
	r.GET("/api/v1/orders/history", h.GetHistoryOrders)
	r.GET("/api/v1/orders/:id", h.GetOrder)

	// Order creation - public for testing (should be protected in production)
	r.POST("/api/v1/orders", h.CreateOrder)

	// Solana deposit lookups
	r.GET("/api/v1/solana/deposits/memo/:memo", h.GetSolanaDepositByMemo)
	r.GET("/api/v1/solana/deposits/tx/:txHash", h.GetSolanaDepositByTxHash)

	// Auth - public for login (must be before :id routes)
	r.POST("/api/v1/auth/login", h.Login)

	// WebSocket
	r.GET("/ws", h.HandleWebSocket)
	// WebSocket debug logs
	r.GET("/debug/ws-logs", h.GetWebSocketDebugLogs)

	// Order routes - public (works with address query param)
	r.DELETE("/api/v1/orders/:id", h.CancelOrder)

	// Fill routes - public (query by address)
	r.GET("/api/v1/fills/address/:address", h.GetFillsByAddress)

	// Protected routes
	protected := r.Group("/api/v1")
	protected.Use(h.AuthMiddleware())
	{
		// User routes
		protected.GET("/user/profile", h.GetProfile)
		protected.PUT("/user/profile", h.UpdateProfile)
		protected.GET("/user/balances", h.GetBalances)

		// Protected order routes (for authenticated users)
		protected.DELETE("/orders", h.BatchCancelOrders)

		// Fill routes
		protected.GET("/fills", h.GetFills)
		protected.GET("/fills/:id", h.GetFill)

		// Commit-Reveal routes
		protected.POST("/orders/commit", h.CommitOrder)
		protected.POST("/orders/reveal", h.RevealOrder)
	}
}

// Login handles user authentication and returns JWT token
func (h *Handler) Login(c *gin.Context) {
	var req struct {
		Address string `json:"address" binding:"required"`
		Network string `json:"network"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address is required"})
		return
	}

	if req.Network == "" {
		req.Network = string(models.NetworkBSC)
	}

	ctx := c.Request.Context()

	// Get or create user
	user, err := h.userRepo.GetOrCreate(ctx, req.Address)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create user"})
		return
	}

	// Generate JWT token
	token, err := h.authService.GenerateToken(user.ID, req.Address, req.Network)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"token":   token,
		"user_id": user.ID,
		"address": user.Address,
		"network": req.Network,
	})
}

// buildPairResponse enriches a pair with stats for frontend consumption.
func isContextDone(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (h *Handler) buildPairResponse(ctx context.Context, pair *models.Pair) (*PairResponse, error) {
	// ✅ FIX #3: Try to use cached stats first
	var stats *models.TradeStats
	var err error

	if h.cache != nil {
		if cachedStats, err := h.cache.GetCachedStats(ctx, pair.ID); err == nil && len(cachedStats) > 0 {
			if err := json.Unmarshal(cachedStats, &stats); err != nil {
				stats = nil
			}
		}
	}

	if stats == nil {
		stats, err = h.pairRepo.GetStats(ctx, pair.ID)
		if err != nil {
			if isContextDone(err) || err.Error() == "record not found" {
				log.Printf("[GetPairs] warning: stats fetch fallback for %s: %v", pair.ID, err)
				stats = &models.TradeStats{
					PairID:         pair.ID,
					Price:          decimal.Zero,
					PriceChange24h: decimal.Zero,
					PriceHigh24h:   decimal.Zero,
					PriceLow24h:    decimal.Zero,
					Volume24h:      decimal.Zero,
					Liquidity:      decimal.Zero,
				}
			} else {
				log.Printf("[GetPairs] warning: failed to fetch stats for %s, using empty stats: %v", pair.ID, err)
				stats = &models.TradeStats{
					PairID:         pair.ID,
					Price:          decimal.Zero,
					PriceChange24h: decimal.Zero,
					PriceHigh24h:   decimal.Zero,
					PriceLow24h:    decimal.Zero,
					Volume24h:      decimal.Zero,
					Liquidity:      decimal.Zero,
				}
			}
		}
	}

	// If High/Low are zero (common in simulated/new pairs), calculate from candles
	if stats.PriceHigh24h.IsZero() || stats.PriceLow24h.IsZero() {
		// Fetch 24h candles (resolution 3600s = 1h, limit 24)
		candles, err := h.fillRepo.GetCandles(ctx, pair.ID, 3600, 24)
		if err == nil && len(candles) > 0 {
			var high, low decimal.Decimal
			initialized := false
			for _, c := range candles {
				if !initialized {
					high = c.High
					low = c.Low
					initialized = true
					continue
				}
				if c.High.GreaterThan(high) {
					high = c.High
				}
				if c.Low.LessThan(low) && c.Low.GreaterThan(decimal.Zero) {
					low = c.Low
				}
			}
			if stats.PriceHigh24h.IsZero() {
				stats.PriceHigh24h = high
			}
			if stats.PriceLow24h.IsZero() {
				stats.PriceLow24h = low
			}
		}
	}

	// If no price from fills, try to get mid-price from orderbook
	if stats.Price.IsZero() {
		ob := h.engine.GetOrderBook(pair.ID)
		if ob != nil && len(ob.Bids) > 0 && len(ob.Asks) > 0 {
			// Calculate mid-price
			bestBid := ob.Bids[0].Price
			bestAsk := ob.Asks[0].Price
			if bestBid.GreaterThan(decimal.Zero) && bestAsk.GreaterThan(decimal.Zero) {
				stats.Price = bestBid.Add(bestAsk).Div(decimal.NewFromInt(2))
				fmt.Printf("[DEBUG] Got mid-price from orderbook for %s: bid=%s, ask=%s, mid=%s\n",
					pair.ID, bestBid.String(), bestAsk.String(), stats.Price.String())
			}
		}
	}

	// ── Price-Worker fallback ────────────────────────────────────────────────
	// If still no price (pair has never been traded on this DEX and has no
	// orderbook activity), read from the Redis key written by the price-worker
	// microservice: pair:price:{pairId}
	// This gives us the live GeckoTerminal market price as a display fallback.
	if stats.Price.IsZero() && h.cache != nil {
		if priceJSON, redisErr := h.cache.GetRaw(ctx, "pair:price:"+pair.ID); redisErr == nil && len(priceJSON) > 0 {
			var priceData struct {
				Price          string `json:"price"`
				PriceUSD       string `json:"priceUSD"`
				PriceChange24h string `json:"priceChange24h"`
				Volume24h      string `json:"volume24h"`
				Liquidity      string `json:"liquidity"`
				MarketCap      string `json:"marketCap"`
			}
			if json.Unmarshal(priceJSON, &priceData) == nil {
				if p, e := decimal.NewFromString(priceData.PriceUSD); e == nil && p.GreaterThan(decimal.Zero) {
					stats.Price = p
				} else if p, e := decimal.NewFromString(priceData.Price); e == nil && p.GreaterThan(decimal.Zero) {
					stats.Price = p
				}
				if pc, e := decimal.NewFromString(priceData.PriceChange24h); e == nil {
					stats.PriceChange24h = pc
				}
				// Volume is intentionally NOT pulled from price-worker/GeckoTerminal.
				// Volume must only reflect real trades on our backend (fills table).
				if liq, e := decimal.NewFromString(priceData.Liquidity); e == nil && stats.Liquidity.IsZero() {
					stats.Liquidity = liq
				}
				log.Printf("[buildPairResponse] %s: using price-worker price=%s change=%s%%",
					pair.ID, stats.Price.String(), stats.PriceChange24h.String())
			}
		}
	}
	// ────────────────────────────────────────────────────────────────────────

	// DB price columns fallback: if no fills/orderbook price, use native price
	// from the pairs table (written by price-worker from GeckoTerminal).
	// stats.Price = native price (e.g. 0.0014 WBNB) — used for trading math.
	// PriceUSD (e.g. $0.8976) is returned separately in PairResponse.PriceUSD.
	if stats.Price.IsZero() && pair.Price.GreaterThan(decimal.Zero) {
		stats.Price = pair.Price
		if stats.PriceChange24h.IsZero() {
			stats.PriceChange24h = pair.PriceChange24h
		}
		// Volume is intentionally NOT pulled from the DB column (price-worker / GeckoTerminal).
		// stats.Volume24h stays at zero for pairs with no real fills on our backend.
		if stats.Liquidity.IsZero() && pair.Liquidity.GreaterThan(decimal.Zero) {
			stats.Liquidity = pair.Liquidity
		}
		log.Printf("[buildPairResponse] %s: DB native price=%s change=%.2f%%",
			pair.ID, stats.Price.String(), stats.PriceChange24h.InexactFloat64())
	}

	// Get token decimals for liquidity calculation
	quoteTokenDecimals := 18 // Default to 18 decimals (standard ERC20)
	baseTokenDecimals := 18  // Default to 18 decimals (standard ERC20)

	// Extract quote and base token metadata from JSON
	var quoteTokenMetadata map[string]interface{}
	if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenMetadata) == nil {
		if decimalsVal, ok := quoteTokenMetadata["decimals"]; ok {
			switch v := decimalsVal.(type) {
			case float64:
				quoteTokenDecimals = int(v)
			case int:
				quoteTokenDecimals = v
			}
		}
	}

	// Extract base token decimals from JSON
	var baseTokenData map[string]interface{}
	if json.Unmarshal([]byte(pair.BaseToken), &baseTokenData) == nil {
		if val, ok := baseTokenData["decimals"]; ok {
			switch v := val.(type) {
			case float64:
				baseTokenDecimals = int(v)
			case int:
				baseTokenDecimals = v
			}
		}
		// Special case for CREPE token with correct decimals
		if symbol, ok := baseTokenData["symbol"].(string); ok && symbol == "CREPE" {
			baseTokenDecimals = 9
		}
	}

	// Calculate proper liquidity from orderbook depth
	liquidity := h.calculateOrderbookLiquidity(pair.ID, stats.Price, baseTokenDecimals, quoteTokenDecimals)

	// Get quote token USD price and symbol for conversions
	quoteTokenUSDPrice := decimal.NewFromInt(1)
	quoteTokenSymbol := pair.QuoteSymbol

	if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenMetadata) == nil {
		if quoteTokenSymbol == "" {
			if symbol, ok := quoteTokenMetadata["symbol"].(string); ok {
				quoteTokenSymbol = symbol
			}
		}
		if decimalsVal, ok := quoteTokenMetadata["decimals"]; ok {
			switch v := decimalsVal.(type) {
			case float64:
				quoteTokenDecimals = int(v)
			case int:
				quoteTokenDecimals = v
			}
		}
	}

	// ✅ FIX #2: Add timeout for GetTokenUSDPrice to prevent hanging
	if h.ethService != nil && quoteTokenSymbol != "" {
		// Create a context with 3-second timeout for price fetching
		priceCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		if usdPrice, err := h.ethService.GetTokenUSDPrice(priceCtx, string(pair.Network), pair.QuoteToken, quoteTokenSymbol); err == nil {
			quoteTokenUSDPrice = usdPrice
			fmt.Printf("[DEBUG] Got USD price for %s on %s: %s\n", quoteTokenSymbol, pair.Network, usdPrice.String())
		} else {
			fmt.Printf("[DEBUG] Failed to get USD price for %s on %s (timeout or error): %v\n", quoteTokenSymbol, pair.Network, err)
			// Use default USD price of 1 instead of failing
			quoteTokenUSDPrice = decimal.NewFromInt(1)
		}
	}

	fmt.Printf("[DEBUG] Pair %s: price=%s, volume=%s, liquidity=%s, quoteToken=%s (decimals=%d), usdPrice=%s\n",
		pair.ID, stats.Price.String(), stats.Volume24h.String(), liquidity.String(), quoteTokenSymbol, quoteTokenDecimals, quoteTokenUSDPrice.String())

	// Normalize volume and liquidity by converting from wei format (raw decimals) to human-readable
	normalizedVolume := convertFromWei(stats.Volume24h, quoteTokenDecimals)
	normalizedLiquidity := convertFromWei(liquidity, quoteTokenDecimals)

	fmt.Printf("[DEBUG] Volume normalization: raw_volume=%s, normalized_volume=%s, decimals=%d\n",
		stats.Volume24h.String(), normalizedVolume.String(), quoteTokenDecimals)

	// Calculate USD equivalents using normalized values
	// If there are real fills (stats.Price differs from gecko pair.Price), compute
	// priceUSD fresh by multiplying the fill price by the live quote-token USD rate
	// (from Chainlink). This ensures that after trades execute the USD value reflects
	// the actual traded price, not the stale price-worker DB column.
	// Otherwise (no fills yet) fall back to the DB price_usd from the price-worker.
	var priceUSD decimal.Decimal
	hasRealTrades := stats.Price.GreaterThan(decimal.Zero)
	if hasRealTrades && quoteTokenUSDPrice.GreaterThan(decimal.Zero) {
		// Fresh computation: fill_price × live_quote_token_USD_rate
		priceUSD = stats.Price.Mul(quoteTokenUSDPrice)
	} else if pair.PriceUSD.GreaterThan(decimal.Zero) {
		// No fills yet (or Chainlink unavailable) — use the price-worker DB value
		priceUSD = pair.PriceUSD
	} else {
		// Last resort: gecko price × quote token rate
		priceUSD = pair.Price.Mul(quoteTokenUSDPrice)
	}
	volume24hUSD := normalizedVolume.Mul(quoteTokenUSDPrice)
	liquidityUSD := normalizedLiquidity.Mul(quoteTokenUSDPrice)

	fmt.Printf("[DEBUG] USD calculations: priceUSD=%s, volumeUSD=%s, liquidityUSD=%s\n",
		priceUSD.String(), volume24hUSD.String(), liquidityUSD.String())

	fmt.Printf("[DEBUG] Final response values: Volume24h=%s, Volume24hUSD=%s\n",
		normalizedVolume.String(), volume24hUSD.String())

	// Market cap is fetched from the database (via server from Gecko Terminal)
	// If USD value is missing, derive it from the quote token USD price.
	marketCap := pair.MarketCap
	marketCapUSD := pair.MarketCapUSD
	if marketCapUSD.IsZero() {
		if quoteTokenUSDPrice.GreaterThan(decimal.Zero) {
			marketCapUSD = marketCap.Mul(quoteTokenUSDPrice)
		} else {
			marketCapUSD = marketCap
		}
	}

	return &PairResponse{
		Pair:           *pair,
		Price:          stats.Price.String(),
		PriceUSD:       priceUSD.String(),
		PriceChange24h: stats.PriceChange24h.String(),
		PriceHigh24h:   stats.PriceHigh24h.String(),
		PriceLow24h:    stats.PriceLow24h.String(),
		Volume24h:      normalizedVolume.String(), // Normalize volume for frontend display
		Volume24hUSD:   volume24hUSD.String(),
		Liquidity:      normalizedLiquidity.String(), // Keep normalized liquidity for frontend formatting
		LiquidityUSD:   liquidityUSD.String(),
		MarketCap:      marketCap.String(),
		MarketCapUSD:   marketCapUSD.String(),
		LastTradeAt:    stats.LastTradeAt,
		LastTradePrice: func() string {
			if pair.LastTradePrice != nil && !pair.LastTradePrice.IsZero() {
				return pair.LastTradePrice.String()
			}
			return ""
		}(),
		// Always expose the GeckoTerminal price from the price-worker, regardless
		// of whether fills exist. The frontend uses this as the fallback display
		// price after a trade goes stale (> 5 min with no new fills).
		GeckoPrice: func() string {
			if pair.Price.GreaterThan(decimal.Zero) {
				return pair.Price.String()
			}
			return ""
		}(),
		GeckoPriceUSD: func() string {
			if pair.PriceUSD.GreaterThan(decimal.Zero) {
				return pair.PriceUSD.String()
			}
			return ""
		}(),
		// Always expose the GeckoTerminal 24h change so the frontend can fall back
		// to it after our backend's last fill goes stale (> 5 min with no trades).
		GeckoPriceChange24h: func() string {
			// pair.PriceChange24h is the gecko value written by the price-worker.
			// Only emit it if it's non-zero so the frontend can distinguish
			// "no gecko data" from "gecko says 0% change".
			if !pair.PriceChange24h.IsZero() {
				return pair.PriceChange24h.String()
			}
			return ""
		}(),
	}, nil
}

// calculateOrderbookLiquidity calculates total liquidity from orderbook depth
func (h *Handler) calculateOrderbookLiquidity(pairID string, currentPrice decimal.Decimal, baseDecimals, quoteDecimals int) decimal.Decimal {
	ob := h.engine.GetOrderBook(pairID)
	if ob == nil {
		return decimal.Zero
	}

	totalLiquidity := decimal.Zero
	multiplier := decimal.NewFromFloat(math.Pow10(quoteDecimals - baseDecimals))

	// Sum up bid side liquidity (buy orders)
	for _, level := range ob.Bids {
		if level.Amount.GreaterThan(decimal.Zero) {
			// For bids, liquidity is amount * price * 10^(quoteDecimals - baseDecimals)
			levelLiquidity := level.Amount.Mul(level.Price).Mul(multiplier)
			totalLiquidity = totalLiquidity.Add(levelLiquidity)
		}
	}

	// Sum up ask side liquidity (sell orders)
	for _, level := range ob.Asks {
		if level.Amount.GreaterThan(decimal.Zero) {
			// For asks, liquidity is amount * price * 10^(quoteDecimals - baseDecimals)
			levelLiquidity := level.Amount.Mul(level.Price).Mul(multiplier)
			totalLiquidity = totalLiquidity.Add(levelLiquidity)
		}
	}

	return totalLiquidity
}

// BroadcastLiquidity broadcasts the current liquidity for a pair
func (h *Handler) BroadcastLiquidity(ctx context.Context, pairID string) {
	// Get pair to find quote token decimals
	pair, err := h.pairRepo.GetByID(ctx, pairID)
	if err != nil {
		log.Printf("[Liquidity] Failed to get pair %s: %v", pairID, err)
		return
	}

	// Parse token decimals
	quoteTokenDecimals := 18
	baseTokenDecimals := 18
	var quoteTokenData map[string]interface{}
	if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenData) == nil {
		if decimalsVal, ok := quoteTokenData["decimals"]; ok {
			switch v := decimalsVal.(type) {
			case float64:
				quoteTokenDecimals = int(v)
			case int:
				quoteTokenDecimals = v
			}
		}
	}
	var baseTokenData map[string]interface{}
	if json.Unmarshal([]byte(pair.BaseToken), &baseTokenData) == nil {
		if decimalsVal, ok := baseTokenData["decimals"]; ok {
			switch v := decimalsVal.(type) {
			case float64:
				baseTokenDecimals = int(v)
			case int:
				baseTokenDecimals = v
			}
		}
		// Special case for CREPE token with correct decimals
		if symbol, ok := baseTokenData["symbol"].(string); ok && symbol == "CREPE" {
			baseTokenDecimals = 9
		}
	}

	// Calculate liquidity from orderbook
	liquidity := h.calculateOrderbookLiquidity(pairID, decimal.Zero, baseTokenDecimals, quoteTokenDecimals)

	// Convert to human-readable
	liquidityHuman := convertFromWei(liquidity, quoteTokenDecimals)

	// Calculate USD value if we have eth service
	var liquidityUSD string = "0"
	quoteTokenSymbol := ""
	if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenData) == nil {
		if symbol, ok := quoteTokenData["symbol"].(string); ok {
			quoteTokenSymbol = symbol
		}
	}

	if h.ethService != nil && quoteTokenSymbol != "" {
		if usdPrice, err := h.ethService.GetTokenUSDPrice(ctx, string(pair.Network), pair.QuoteToken, quoteTokenSymbol); err == nil {
			liquidityUSD = liquidityHuman.Mul(usdPrice).String()
		}
	}

	log.Printf("[Liquidity] Broadcasting for %s: liquidity=%s USD=%s", pairID, liquidityHuman.String(), liquidityUSD)

	// Broadcast to connected clients
	h.hub.BroadcastLiquidityUpdate(pairID, liquidityHuman.String(), liquidityUSD)
}

// GetPairs returns all trading pairs
func (h *Handler) GetPairs(c *gin.Context) {
	network := c.Query("network")
	limit := 100
	if network == "" {
		limit = 50 // Limit when fetching all networks to prevent slow responses
	}
	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}

	ctx := c.Request.Context()
	start := time.Now()

	log.Printf("[GetPairs] START network=%q limit=%d cacheEnabled=%v", network, limit, h.cache != nil)

	// ✅ FIX #1: Check full cached pairs list FIRST before DB query
	var responses []*PairResponse
	cacheStatus := "DISABLED"

	if h.cache != nil {
		cacheCheckStart := time.Now()
		cacheStatus = "MISS"

		// Try exact network cache first
		if network != "" {
			log.Printf("[GetPairs] Checking exact network cache for network=%q", network)
			if cached, err := h.cache.GetCachedPairs(ctx, network); err == nil && len(cached) > 0 {
				log.Printf("[GetPairs] Found exact network cache (%d bytes), unmarshaling...", len(cached))
				if respList, err := h.unmarshalCachedPairResponses(cached); err == nil {
					responses = respList
					cacheStatus = "HIT"
					cacheCheckTime := time.Since(cacheCheckStart)
					totalTime := time.Since(start)
					log.Printf("[GetPairs] ✅ EXACT_CACHE_HIT network=%s count=%d cacheCheckTime=%dms totalTime=%dms", network, len(responses), cacheCheckTime.Milliseconds(), totalTime.Milliseconds())
					c.Header("X-Cache", cacheStatus)
					c.JSON(http.StatusOK, gin.H{"data": responses, "count": len(responses)})
					return
				} else {
					log.Printf("[GetPairs] ❌ Unmarshal failed for exact cache: %v", err)
					if deleteErr := h.cache.DeleteCachedPairs(ctx, network); deleteErr != nil {
						log.Printf("[GetPairs] failed to delete invalid exact cache network=%s: %v", network, deleteErr)
					}
				}
			} else {
				log.Printf("[GetPairs] ❌ Exact network cache not found or empty (err=%v, len=%d)", err, len(cached))
			}
		}

		// Try full cache as fallback
		if cacheStatus != "HIT" {
			log.Printf("[GetPairs] Checking full cache (network=\"\")")
			if cached, err := h.cache.GetCachedPairs(ctx, ""); err == nil && len(cached) > 0 {
				log.Printf("[GetPairs] Found full cache (%d bytes), unmarshaling and filtering...", len(cached))
				if allResponses, err := h.unmarshalCachedPairResponses(cached); err == nil {
					allowed := map[string]bool{}
					if network != "" {
						for _, n := range strings.Split(network, ",") {
							if n = strings.TrimSpace(n); n != "" {
								allowed[strings.ToLower(n)] = true
							}
						}
					}
					for _, resp := range allResponses {
						if network == "" || allowed[strings.ToLower(string(resp.Network))] {
							responses = append(responses, resp)
						}
					}
					if len(responses) > 0 {
						cacheStatus = "HIT"
						cacheCheckTime := time.Since(cacheCheckStart)
						totalTime := time.Since(start)
						log.Printf("[GetPairs] ✅ FULL_CACHE_HIT network=%q count=%d filtered=%d cacheCheckTime=%dms totalTime=%dms", network, len(allResponses), len(responses), cacheCheckTime.Milliseconds(), totalTime.Milliseconds())
						c.Header("X-Cache", cacheStatus)
						c.JSON(http.StatusOK, gin.H{"data": responses, "count": len(responses)})
						return
					} else {
						log.Printf("[GetPairs] ❌ Full cache exists but no matching networks. network=%q, allowed=%v", network, allowed)
					}
				} else {
					log.Printf("[GetPairs] ❌ Unmarshal failed for full cache: %v", err)
					if deleteErr := h.cache.DeleteCachedPairs(ctx, ""); deleteErr != nil {
						log.Printf("[GetPairs] failed to delete invalid full cache: %v", deleteErr)
					}
				}
			} else {
				log.Printf("[GetPairs] ❌ Full cache not found or empty (err=%v, len=%d)", err, len(cached))
			}
		}
	} else {
		log.Printf("[GetPairs] ❌ Cache is disabled (nil)")
	}

	// Cache miss - query database
	pairs, err := h.pairRepo.GetPairs(ctx, network, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	dbQueryTime := time.Since(start)
	log.Printf("[GetPairs] CACHE_MISS - DB query took %dms, fetched %d pairs for network=%s", dbQueryTime.Milliseconds(), len(pairs), network)

	hitCount := 0
	buildTime := int64(0)
	responses = make([]*PairResponse, 0, len(pairs))

	for _, pair := range pairs {
		if h.cache != nil {
			if cached, err := h.cache.GetCachedPair(ctx, pair.ID); err == nil && len(cached) > 0 {
				var resp PairResponse
				if err := json.Unmarshal(cached, &resp); err == nil {
					responses = append(responses, &resp)
					hitCount++
					continue
				}
			}
		}
		// Cache miss, build response
		buildStart := time.Now()
		resp, err := h.buildPairResponse(ctx, &pair)
		buildTime += time.Since(buildStart).Milliseconds()
		if err != nil {
			log.Printf("[GetPairs] ERROR building pair %s: %v", pair.ID, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		responses = append(responses, resp)
		// Cache the new response
		if h.cache != nil {
			if data, err := json.Marshal(resp); err == nil {
				if err := h.cache.CachePair(ctx, pair.ID, data); err != nil {
					log.Printf("[Cache] failed to cache pair %s: %v", pair.ID, err)
				}
			}
		}
	}

	if h.cache != nil {
		if hitCount == len(pairs) {
			cacheStatus = "HIT"
		} else if hitCount > 0 {
			cacheStatus = "PARTIAL"
		} else {
			cacheStatus = "MISS"
		}
	}
	c.Header("X-Cache", cacheStatus)

	if h.cache != nil {
		if data, err := json.Marshal(responses); err == nil {
			if err := h.cache.CachePairsAll(ctx, network, data); err != nil {
				log.Printf("[Cache] failed to cache pairs list: %v", err)
			}
		}
	}

	totalTime := time.Since(start)
	log.Printf("[GetPairs] CACHE_MISS_RESPONSE network=%s status=%s hitCount=%d/%d dbTime=%dms buildTime=%dms totalTime=%dms", network, cacheStatus, hitCount, len(pairs), dbQueryTime.Milliseconds(), buildTime, totalTime.Milliseconds())

	c.JSON(http.StatusOK, gin.H{
		"data":  responses,
		"count": len(responses),
	})
}

// GetTrendingPairs returns the top trading pairs sorted by 24h volume
func (h *Handler) GetTrendingPairs(c *gin.Context) {
	network := c.Query("network")
	limit := 100
	if l := c.Query("limit"); l != "" {
		if _, err := fmt.Sscanf(l, "%d", &limit); err != nil || limit <= 0 {
			limit = 100
		}
	}

	if limit > 12 {
		limit = 12
	}

	const fetchLimit = 200
	ctx := c.Request.Context()
	cacheStatus := "DISABLED"
	responses := make([]*PairResponse, 0, 0)

	if h.cache != nil {
		cacheStatus = "MISS"
		if network != "" {
			if cached, err := h.cache.GetCachedPairs(ctx, network); err == nil && len(cached) > 0 {
				if respList, err := h.unmarshalCachedPairResponses(cached); err == nil {
					responses = respList
					cacheStatus = "HIT"
					log.Printf("[Cache] GetTrendingPairs HIT exact network=%q count=%d", network, len(responses))
				}
			}
		}

		if cacheStatus != "HIT" {
			if cached, err := h.cache.GetCachedPairs(ctx, ""); err == nil && len(cached) > 0 {
				if allResponses, err := h.unmarshalCachedPairResponses(cached); err == nil {
					allowed := map[string]bool{}
					for _, n := range strings.Split(network, ",") {
						if n = strings.TrimSpace(n); n != "" {
							allowed[strings.ToLower(n)] = true
						}
					}
					for _, resp := range allResponses {
						if network == "" || allowed[strings.ToLower(string(resp.Network))] {
							responses = append(responses, resp)
						}
					}
					cacheStatus = "HIT"
					log.Printf("[Cache] GetTrendingPairs HIT full cache network=%q count=%d", network, len(responses))
				}
			}
		}
	}

	if cacheStatus == "HIT" {
		sort.SliceStable(responses, func(i, j int) bool {
			iVol, err1 := decimal.NewFromString(responses[i].Volume24h)
			jVol, err2 := decimal.NewFromString(responses[j].Volume24h)
			if err1 != nil || err2 != nil {
				return false
			}
			return iVol.GreaterThan(jVol)
		})

		if len(responses) > limit {
			responses = responses[:limit]
		}

		c.Header("X-Cache", cacheStatus)
		c.JSON(http.StatusOK, gin.H{"data": responses, "count": len(responses)})
		return
	}

	pairs, err := h.pairRepo.GetPairs(ctx, network, fetchLimit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responses = make([]*PairResponse, 0, len(pairs))
	for _, pair := range pairs {
		resp, err := h.buildPairResponse(ctx, &pair)
		if err != nil {
			log.Printf("[Trending] skipping pair %s: %v", pair.ID, err)
			continue
		}

		if vol, err := decimal.NewFromString(resp.Volume24h); err != nil || vol.LessThanOrEqual(decimal.Zero) {
			continue
		}

		responses = append(responses, resp)
	}

	sort.SliceStable(responses, func(i, j int) bool {
		iVol, err1 := decimal.NewFromString(responses[i].Volume24h)
		jVol, err2 := decimal.NewFromString(responses[j].Volume24h)
		if err1 != nil || err2 != nil {
			return false
		}
		return iVol.GreaterThan(jVol)
	})

	if len(responses) > limit {
		responses = responses[:limit]
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  responses,
		"count": len(responses),
	})
}

// GetPair returns a single trading pair
func (h *Handler) GetPair(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	cacheStatus := "DISABLED"
	if h.cache != nil {
		cacheStatus = "MISS"
		if cached, err := h.cache.GetCachedPair(ctx, id); err == nil && len(cached) > 0 {
			var pairResp PairResponse
			if err := json.Unmarshal(cached, &pairResp); err == nil {
				cacheStatus = "HIT"
				c.Header("X-Cache", cacheStatus)
				c.JSON(http.StatusOK, pairResp)
				return
			}
		}
	}
	c.Header("X-Cache", cacheStatus)

	pair, err := h.pairRepo.GetByID(ctx, id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pair not found"})
		return
	}

	pairResp, err := h.buildPairResponse(ctx, pair)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if h.cache != nil {
		if data, err := json.Marshal(pairResp); err == nil {
			if err := h.cache.CachePair(ctx, id, data); err != nil {
				log.Printf("[Cache] failed to cache pair %s: %v", id, err)
			}
		}
	}

	c.JSON(http.StatusOK, pairResp)
}

// GetOrderBook returns the order book for a pair
func (h *Handler) GetOrderBook(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	pair, err := h.pairRepo.GetByID(ctx, id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pair not found"})
		return
	}

	cacheStatus := "DISABLED"
	if h.cache != nil {
		cacheStatus = "MISS"
		if cached, err := h.cache.GetCachedOrderbook(ctx, id); err == nil && len(cached) > 0 {
			var ob models.OrderBookResponse
			if err := json.Unmarshal(cached, &ob); err == nil {
				cacheStatus = "HIT"
				c.Header("X-Cache", cacheStatus)
				c.JSON(http.StatusOK, ob)
				return
			}
		}
	}
	c.Header("X-Cache", cacheStatus)

	ob, err := h.orderRepo.GetOrderBook(ctx, id, pair.Network, 50)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if h.cache != nil {
		if data, err := json.Marshal(ob); err == nil {
			if err := h.cache.CacheOrderbook(ctx, id, data); err != nil {
				log.Printf("[Cache] failed to cache orderbook %s: %v", id, err)
			}
		}
	}

	c.JSON(http.StatusOK, ob)
}

// GetTrades returns recent trades for a pair
func (h *Handler) GetTrades(c *gin.Context) {
	id := c.Param("id")
	limit := 50
	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}

	ctx := c.Request.Context()
	pair, err := h.pairRepo.GetByID(ctx, id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pair not found"})
		return
	}

	trades, err := h.orderRepo.GetRecentTrades(ctx, id, pair.Network, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  trades,
		"count": len(trades),
	})
}

// GetTicker returns ticker data for a pair
func (h *Handler) GetTicker(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()

	pair, err := h.pairRepo.GetByID(ctx, id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pair not found"})
		return
	}

	cacheStatus := "DISABLED"
	if h.cache != nil {
		cacheStatus = "MISS"
		if cached, err := h.cache.GetCachedStats(ctx, id); err == nil && len(cached) > 0 {
			var stats models.TradeStats
			if err := json.Unmarshal(cached, &stats); err == nil {
				cacheStatus = "HIT"
				c.Header("X-Cache", cacheStatus)
				c.JSON(http.StatusOK, stats)
				return
			}
		}
	}
	c.Header("X-Cache", cacheStatus)

	stats, err := h.pairRepo.GetStats(ctx, pair.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if h.cache != nil {
		if data, err := json.Marshal(stats); err == nil {
			if err := h.cache.CacheStats(ctx, pair.ID, data); err != nil {
				log.Printf("[Cache] failed to cache stats %s: %v", id, err)
			}
		}
	}

	c.JSON(http.StatusOK, stats)
}

// GetToken returns token information
func (h *Handler) GetToken(c *gin.Context) {
	address := c.Param("address")
	network := models.Network(c.Query("network"))
	if network != models.NetworkBSC && network != models.NetworkBase && network != models.NetworkSolana {
		network = models.NetworkBSC
	}
	ctx := c.Request.Context()

	token, err := h.pairRepo.GetTokenByAddress(ctx, address)
	if err != nil {
		// Token not in DB, try to fetch from contract and save
		if h.ethService != nil {
			decimals, symbol, err := h.ethService.GetTokenInfo(ctx, address, string(network))
			if err == nil {
				// Save to DB for future use
				tokenID := address
				tokenAddr := address
				if network != models.NetworkSolana {
					tokenID = strings.ToLower(address)
					tokenAddr = strings.ToLower(address)
				}
				
				token = &models.Token{
					ID:       tokenID,
					Network:  network,
					Address:  tokenAddr,
					Symbol:   symbol,
					Name:     symbol,
					Decimals: decimals,
				}
				if err := h.pairRepo.UpsertToken(ctx, token); err != nil {
					log.Printf("Failed to save token: %v", err)
				}

				c.JSON(http.StatusOK, token)
				return
			}
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "token not found"})
		return
	}

	// Token in DB but missing decimals? Try to fetch from contract
	if token.Decimals == 0 && h.ethService != nil {
		decimals, _, err := h.ethService.GetTokenInfo(ctx, address, string(network))
		if err == nil {
			token.Decimals = decimals
			// Update in DB
			h.pairRepo.UpsertToken(ctx, token)
		}
	}

	c.JSON(http.StatusOK, token)
}

// Search searches pairs and tokens
func (h *Handler) Search(c *gin.Context) {
	query := c.Query("q")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "query required"})
		return
	}

	ctx := c.Request.Context()
	results, err := h.pairRepo.Search(ctx, query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  results,
		"count": len(results),
	})
}

// CreateOrder creates a new order
func (h *Handler) CreateOrder(c *gin.Context) {
	var req models.CreateOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Debug: log the incoming request
	log.Printf("[DEBUG] CreateOrder request: side=%s, amount=%s, price=%s, amountOutMin=%s, amountInDecimals=%d, amountOutDecimals=%d",
		req.Side, req.Amount.String(), req.Price.String(), req.AmountOutMin.String(), req.AmountInDecimals, req.AmountOutDecimals)

	// Get context first
	ctx := c.Request.Context()

	// Get user from wallet address - look up or create user based on maker address
	var userID uint
	if uid, exists := c.Get("user_id"); exists {
		if uidUint, ok := uid.(uint); ok {
			userID = uidUint
		}
	} else if req.Maker != "" {
		// Try to get or create user by maker address
		user, err := h.userRepo.GetOrCreate(ctx, req.Maker)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get user"})
			return
		}
		userID = user.ID
	} else {
		// Fallback to guest user
		guestUser, err := h.userRepo.GetOrCreateGuestUser(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get user"})
			return
		}
		userID = guestUser.ID
	}

	// Validate pair exists
	pair, err := h.pairRepo.GetByID(ctx, req.PairID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid pair"})
		return
	}

	// Validate time-in-force
	if err := engine.ValidateTimeInForce(req.TimeInForce, req.OrderType); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate post-only order
	if req.OrderType == models.OrderTypePostOnly {
		tempOrder := &models.Order{
			PairID:     req.PairID,
			Side:       req.Side,
			Price:      req.Price,
			IsPostOnly: true,
		}
		if h.engine.ValidatePostOnlyOrder(tempOrder) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "post_only order would immediately fill"})
			return
		}
	}

	// Determine token in/out based on side - use request values if provided
	var tokenIn, tokenOut string
	if req.TokenIn != "" && req.TokenOut != "" {
		tokenIn = req.TokenIn
		tokenOut = req.TokenOut
	} else if req.Side == models.OrderSideBuy {
		tokenIn = pair.QuoteToken
		tokenOut = pair.BaseToken
	} else {
		tokenIn = pair.BaseToken
		tokenOut = pair.QuoteToken
	}

	// Use AmountOutMin from request - it's already calculated by frontend with proper token decimals
	// The frontend sends amounts in the token's native decimals (e.g., 9 for CREPE, 18 for WBNB)
	// We should NOT recalculate here as it would lose the decimal precision
	amountOutMin := req.AmountOutMin

	log.Printf("CreateOrder: side=%s, amount=%s, price=%s, amountOutMin=%s, amountInDecimals=%d, amountOutDecimals=%d",
		req.Side, req.Amount.StringFixed(8), req.Price.StringFixed(8), amountOutMin.StringFixed(8),
		req.AmountInDecimals, req.AmountOutDecimals)

	order := &models.Order{
		UserID:            userID,
		Network:           req.Network,
		PairID:            req.PairID,
		Side:              req.Side,
		OrderType:         req.OrderType,
		Price:             req.Price,
		Amount:            req.Amount, // Base token amount
		FilledAmount:      decimal.Zero,
		AmountIn:          req.AmountIn, // TokenIn amount (quote for buy, base for sell)
		AmountOutMin:      amountOutMin,
		TokenIn:           tokenIn,
		TokenOut:          tokenOut,
		Receiver:          req.Receiver,
		Maker:             req.Maker,
		Signature:         req.Signature,
		Expiration:        req.Expiration,
		Status:            models.OrderStatusPending,
		IsLadder:          req.IsLadder,
		IsPostOnly:        req.OrderType == models.OrderTypePostOnly,
		TriggerPrice:      req.TriggerPrice,
		ReduceOnly:        req.ReduceOnly,
		TimeInForce:       req.TimeInForce,
		DepositMemo:       req.DepositMemo,
		DepositAmount:     req.DepositAmount,
		DepositTokenMint:  req.DepositTokenMint,
		DepositType:       req.DepositType,
		DepositTxHash:     req.DepositTxHash,
		AmountInDecimals:  req.AmountInDecimals,
		AmountOutDecimals: req.AmountOutDecimals,
	}

	// Use provided order hash or generate one
	if req.OrderHash != "" {
		order.OrderHash = req.OrderHash
	}

	if req.IsLadder && req.LadderConfig != nil {
		order.IsLadder = true
		order.LadderLevels = req.LadderConfig.Levels
		order.LadderPriceStart = req.LadderConfig.PriceStart
		order.LadderPriceEnd = req.LadderConfig.PriceEnd
	}

	// Use provided nonce/salt or generate new ones
	if req.Nonce != nil {
		order.Nonce = *req.Nonce
	} else {
		order.Nonce = uint64(time.Now().UnixNano())
	}
	if req.Salt != nil {
		order.Salt = *req.Salt
	} else {
		order.Salt = uint64(time.Now().UnixNano())
	}

	// Preserve provided signed order hash when available
	if req.OrderHash == "" {
		order.OrderHash = generateOrderHash(order)
	}

	// Create the parent order first (needed for ladder child orders)
	if err := h.orderRepo.Create(ctx, order); err != nil {
		log.Printf("CreateOrder: failed to create order: %v request=%+v", err, req)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create order: " + err.Error()})
		return
	}

	// Handle ladder orders - split into child orders
	if req.IsLadder && req.LadderConfig != nil && req.LadderConfig.Levels > 1 {
		levels := req.LadderConfig.Levels
		priceStart := req.LadderConfig.PriceStart
		priceEnd := req.LadderConfig.PriceEnd

		// Calculate price step
		priceStep := priceEnd.Sub(priceStart).Div(decimal.NewFromFloat(float64(levels - 1)))

		// For ladder orders, distribute amounts based on order side
		// SELL: split base token (GIF) equally, amount_out_min = amount × price
		// BUY: split quote token (WBNB) equally, amount_out_min = amount / price
		var amountPerLevel, totalAmountIn decimal.Decimal

		if req.Side == models.OrderSideBuy {
			// For BUY: distribute the total quote amount equally across levels
			totalAmountIn = req.AmountIn // Total quote token to pay
			amountPerLevel = totalAmountIn.Div(decimal.NewFromFloat(float64(levels)))
		} else {
			// For SELL: distribute the base token amount equally across levels
			totalAmountIn = req.Amount // Total base token to sell
			amountPerLevel = totalAmountIn.Div(decimal.NewFromFloat(float64(levels)))
		}

		var childOrders []*models.Order

		// Calculate total amount out min for display purposes
		var totalAmountOutMin decimal.Decimal

		for i := 0; i < levels; i++ {
			levelPrice := priceStart.Add(priceStep.Mul(decimal.NewFromFloat(float64(i))))
			if i == levels-1 {
				// Last level gets any remainder
				levelPrice = priceEnd
			}

			var levelAmountIn, levelAmountOutMin decimal.Decimal

			if req.Side == models.OrderSideBuy {
				// For BUY: amountIn is quote token per level (WBNB)
				// amountOutMin is base token per level (GIF) at this level price
				levelAmountIn = amountPerLevel
				levelAmountOutMin = amountPerLevel.Div(levelPrice)
			} else {
				// For SELL: amountIn is base token per level (GIF)
				// amountOutMin is quote token per level (WBNB) at this level price
				levelAmountIn = amountPerLevel
				levelAmountOutMin = amountPerLevel.Mul(levelPrice)
			}

			// Accumulate total amount out min
			totalAmountOutMin = totalAmountOutMin.Add(levelAmountOutMin)

			amountForOrder := levelAmountOutMin
			if req.Side == models.OrderSideSell {
				amountForOrder = levelAmountIn
			}

			childOrder := &models.Order{
				UserID:            userID,
				Network:           req.Network,
				PairID:            req.PairID,
				Side:              req.Side,
				OrderType:         req.OrderType,
				Price:             levelPrice,
				Amount:            amountForOrder,
				FilledAmount:      decimal.Zero,
				AmountIn:          levelAmountIn,
				AmountOutMin:      levelAmountOutMin,
				TokenIn:           order.TokenIn,
				TokenOut:          order.TokenOut,
				Receiver:          order.Receiver,
				Maker:             order.Maker,
				Signature:         order.Signature,
				Expiration:        req.Expiration,
				Nonce:             order.Nonce + uint64(i+1),
				Salt:              order.Salt + uint64(i+1),
				Status:            models.OrderStatusPending,
				IsLadder:          false,
				LadderParentID:    &order.ID,
				LadderLevels:      order.LadderLevels,
				LadderPriceStart:  order.LadderPriceStart,
				LadderPriceEnd:    order.LadderPriceEnd,
				TriggerPrice:      req.TriggerPrice,
				ReduceOnly:        req.ReduceOnly,
				TimeInForce:       req.TimeInForce,
				AmountInDecimals:  order.AmountInDecimals,
				AmountOutDecimals: order.AmountOutDecimals,
			}
			childOrder.OrderHash = generateOrderHash(childOrder)
			childOrders = append(childOrders, childOrder)
		}

		// Create child orders in batch
		if err := h.orderRepo.CreateBatch(ctx, childOrders); err != nil {
			log.Printf("CreateOrder: failed to create ladder child orders: %v", err)
			// Parent order exists, but child orders failed
		} else {
			log.Printf("CreateOrder: created %d ladder child orders for parent %d", len(childOrders), order.ID)
			order.LadderLevels = levels
			// Store total amounts for display purposes before resetting to 0
			// The parent order won't be filled, only child orders will
			order.LadderTotalAmountIn = totalAmountIn
			order.LadderTotalAmountOutMin = totalAmountOutMin
			order.Amount = decimal.Zero
			order.AmountIn = decimal.Zero
			order.AmountOutMin = decimal.Zero
			order.Price = decimal.Zero
			if err := h.orderRepo.Update(ctx, order); err != nil {
				log.Printf("CreateOrder: failed to update parent order amounts: %v", err)
			}

			// Update orderbook after creating child orders so they appear immediately
			h.engine.UpdateOrderBook(ctx, req.PairID)
			if h.cache != nil {
				if err := h.cache.RefreshPair(ctx, req.PairID, h.buildPairResponseJSON); err != nil {
					log.Printf("[Cache] failed to refresh pair after ladder child creation %s: %v", req.PairID, err)
				}
			}
			// Broadcast immediately after child orders are created
			h.hub.BroadcastOrderbookUpdate(req.PairID)
			h.BroadcastLiquidity(c.Request.Context(), req.PairID)

			// Match each child ladder order
			for _, childOrder := range childOrders {
				log.Printf("[Ladder] Matching child order ID=%d, side=%s, amount=%s, price=%s",
					childOrder.ID, childOrder.Side, childOrder.Amount.String(), childOrder.Price.String())

				result, err := h.engine.MatchOrder(ctx, childOrder)
				if err != nil {
					log.Printf("CreateOrder: ladder child order matching failed: %v", err)
				} else if len(result.Fills) > 0 {
					log.Printf("[Ladder] SUCCESS! Child order ID=%d matched, fills=%d, remaining=%s",
						childOrder.ID, len(result.Fills), result.Remaining.String())

					// Save fills and update orders
					for _, fill := range result.Fills {
						fill.Network = order.Network
						fill.PairID = order.PairID
						fill.OrderID = childOrder.ID
						fill.TokenIn = childOrder.TokenIn
						fill.TokenOut = childOrder.TokenOut
						fill.CreatedAt = time.Now()
						fill.Status = "pending"

						if err := h.fillRepo.Create(ctx, &fill); err != nil {
							log.Printf("CreateOrder: failed to create ladder fill: %v", err)
						}

						// ── Price tracking (ladder) ──────────────────────────────
						go func(pairID string, price decimal.Decimal) {
							bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							if err := h.pairRepo.UpdateLastTradePrice(bgCtx, pairID, price); err != nil {
								log.Printf("[PriceTrack] ladder: failed to update last_trade_price for %s: %v", pairID, err)
							}
							if h.hub != nil {
								h.hub.BroadcastPriceUpdate(websocket.PriceUpdate{
									PairID:         pairID,
									LastTradePrice: price.String(),
									LastTradeAt:    time.Now().Unix(),
									Source:         "trade",
								})
							}
						}(fill.PairID, fill.Price)
						// ─────────────────────────────────────────────────────────

						// Update maker order
						makerOrder, err := h.orderRepo.GetByID(ctx, fill.MakerOrderID)
						if err == nil {
							makerOrder.FilledAmount = makerOrder.FilledAmount.Add(fill.Amount)
							if makerOrder.FilledAmount.GreaterThanOrEqual(makerOrder.Amount) {
								makerOrder.Status = models.OrderStatusFilled
							} else {
								makerOrder.Status = models.OrderStatusPartial
							}
							h.orderRepo.Update(ctx, makerOrder)
							// Broadcast order update to websocket clients
							if h.hub != nil {
								h.hub.BroadcastOrderUpdate(int64(makerOrder.ID), makerOrder)
							}
						}
					}

					// Update child order status
					if result.Status == models.OrderStatusFilled {
						childOrder.Status = models.OrderStatusPartial
						childOrder.FilledAmount = childOrder.Amount
					} else if result.Status == models.OrderStatusPartial {
						childOrder.Status = models.OrderStatusPartial
						childOrder.FilledAmount = childOrder.Amount.Sub(result.Remaining)
					}
					h.orderRepo.Update(ctx, childOrder)
				}
			}
		}
	}

	// Update order book
	h.engine.UpdateOrderBook(ctx, req.PairID)
	if h.cache != nil {
		if err := h.cache.RefreshPair(ctx, req.PairID, h.buildPairResponseJSON); err != nil {
			log.Printf("[Cache] failed to refresh pair after order creation %s: %v", req.PairID, err)
		}
	}

	// Broadcast orderbook update immediately after storage
	h.hub.BroadcastOrderbookUpdate(req.PairID)
	h.BroadcastLiquidity(c.Request.Context(), req.PairID)

	// Try to match the order immediately (for limit/market orders)
	if order.Status == models.OrderStatusPending && !order.IsLadder {
		log.Printf("[Matching] Attempting to match order ID=%d, side=%s, amount=%s, price=%s",
			order.ID, order.Side, order.Amount.String(), order.Price.String())

		ob := h.engine.GetOrderBook(order.PairID)
		if ob != nil {
			log.Printf("[Matching] OrderBook: asks=%d, bids=%d", len(ob.Asks), len(ob.Bids))
			// For SELL, check bids (buy orders) - sell price <= bid price = match
			// For BUY, check asks (sell orders) - buy price >= ask price = match
			if order.Side == models.OrderSideSell && len(ob.Bids) > 0 {
				log.Printf("[Matching] Checking against %d bids (buy orders)", len(ob.Bids))
				for i, bid := range ob.Bids {
					if i < 2 {
						match := order.Price.LessThanOrEqual(bid.Price)
						log.Printf("[Matching] Bid[%d] price=%s, takerSellPrice=%s, match=%v",
							i, bid.Price.String(), order.Price.String(), match)
					}
				}
			}
			if order.Side == models.OrderSideBuy && len(ob.Asks) > 0 {
				log.Printf("[Matching] Checking against %d asks (sell orders)", len(ob.Asks))
				for i, ask := range ob.Asks {
					if i < 2 {
						match := order.Price.GreaterThanOrEqual(ask.Price)
						log.Printf("[Matching] Ask[%d] price=%s, takerBuyPrice=%s, match=%v",
							i, ask.Price.String(), order.Price.String(), match)
					}
				}
			}
		} else {
			log.Printf("[Matching] No orderbook found for pair=%s", order.PairID)
		}

		result, err := h.engine.MatchOrder(ctx, order)
		if err != nil {
			log.Printf("CreateOrder: matching failed: %v", err)
		} else if len(result.Fills) > 0 {
			log.Printf("[Matching] SUCCESS! Found %d matches, remaining=%s", len(result.Fills), result.Remaining.String())

			// Track total filled amount for the taker order
			totalFilledAmount := decimal.Zero

			// Save fills to database
			for _, fill := range result.Fills {
				fill.Network = order.Network
				fill.PairID = order.PairID
				fill.OrderID = order.ID // Required for foreign key constraint
				fill.TokenIn = order.TokenIn
				fill.TokenOut = order.TokenOut
				fill.CreatedAt = time.Now()
				fill.Status = "pending"

				if err := h.fillRepo.Create(ctx, &fill); err != nil {
					log.Printf("CreateOrder: failed to create fill: %v", err)
				}

				// ── Price tracking ──────────────────────────────────────────────
				// Write last_trade_price to the pairs table immediately on every fill.
				// This is what the UI reads to show "your exchange" price vs gecko price.
				go func(pairID string, price decimal.Decimal) {
					bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := h.pairRepo.UpdateLastTradePrice(bgCtx, pairID, price); err != nil {
						log.Printf("[PriceTrack] failed to update last_trade_price for %s: %v", pairID, err)
					}
					// Broadcast lightweight price_update over WebSocket so the UI flips instantly
					if h.hub != nil {
						h.hub.BroadcastPriceUpdate(websocket.PriceUpdate{
							PairID:         pairID,
							LastTradePrice: price.String(),
							LastTradeAt:    time.Now().Unix(),
							Source:         "trade",
						})
					}
				}(fill.PairID, fill.Price)
				// ────────────────────────────────────────────────────────────────

				// Track filled amount for taker order
				totalFilledAmount = totalFilledAmount.Add(fill.Amount)

				// Update maker order filled amount
				makerOrder, err := h.orderRepo.GetByID(ctx, fill.MakerOrderID)
				if err == nil {
					makerOrder.FilledAmount = makerOrder.FilledAmount.Add(fill.Amount)
					if makerOrder.FilledAmount.GreaterThanOrEqual(makerOrder.Amount) {
						makerOrder.Status = models.OrderStatusFilled
					} else {
						makerOrder.Status = models.OrderStatusPartial
					}
					h.orderRepo.Update(ctx, makerOrder)
					// Broadcast order update to websocket clients
					if h.hub != nil {
						h.hub.BroadcastOrderUpdate(int64(makerOrder.ID), makerOrder)
					}
				}

				log.Printf("CreateOrder: fill created - maker: %s, taker: %d, price: %s, amount: %s",
					fill.Maker, fill.TakerOrderID, fill.Price.String(), fill.Amount.String())
				// Note: Broadcast of trade update is intentionally skipped here; settlement executor will broadcast after on-chain confirmation.
				// This placeholder ensures code structure remains valid.
				// (No immediate UI update)
			}

			// Update taker order status and filled amount
			order.FilledAmount = totalFilledAmount
			if order.FilledAmount.GreaterThanOrEqual(order.Amount) {
				order.Status = models.OrderStatusFilled
			} else {
				order.Status = models.OrderStatusPartial
			}
			if err := h.orderRepo.Update(ctx, order); err != nil {
				log.Printf("CreateOrder: failed to update taker order status: %v", err)
			}
			// Broadcast order update for taker order (the order being created)
			if h.hub != nil {
				h.hub.BroadcastOrderUpdate(int64(order.ID), order)
			}
		}
	}

	// Refresh cache again after matching and fills
	if h.cache != nil {
		if err := h.cache.RefreshPair(ctx, req.PairID, h.buildPairResponseJSON); err != nil {
			log.Printf("[Cache] failed to refresh pair after matching %s: %v", req.PairID, err)
		}
	}

	// Broadcast orderbook and liquidity for this pair after matching (final update)
	h.hub.BroadcastOrderbookUpdate(req.PairID)
	h.BroadcastLiquidity(c.Request.Context(), req.PairID)

	c.JSON(http.StatusCreated, gin.H{
		"data": order,
	})
}

// GetOrders returns user's orders (requires auth or address query param)
func (h *Handler) GetOrders(c *gin.Context) {
	var userID uint
	var err error

	// Try to get user from auth token first
	if uid, exists := c.Get("user_id"); exists {
		userID = uid.(uint)
	} else if address := c.Query("address"); address != "" {
		// Try to get user by address for public access
		user, err := h.userRepo.GetByAddress(c.Request.Context(), address)
		if err != nil || user == nil {
			c.JSON(http.StatusOK, gin.H{"data": []models.Order{}, "count": 0})
			return
		}
		userID = user.ID
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "authorization required"})
		return
	}

	limit := 50
	offset := 0

	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	if o := c.Query("offset"); o != "" {
		fmt.Sscanf(o, "%d", &offset)
	}

	status := c.Query("status")
	pairID := c.Query("pair")

	ctx := c.Request.Context()
	orders, err := h.orderRepo.GetByUserIDFilter(ctx, userID, limit, offset, pairID, status)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Convert orders to include pair info - batch load for performance
	response := h.convertOrdersToWithPairBatch(ctx, orders)

	c.JSON(http.StatusOK, gin.H{
		"data":  response,
		"count": len(response),
	})
}

// GetOpenOrders returns user's active (pending/partial) orders
func (h *Handler) GetOpenOrders(c *gin.Context) {
	var userID uint
	var err error

	// Try to get user from auth token first
	if uid, exists := c.Get("user_id"); exists {
		userID = uid.(uint)
	} else if address := c.Query("address"); address != "" {
		user, err := h.userRepo.GetByAddress(c.Request.Context(), address)
		if err != nil || user == nil {
			c.JSON(http.StatusOK, gin.H{"data": []models.Order{}, "count": 0})
			return
		}
		userID = user.ID
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "authorization required"})
		return
	}

	limit := 50
	offset := 0

	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	if o := c.Query("offset"); o != "" {
		fmt.Sscanf(o, "%d", &offset)
	}

	pairID := c.Query("pair")
	network := c.Query("network")

	ctx := c.Request.Context()
	var orders []models.Order

	if pairID != "" {
		orders, err = h.orderRepo.GetByUserIDFilter(ctx, userID, limit, offset, pairID, "")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// Filter for open orders only (pending, partial, open)
		filtered := make([]models.Order, 0)
		for _, o := range orders {
			if o.Status == models.OrderStatusPending || o.Status == models.OrderStatusPartial || o.Status == models.OrderStatusOpen {
				// Filter by network if specified
				if network != "" && string(o.Network) != network {
					continue
				}
				filtered = append(filtered, o)
			}
		}
		orders = filtered
	} else {
		orders, err = h.orderRepo.GetActiveOrdersByUser(ctx, userID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// Filter by network if specified
		if network != "" {
			filtered := make([]models.Order, 0)
			for _, o := range orders {
				if string(o.Network) == network {
					filtered = append(filtered, o)
				}
			}
			orders = filtered
		}
	}

	// Convert orders to include pair info - batch load for performance
	response := h.convertOrdersToWithPairBatch(ctx, orders)

	c.JSON(http.StatusOK, gin.H{
		"data":  response,
		"count": len(response),
	})
}

// GetHistoryOrders returns user's historical orders (filled/cancelled/expired)
func (h *Handler) GetHistoryOrders(c *gin.Context) {
	var userID uint
	var err error

	// Try to get user from auth token first
	if uid, exists := c.Get("user_id"); exists {
		userID = uid.(uint)
	} else if address := c.Query("address"); address != "" {
		user, err := h.userRepo.GetByAddress(c.Request.Context(), address)
		if err != nil || user == nil {
			c.JSON(http.StatusOK, gin.H{"data": []models.Order{}, "count": 0})
			return
		}
		userID = user.ID
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "authorization required"})
		return
	}

	limit := 50
	offset := 0

	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	if o := c.Query("offset"); o != "" {
		fmt.Sscanf(o, "%d", &offset)
	}

	pairID := c.Query("pair")
	network := c.Query("network")

	ctx := c.Request.Context()
	var orders []models.Order

	if pairID != "" {
		orders, err = h.orderRepo.GetByUserIDFilter(ctx, userID, limit, offset, pairID, "")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// Filter for historical orders only (filled, cancelled, expired, triggered)
		filtered := make([]models.Order, 0)
		for _, o := range orders {
			if o.Status == models.OrderStatusFilled || o.Status == models.OrderStatusCancelled || o.Status == models.OrderStatusExpired || o.Status == models.OrderStatusTriggered {
				// Filter by network if specified
				if network != "" && string(o.Network) != network {
					continue
				}
				filtered = append(filtered, o)
			}
		}
		orders = filtered
	} else {
		orders, err = h.orderRepo.GetHistoryOrders(ctx, userID, limit, offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// Filter by network if specified
		if network != "" {
			filtered := make([]models.Order, 0)
			for _, o := range orders {
				if string(o.Network) == network {
					filtered = append(filtered, o)
				}
			}
			orders = filtered
		}
	}

	// Convert orders to include pair info - batch load for performance
	response := h.convertOrdersToWithPairBatch(ctx, orders)

	c.JSON(http.StatusOK, gin.H{
		"data":  response,
		"count": len(response),
	})
}

// GetOrder returns a single order
func (h *Handler) GetOrder(c *gin.Context) {
	id := c.Param("id")
	var orderID uint
	fmt.Sscanf(id, "%d", &orderID)

	ctx := c.Request.Context()
	order, err := h.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
		return
	}

	c.JSON(http.StatusOK, order)
}

// CancelOrder cancels an order
func (h *Handler) CancelOrder(c *gin.Context) {
	id := c.Param("id")
	var orderID uint
	fmt.Sscanf(id, "%d", &orderID)

	ctx := c.Request.Context()

	order, err := h.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
		return
	}

	// Check authorization - either via auth token or address query param
	var userID uint
	if uid, exists := c.Get("user_id"); exists {
		userID = uid.(uint)
		log.Printf("[CancelOrder] Using user_id from auth token: %d", userID)
	} else if address := c.Query("address"); address != "" {
		// Try to get or create user by address
		log.Printf("[CancelOrder] Looking up user by address: %s", address)
		user, err := h.userRepo.GetOrCreate(ctx, address)
		if err != nil {
			log.Printf("[CancelOrder] GetOrCreate failed: %v", err)
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		userID = user.ID
		log.Printf("[CancelOrder] Got/created user ID: %d for address: %s", userID, address)
	} else {
		log.Printf("[CancelOrder] No auth and no address query param")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	if order.UserID != userID {
		c.JSON(http.StatusForbidden, gin.H{"error": "not your order"})
		return
	}

	if order.Status != models.OrderStatusPending && order.Status != models.OrderStatusPartial {
		c.JSON(http.StatusBadRequest, gin.H{"error": "order cannot be cancelled"})
		return
	}

	orderIDsToCancel := []uint{orderID}

	// If this is a ladder parent order, also cancel all child orders
	if order.IsLadder && order.LadderParentID == nil {
		children, err := h.orderRepo.GetLadderChildren(ctx, orderID)
		if err == nil && len(children) > 0 {
			for _, child := range children {
				orderIDsToCancel = append(orderIDsToCancel, child.ID)
			}
		}
	}

	if err := h.orderRepo.BatchCancel(ctx, orderIDsToCancel); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Create refund requests for cancelled orders
	if h.refundService != nil {
		for _, oid := range orderIDsToCancel {
			cancelledOrder, err := h.orderRepo.GetByID(ctx, oid)
			if err != nil {
				log.Printf("[CancelOrder] Failed to get cancelled order %d for refund: %v", oid, err)
				continue
			}

			if err := h.refundService.CreateRefundForOrder(ctx, cancelledOrder); err != nil {
				log.Printf("[CancelOrder] Failed to create refund for order %d: %v", oid, err)
				// Don't fail the cancellation if refund creation fails
			}
		}
	}

	h.engine.UpdateOrderBook(ctx, order.PairID)
	if h.cache != nil {
		if err := h.cache.RefreshPair(ctx, order.PairID, h.buildPairResponseJSON); err != nil {
			log.Printf("[Cache] failed to refresh pair after order cancellation %s: %v", order.PairID, err)
		}
	}
	h.hub.BroadcastOrderbookUpdate(order.PairID)
	h.BroadcastLiquidity(ctx, order.PairID)

	c.JSON(http.StatusOK, gin.H{
		"message":         "order cancelled",
		"cancelled_ids":   orderIDsToCancel,
		"cancelled_count": len(orderIDsToCancel),
	})
}

// BatchCancelOrders cancels multiple orders
func (h *Handler) BatchCancelOrders(c *gin.Context) {
	var req struct {
		OrderIDs []uint `json:"order_ids" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	userID, _ := c.Get("user_id")
	ctx := c.Request.Context()

	// Verify ownership
	orders, err := h.orderRepo.GetByIDs(ctx, req.OrderIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ownIDs := make([]uint, 0)
	pairIDs := make(map[string]bool)
	for _, o := range orders {
		if o.UserID == userID.(uint) {
			if o.Status == models.OrderStatusPending || o.Status == models.OrderStatusPartial {
				ownIDs = append(ownIDs, o.ID)
				pairIDs[o.PairID] = true
			}
		}
	}

	if len(ownIDs) > 0 {
		h.orderRepo.BatchCancel(ctx, ownIDs)

		// Create refund requests for cancelled orders
		if h.refundService != nil {
			for _, oid := range ownIDs {
				cancelledOrder, err := h.orderRepo.GetByID(ctx, oid)
				if err != nil {
					log.Printf("[BatchCancelOrders] Failed to get cancelled order %d for refund: %v", oid, err)
					continue
				}

				if err := h.refundService.CreateRefundForOrder(ctx, cancelledOrder); err != nil {
					log.Printf("[BatchCancelOrders] Failed to create refund for order %d: %v", oid, err)
					// Don't fail the cancellation if refund creation fails
				}
			}
		}

		for pairID := range pairIDs {
			h.engine.UpdateOrderBook(ctx, pairID)
			if h.cache != nil {
				if err := h.cache.RefreshPair(ctx, pairID, h.buildPairResponseJSON); err != nil {
					log.Printf("[Cache] failed to refresh pair after batch cancellation %s: %v", pairID, err)
				}
			}
			h.hub.BroadcastOrderbookUpdate(pairID)
			h.BroadcastLiquidity(ctx, pairID)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "orders cancelled",
		"cancelled":     len(ownIDs),
		"total_request": len(req.OrderIDs),
	})
}

// GetFills returns user's fills
func (h *Handler) GetFills(c *gin.Context) {
	userID, _ := c.Get("user_id")
	limit := 50
	offset := 0

	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	if o := c.Query("offset"); o != "" {
		fmt.Sscanf(o, "%d", &offset)
	}

	ctx := c.Request.Context()
	fills, err := h.fillRepo.GetByUserID(ctx, userID.(uint), limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  fills,
		"count": len(fills),
	})
}

// GetFill returns a single fill
func (h *Handler) GetFill(c *gin.Context) {
	id := c.Param("id")
	var fillID uint
	fmt.Sscanf(id, "%d", &fillID)

	ctx := c.Request.Context()
	fill, err := h.fillRepo.GetByID(ctx, fillID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "fill not found"})
		return
	}

	c.JSON(http.StatusOK, fill)
}

// GetFillsByAddress returns fills for a given wallet address
func (h *Handler) GetFillsByAddress(c *gin.Context) {
	address := c.Param("address")
	limit := 50
	offset := 0

	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	if o := c.Query("offset"); o != "" {
		fmt.Sscanf(o, "%d", &offset)
	}

	ctx := c.Request.Context()
	fills, err := h.fillRepo.GetByMakerAddress(ctx, address, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Convert fills to include human-readable amounts
	type FillResponse struct {
		models.Fill
		AmountHuman string `json:"amount_human"`
		PriceHuman  string `json:"price_human"`
		BaseSymbol  string `json:"base_symbol"`
		QuoteSymbol string `json:"quote_symbol"`
	}

	responses := make([]FillResponse, len(fills))
	for i, fill := range fills {
		// Use the order side to get the correct amount decimals for the base token amount
		var amountDecimals int
		if order, err := h.orderRepo.GetByID(ctx, fill.OrderID); err == nil && order != nil {
			if order.Side == models.OrderSideBuy {
				amountDecimals = int(order.AmountOutDecimals)
			} else {
				amountDecimals = int(order.AmountInDecimals)
			}
		} else {
			// Fallback to token lookup for the base token
			if token, err := h.pairRepo.GetTokenByAddress(ctx, fill.TokenOut); err == nil && token != nil {
				amountDecimals = token.Decimals
			} else {
				amountDecimals = 18 // fallback
			}
		}

		// Format amount (base token amount)
		amountHuman := convertWeiToHuman(fill.Amount, amountDecimals)

		// Format price: price is already a calculated ratio, preserve precision and trim trailing zeros
		var priceHuman string
		if fill.Price.IsZero() {
			priceHuman = "0"
		} else {
			priceHuman = fill.Price.String()
			priceHuman = strings.TrimRight(priceHuman, "0")
			priceHuman = strings.TrimRight(priceHuman, ".")
		}

		// Get token symbols from pair
		var baseSymbol, quoteSymbol string
		if pair, err := h.pairRepo.GetByID(ctx, fill.PairID); err == nil && pair != nil {
			// Extract symbols from pair JSON
			var baseTokenData, quoteTokenData map[string]interface{}
			if json.Unmarshal([]byte(pair.BaseToken), &baseTokenData) == nil {
				if s, ok := baseTokenData["symbol"].(string); ok {
					baseSymbol = s
				}
			}
			if json.Unmarshal([]byte(pair.QuoteToken), &quoteTokenData) == nil {
				if s, ok := quoteTokenData["symbol"].(string); ok {
					quoteSymbol = s
				}
			}
		}

		// Fallback to token table if symbols not found in pair
		if baseSymbol == "" {
			if token, err := h.pairRepo.GetTokenByAddress(ctx, fill.TokenOut); err == nil && token != nil {
				baseSymbol = token.Symbol
			}
		}
		if quoteSymbol == "" {
			if token, err := h.pairRepo.GetTokenByAddress(ctx, fill.TokenIn); err == nil && token != nil {
				quoteSymbol = token.Symbol
			}
		}

		responses[i] = FillResponse{
			Fill:        fill,
			AmountHuman: amountHuman,
			PriceHuman:  priceHuman,
			BaseSymbol:  baseSymbol,
			QuoteSymbol: quoteSymbol,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  responses,
		"count": len(responses),
	})
}

// CommitOrder commits an order (commit-reveal)
func (h *Handler) CommitOrder(c *gin.Context) {
	var req struct {
		CommitHash string `json:"commit_hash" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Store commit hash in database (will be revealed later)
	// For now, just acknowledge the commit
	c.JSON(http.StatusOK, gin.H{
		"message":     "commit stored",
		"commit_hash": req.CommitHash,
	})
}

// RevealOrder reveals an order (commit-reveal)
func (h *Handler) RevealOrder(c *gin.Context) {
	var req struct {
		CommitHash string       `json:"commit_hash" binding:"required"`
		Order      models.Order `json:"order" binding:"required"`
		Signature  string       `json:"signature" binding:"required"`
		Secret     string       `json:"secret" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Verify secret matches commit hash
	// Then create the order
	ctx := c.Request.Context()

	userID, _ := c.Get("user_id")
	order := req.Order
	order.UserID = userID.(uint)
	if order.OrderHash == "" {
		order.OrderHash = generateOrderHash(&order)
	}
	order.Signature = req.Signature
	order.CommitHash = req.CommitHash
	order.CommitRevealed = true
	order.Status = models.OrderStatusPending

	if err := h.orderRepo.Create(ctx, &order); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Broadcast the newly created order
	if h.hub != nil {
		h.hub.BroadcastOrderUpdate(int64(order.ID), order)
	}

	h.engine.UpdateOrderBook(ctx, order.PairID)
	h.hub.BroadcastOrderbookUpdate(order.PairID)
	h.BroadcastLiquidity(ctx, order.PairID)

	c.JSON(http.StatusCreated, gin.H{
		"data": &order,
	})
}

// GetProfile returns user profile
func (h *Handler) GetProfile(c *gin.Context) {
	userID, _ := c.Get("user_id")
	ctx := c.Request.Context()

	user, err := h.userRepo.GetByID(ctx, userID.(uint))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}

	c.JSON(http.StatusOK, user)
}

// UpdateProfile updates user profile
func (h *Handler) UpdateProfile(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		Email    string `json:"email"`
		Username string `json:"username"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()
	err := h.userRepo.UpdateProfile(ctx, userID.(uint), req.Email, req.Username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "profile updated"})
}

// GetBalances returns user token balances
func (h *Handler) GetBalances(c *gin.Context) {
	userID, _ := c.Get("user_id")
	ctx := c.Request.Context()

	user, err := h.userRepo.GetByID(ctx, userID.(uint))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}

	balances, err := h.ethService.GetUserBalances(ctx, user.Address)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": balances,
	})
}

// HandleWebSocket handles WebSocket connections
func (h *Handler) HandleWebSocket(c *gin.Context) {
	pairID := c.Query("pair")
	if pairID == "" {
		pairID = "all"
	}

	conn, err := websocket.Upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[WebSocket] upgrade failed for pair=%s remote=%s error=%v", pairID, c.ClientIP(), err)
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	log.Printf("[WebSocket] connected pair=%s remote=%s", pairID, c.ClientIP())

	client := &websocket.Client{
		Hub:    h.hub,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		PairID: pairID,
	}

	h.hub.Register <- client

	go client.WritePump()
	go client.ReadPump()
}

// GetWebSocketDebugLogs returns recent websocket hub log events
func (h *Handler) GetWebSocketDebugLogs(c *gin.Context) {
	logs := h.hub.GetDebugLogs()
	c.JSON(http.StatusOK, gin.H{"logs": logs})
}

// AuthMiddleware handles authentication
func (h *Handler) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authorization required"})
			c.Abort()
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == authHeader {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization format"})
			c.Abort()
			return
		}

		// Verify JWT token
		userID, err := h.authService.VerifyToken(token)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			c.Abort()
			return
		}

		c.Set("user_id", userID)
		c.Next()
	}
}

// Helper functions
func generateOrderHash(order *models.Order) string {
	data := fmt.Sprintf("%s:%s:%s:%s:%s:%d:%d:%d",
		order.Maker,
		order.TokenIn,
		order.TokenOut,
		order.Amount.String(),
		order.Price.String(),
		order.Expiration.Unix(),
		order.Nonce,
		order.Salt,
	)
	hash := sha256.Sum256([]byte(data))
	return "0x" + hex.EncodeToString(hash[:])
}

// GetPairCandles returns OHLCV data for a pair.
// Source priority:
//  1. Fills table  — real trades executed on this DEX (most accurate for your exchange)
//  2. Candles table — pre-fetched GeckoTerminal data stored by ohlcv-worker (reference history)
//
// Query params:
//   prefer=fills  → fills table only
//   prefer=gecko  → candles table only
//   (omitted)     → waterfall: fills first, gecko fallback
//
//   currency=usd   → USD-denominated candles (default)
//   currency=token → quote-token-denominated candles (e.g. price in WBNB)
//                    only applies to gecko candles; fills are always native (token)
func (h *Handler) GetPairCandles(c *gin.Context) {
	pairID := c.Param("id")
	if pairID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "pair id is required"})
		return
	}

	resolutionStr := c.Query("resolution")
	if resolutionStr == "" {
		resolutionStr = "1m"
	}

	var resolutionSec int
	switch resolutionStr {
	case "1m":
		resolutionSec = 60
	case "5m":
		resolutionSec = 300
	case "15m":
		resolutionSec = 900
	case "1h", "60":
		resolutionSec = 3600
	case "4h":
		resolutionSec = 14400
	case "12h":
		resolutionSec = 43200
	case "1d", "1D", "D":
		resolutionSec = 86400
	default:
		resolutionSec = 60
	}

	limit := 1000
	if limitStr := c.Query("limit"); limitStr != "" {
		fmt.Sscanf(limitStr, "%d", &limit)
	}

	prefer   := c.Query("prefer")   // "fills", "gecko", or "" (waterfall)
	currency := c.Query("currency") // "usd" (default) or "token"
	if currency == "" {
		currency = "usd"
	}
	if currency != "usd" && currency != "token" {
		currency = "usd"
	}
	ctx := c.Request.Context()

	// ── prefer=fills: only fill-based candles ──────────────────────────────
	// Fills are always in native token units regardless of currency param.
	if prefer == "fills" {
		fillCandles, err := h.fillRepo.GetCandles(ctx, pairID, resolutionSec, limit)
		if err != nil {
			log.Printf("[GetPairCandles] fill candles error for %s: %v", pairID, err)
			c.Header("X-Candle-Source", "none")
			c.JSON(http.StatusOK, []models.Candle{})
			return
		}
		if len(fillCandles) > 0 {
			c.Header("X-Candle-Source", "trade")
			c.JSON(http.StatusOK, fillCandles)
		} else {
			c.Header("X-Candle-Source", "none")
			c.JSON(http.StatusOK, []models.Candle{})
		}
		return
	}

	// ── prefer=gecko: only gecko candles, respects currency param ──────────
	if prefer == "gecko" {
		if h.candleRepo != nil {
			geckoCandles, err := h.candleRepo.GetByPairAndCurrency(ctx, pairID, resolutionSec, currency, limit)
			if err != nil {
				log.Printf("[GetPairCandles] gecko candles error for %s currency=%s: %v", pairID, currency, err)
			}
			if len(geckoCandles) > 0 {
				c.Header("X-Candle-Source", "gecko")
				c.Header("X-Candle-Currency", currency)
				c.JSON(http.StatusOK, geckoCandles)
				return
			}
		}
		c.Header("X-Candle-Source", "none")
		c.JSON(http.StatusOK, []models.Candle{})
		return
	}

	// ── default waterfall: fills → gecko (with currency) → empty ─────────
	// When currency=token, skip fills (they are already native token) and go
	// straight to the token-denominated gecko candles.
	if currency == "token" {
		if h.candleRepo != nil {
			geckoCandles, err := h.candleRepo.GetByPairAndCurrency(ctx, pairID, resolutionSec, "token", limit)
			if err != nil {
				log.Printf("[GetPairCandles] token candles error for %s: %v", pairID, err)
			}
			if len(geckoCandles) > 0 {
				c.Header("X-Candle-Source", "gecko")
				c.Header("X-Candle-Currency", "token")
				c.JSON(http.StatusOK, geckoCandles)
				return
			}
		}
		c.Header("X-Candle-Source", "none")
		c.JSON(http.StatusOK, []models.Candle{})
		return
	}

	// USD waterfall (original behaviour)
	fillCandles, err := h.fillRepo.GetCandles(ctx, pairID, resolutionSec, limit)
	if err != nil {
		log.Printf("[GetPairCandles] fill candles error for %s: %v", pairID, err)
	}

	if len(fillCandles) > 0 {
		c.Header("X-Candle-Source", "trade")
		c.JSON(http.StatusOK, fillCandles)
		return
	}

	if h.candleRepo != nil {
		geckoCandles, err := h.candleRepo.GetByPair(ctx, pairID, resolutionSec, limit)
		if err != nil {
			log.Printf("[GetPairCandles] gecko candles error for %s: %v", pairID, err)
		}
		if len(geckoCandles) > 0 {
			c.Header("X-Candle-Source", "gecko")
			c.JSON(http.StatusOK, geckoCandles)
			return
		}
	}

	c.Header("X-Candle-Source", "none")
	c.JSON(http.StatusOK, []models.Candle{})
}

func (h *Handler) GetDebugCandles(c *gin.Context) {
	pairID := c.Param("id")
	resolutionSec := 60 // Default to 1m
	limit := 1000

	debugInfo, err := h.fillRepo.GetCandlesDebug(c.Request.Context(), pairID, resolutionSec, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, debugInfo)
}

