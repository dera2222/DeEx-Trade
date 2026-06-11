package engine

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"github.com/nexus/backend/internal/repository"
	"github.com/nexus/backend/internal/services"
	"github.com/shopspring/decimal"
)

type MatchingEngine struct {
	config      *config.Config
	db          *db.DB
	redis       *db.RedisClient
	orderRepo   *repository.OrderRepository
	fillRepo    *repository.FillRepository
	pairRepo    *repository.PairRepository
	balanceRepo *repository.BalanceRepository
	refundService *services.RefundService

	// Order book cache
	orderBooks map[string]*OrderBook
	mu         sync.RWMutex

	// Matching workers
	matchChan chan *MatchRequest
	quitChan  chan struct{}
}

type OrderBook struct {
	PairID   string
	Asks     []PriceLevel // Sorted ascending (lowest first)
	Bids     []PriceLevel // Sorted descending (highest first)
	Sequence int64
	mu       sync.RWMutex
}

type PriceLevel struct {
	Price  decimal.Decimal
	Orders []models.Order
	Amount decimal.Decimal
}

type MatchRequest struct {
	OrderID           uint
	PairID            string
	Side              models.OrderSide
	Amount            decimal.Decimal
	Price             decimal.Decimal // For market orders, this is the limit price
	Type              models.OrderType
	AmountInDecimals  int
	AmountOutDecimals int
	ResultChan        chan *MatchResult
}

type MatchResult struct {
	Fills     []models.Fill
	Remaining decimal.Decimal
	Status    models.OrderStatus
	Error     error
}

type Fill struct {
	MakerOrderID uint
	TakerOrderID uint
	Maker        string
	Taker        string
	Price        decimal.Decimal
	Amount       decimal.Decimal
	AmountIn     decimal.Decimal
	AmountOut    decimal.Decimal
	Side         models.OrderSide
}

// NewMatchingEngine creates a new matching engine
func NewMatchingEngine(cfg *config.Config, db *db.DB, redis *db.RedisClient, balanceRepo *repository.BalanceRepository, refundService *services.RefundService) *MatchingEngine {
	orderRepo := repository.NewOrderRepository(db, redis)
	fillRepo := repository.NewFillRepository(db, redis)
	pairRepo := repository.NewPairRepository(db, redis)

	return &MatchingEngine{
		config:        cfg,
		db:            db,
		redis:         redis,
		orderRepo:     orderRepo,
		fillRepo:      fillRepo,
		pairRepo:      pairRepo,
		balanceRepo:   balanceRepo,
		refundService: refundService,

		orderBooks: make(map[string]*OrderBook),
		matchChan:  make(chan *MatchRequest, 1000),
		quitChan:   make(chan struct{}),
	}
}

// Start starts the matching engine
func (e *MatchingEngine) Start(ctx context.Context) {
	// Load order books for all pairs
	e.loadOrderBooks(ctx)

	// Start matching worker
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[Matching] panic in runMatchingWorker: %v\n", r)
			}
		}()
		e.runMatchingWorker(ctx)
	}()

	// Start order book updater
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[Matching] panic in runOrderBookUpdater: %v\n", r)
			}
		}()
		e.runOrderBookUpdater(ctx)
	}()

	// Start expired order processor
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[Matching] panic in runExpiredOrderProcessor: %v\n", r)
			}
		}()
		e.runExpiredOrderProcessor(ctx)
	}()

	// Start price monitor for stop-loss/take-profit
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[Matching] panic in runPriceMonitor: %v\n", r)
			}
		}()
		e.runPriceMonitor(ctx)
	}()

	fmt.Println("Matching engine started")
}

// Stop stops the matching engine
func (e *MatchingEngine) Stop() {
	close(e.quitChan)
	fmt.Println("Matching engine stopped")
}

// loadOrderBooks loads order books for all active pairs
func (e *MatchingEngine) loadOrderBooks(ctx context.Context) {
	pairs, err := e.pairRepo.GetAllActive(ctx, 500)
	if err != nil {
		fmt.Printf("Failed to load pairs: %v\n", err)
		return
	}

	for _, pair := range pairs {
		// Load actual orders, not just aggregated levels
		asks, err := e.orderRepo.GetActiveByPair(ctx, pair.ID, pair.Network, models.OrderSideSell)
		if err != nil {
			fmt.Printf("Failed to load sell orders for %s: %v\n", pair.ID, err)
		}
		bids, err := e.orderRepo.GetActiveByPair(ctx, pair.ID, pair.Network, models.OrderSideBuy)
		if err != nil {
			fmt.Printf("Failed to load buy orders for %s: %v\n", pair.ID, err)
		}

		fmt.Printf("[Matching] Loaded pair=%s, asks=%d, bids=%d\n", pair.ID, len(asks), len(bids))

		e.mu.Lock()
		e.orderBooks[pair.ID] = &OrderBook{
			PairID:   pair.ID,
			Asks:     groupOrdersByPrice(asks, models.OrderSideSell),
			Bids:     groupOrdersByPrice(bids, models.OrderSideBuy),
			Sequence: time.Now().Unix(),
		}
		e.mu.Unlock()
	}

	fmt.Printf("Loaded %d order books\n", len(pairs))
}

// runMatchingWorker runs the matching worker
func (e *MatchingEngine) runMatchingWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.quitChan:
			return
		case req := <-e.matchChan:
			e.processMatchRequest(ctx, req)
		}
	}
}

// processMatchRequest processes a match request
func (e *MatchingEngine) processMatchRequest(ctx context.Context, req *MatchRequest) {
	result := &MatchResult{
		Fills:     make([]models.Fill, 0),
		Remaining: req.Amount,
		Status:    models.OrderStatusPending,
	}

	e.mu.RLock()
	ob := e.orderBooks[req.PairID]
	e.mu.RUnlock()

	if ob == nil {
		result.Error = fmt.Errorf("order book not found for pair: %s", req.PairID)
		req.ResultChan <- result
		return
	}

	var matchingLevels []PriceLevel
	if req.Side == models.OrderSideBuy {
		ob.mu.RLock()
		matchingLevels = ob.Asks
		ob.mu.RUnlock()
	} else {
		ob.mu.RLock()
		matchingLevels = ob.Bids
		ob.mu.RUnlock()
	}

	// Match against opposite side
	remaining := req.Amount
	for _, level := range matchingLevels {
		if remaining.IsZero() {
			break
		}

		// Check price compatibility
		priceMatch := false
		if req.Side == models.OrderSideBuy {
			if req.Type == models.OrderTypeMarket || req.Price.GreaterThanOrEqual(level.Price) {
				priceMatch = true
			}
		} else {
			if req.Type == models.OrderTypeMarket || req.Price.LessThanOrEqual(level.Price) {
				priceMatch = true
			}
		}

		fmt.Printf("[Matching] Level price=%s, takerPrice=%s, type=%s, priceMatch=%v\n",
			level.Price.String(), req.Price.String(), req.Type, priceMatch)

		if !priceMatch {
			continue
		}

		// Match against orders at this level
		fmt.Printf("[Matching] Matching against %d orders at this level\n", len(level.Orders))
		for _, order := range level.Orders {
			fmt.Printf("[Matching] Order: id=%d, amount=%s, filled=%s, available=%s\n",
				order.ID, order.Amount.String(), order.FilledAmount.String(),
				order.Amount.Sub(order.FilledAmount).String())
			if remaining.IsZero() {
				break
			}

			available := order.Amount.Sub(order.FilledAmount)
			if available.IsZero() {
				continue
			}

			fillAmount := available
			if remaining.LessThan(available) {
				fillAmount = remaining
			}

			// Calculate fill
			// fill.Amount = base token amount being swapped
			// fill.AmountIn = quote token amount for the trade
			fillAmountBase := fillAmount
			baseDecimals := req.AmountInDecimals
			quoteDecimals := req.AmountOutDecimals
			if req.Side == models.OrderSideBuy {
				baseDecimals = req.AmountOutDecimals
				quoteDecimals = req.AmountInDecimals
			}
			fillAmountQuote := computeQuoteAmount(fillAmountBase, level.Price, baseDecimals, quoteDecimals)

			fill := Fill{
				MakerOrderID: order.ID,
				TakerOrderID: req.OrderID,
				Maker:        order.Maker,
				Price:        level.Price,
				Amount:       fillAmountBase,  // Base token amount
				AmountIn:     fillAmountQuote, // Quote token amount
				AmountOut:    fillAmountBase,  // Base token amount
				Side:         req.Side,
			}

			result.Fills = append(result.Fills, models.Fill{
				MakerOrderID: fill.MakerOrderID,
				TakerOrderID: fill.TakerOrderID,
				Maker:        fill.Maker,
				Price:        fill.Price,
				Amount:       fill.Amount,
				AmountIn:     fill.AmountIn,
				AmountOut:    fill.AmountOut,
				Side:         fill.Side,
			})

			fmt.Printf("[Matching] Fill created: makerOrderID=%d, takerOrderID=%d, price=%s, amount=%s\n",
				fill.MakerOrderID, fill.TakerOrderID, fill.Price.String(), fill.Amount.String())

			remaining = remaining.Sub(fillAmount)
		}
	}

	fmt.Printf("[Matching] Result: fills=%d, remaining=%s, status=%s\n",
		len(result.Fills), remaining.String(), result.Status)
	if remaining.IsZero() {
		result.Status = models.OrderStatusFilled
	} else if remaining.LessThan(req.Amount) {
		result.Status = models.OrderStatusPartial
	}

	req.ResultChan <- result
}

// MatchOrder matches an incoming order against the order book
func (e *MatchingEngine) MatchOrder(ctx context.Context, order *models.Order) (*MatchResult, error) {
	resultChan := make(chan *MatchResult)

	req := &MatchRequest{
		OrderID:           order.ID,
		PairID:            order.PairID,
		Side:              order.Side,
		Amount:            order.Amount,
		Price:             order.Price,
		Type:              order.OrderType,
		AmountInDecimals:  order.AmountInDecimals,
		AmountOutDecimals: order.AmountOutDecimals,
		ResultChan:        resultChan,
	}

	select {
	case e.matchChan <- req:
		result := <-resultChan
		return result, result.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetOrderBook returns the order book for a pair
func (e *MatchingEngine) GetOrderBook(pairID string) *OrderBook {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.orderBooks[pairID]
}

// UpdateOrderBook updates the order book for a pair
func (e *MatchingEngine) UpdateOrderBook(ctx context.Context, pairID string) error {
	pair, err := e.pairRepo.GetByID(ctx, pairID)
	if err != nil {
		return err
	}

	// Load actual orders, not just aggregated levels
	asks, err := e.orderRepo.GetActiveByPair(ctx, pairID, pair.Network, models.OrderSideSell)
	if err != nil {
		return err
	}
	bids, err := e.orderRepo.GetActiveByPair(ctx, pairID, pair.Network, models.OrderSideBuy)
	if err != nil {
		return err
	}

	fmt.Printf("[Matching] UpdateOrderBook: pair=%s, asks=%d, bids=%d\n", pairID, len(asks), len(bids))

	e.mu.Lock()
	if e.orderBooks[pairID] == nil {
		e.orderBooks[pairID] = &OrderBook{PairID: pairID}
	}
	ob := e.orderBooks[pairID]
	ob.mu.Lock()
	ob.Asks = groupOrdersByPrice(asks, models.OrderSideSell)
	ob.Bids = groupOrdersByPrice(bids, models.OrderSideBuy)
	ob.Sequence = time.Now().Unix()
	ob.mu.Unlock()
	e.mu.Unlock()

	return nil
}

// runOrderBookUpdater periodically updates order books
func (e *MatchingEngine) runOrderBookUpdater(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.quitChan:
			return
		case <-ticker.C:
			e.mu.RLock()
			pairIDs := make([]string, 0, len(e.orderBooks))
			for pairID := range e.orderBooks {
				pairIDs = append(pairIDs, pairID)
			}
			e.mu.RUnlock()

			for _, pairID := range pairIDs {
				if err := e.UpdateOrderBook(ctx, pairID); err != nil {
					fmt.Printf("Failed to update order book for %s: %v\n", pairID, err)
				}
			}
		}
	}
}

// runExpiredOrderProcessor marks expired orders
func (e *MatchingEngine) runExpiredOrderProcessor(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// Run immediately on startup so any orders already past expiry are caught fast
	e.processExpiredOrders(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.quitChan:
			return
		case <-ticker.C:
			e.processExpiredOrders(ctx)
		}
	}
}

func (e *MatchingEngine) processExpiredOrders(ctx context.Context) {
	orders, err := e.orderRepo.GetExpiredOrders(ctx)
	if err != nil {
		fmt.Printf("[Expiry] Failed to get expired orders: %v\n", err)
		return
	}

	if len(orders) == 0 {
		return
	}

	fmt.Printf("[Expiry] Processing %d expired order(s)\n", len(orders))

	for _, o := range orders {
		// Attempt to release locked funds — log the error but ALWAYS mark the order expired.
		// For Solana (custody model) and any order without a balance row, this may return
		// "record not found" or "insufficient locked balance", which is expected and safe to ignore.
		if o.AmountIn.GreaterThan(decimal.Zero) {
			if releaseErr := e.balanceRepo.ReleaseLockedFunds(ctx, o.UserID, o.Network, o.TokenIn, o.AmountIn); releaseErr != nil {
				// Non-fatal: Solana orders use custody model so there may be no balance row.
				// Log and continue — expiration must still proceed.
				fmt.Printf("[Expiry] Warning: could not release locked funds for order %d (network=%s): %v — proceeding with expiration anyway\n",
					o.ID, o.Network, releaseErr)
			}
		}

		// Always mark as expired regardless of fund-release outcome
		o.Status = models.OrderStatusExpired
		if err := e.orderRepo.Update(ctx, &o); err != nil {
			fmt.Printf("[Expiry] Failed to update expired order %d: %v\n", o.ID, err)
			continue
		}

		fmt.Printf("[Expiry] Order %d marked as expired (network=%s, pair=%s)\n", o.ID, o.Network, o.PairID)

		// Create refund request for expired Solana orders
		if e.refundService != nil {
			if err := e.refundService.CreateRefundForOrder(ctx, &o); err != nil {
				fmt.Printf("[Expiry] Failed to create refund for expired order %d: %v\n", o.ID, err)
				// Don't fail the expiration if refund creation fails
			}
		}
	}

	fmt.Printf("[Expiry] Done: marked %d order(s) as expired\n", len(orders))
}

// runPriceMonitor monitors prices and triggers stop-loss/take-profit orders
func (e *MatchingEngine) runPriceMonitor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.quitChan:
			return
		case <-ticker.C:
			e.mu.RLock()
			pairIDs := make([]string, 0, len(e.orderBooks))
			for pairID := range e.orderBooks {
				pairIDs = append(pairIDs, pairID)
			}
			e.mu.RUnlock()

			for _, pairID := range pairIDs {
				ob := e.GetOrderBook(pairID)
				if ob == nil || len(ob.Bids) == 0 {
					continue
				}

				currentPrice := ob.Bids[0].Price

				if err := e.CheckAndTriggerStopLoss(ctx, pairID, currentPrice); err != nil {
					fmt.Printf("Error triggering stop-loss for %s: %v\n", pairID, err)
				}

				if err := e.CheckAndTriggerTakeProfit(ctx, pairID, currentPrice); err != nil {
					fmt.Printf("Error triggering take-profit for %s: %v\n", pairID, err)
				}
			}
		}
	}
}

// Helper functions
func convertToPriceLevels(levels []models.OrderLevel) []PriceLevel {
	result := make([]PriceLevel, len(levels))
	for i, l := range levels {
		result[i] = PriceLevel{
			Price:  l.Price,
			Amount: l.Amount,
		}
	}
	return result
}

// groupOrdersByPrice groups orders by price level for the matching engine
func computeQuoteAmount(amountBase decimal.Decimal, price decimal.Decimal, baseDecimals, quoteDecimals int) decimal.Decimal {
	if amountBase.IsZero() || price.IsZero() {
		return decimal.Zero
	}

	if baseDecimals == 0 {
		baseDecimals = 18
	}
	if quoteDecimals == 0 {
		quoteDecimals = 18
	}

	scale := int32(baseDecimals - quoteDecimals)
	return amountBase.Mul(price).Shift(-scale)
}

func groupOrdersByPrice(orders []models.Order, side models.OrderSide) []PriceLevel {
	if len(orders) == 0 {
		return nil
	}

	priceMap := make(map[string]*PriceLevel)
	var result []PriceLevel

	for _, order := range orders {
		priceKey := order.Price.String()
		if level, exists := priceMap[priceKey]; exists {
			level.Orders = append(level.Orders, order)
			level.Amount = level.Amount.Add(order.Amount.Sub(order.FilledAmount))
		} else {
			level := &PriceLevel{
				Price:  order.Price,
				Amount: order.Amount.Sub(order.FilledAmount),
				Orders: []models.Order{order},
			}
			priceMap[priceKey] = level
		}
	}

	// Rebuild result from priceMap to include all aggregated updates
	result = make([]PriceLevel, 0, len(priceMap))
	for _, level := range priceMap {
		if level.Amount.GreaterThan(decimal.Zero) {
			result = append(result, *level)
		}
	}

	if side == models.OrderSideBuy {
		sort.Slice(result, func(i, j int) bool {
			return result[i].Price.GreaterThan(result[j].Price)
		})
	} else {
		sort.Slice(result, func(i, j int) bool {
			return result[i].Price.LessThan(result[j].Price)
		})
	}

	return result
}

// CheckAndTriggerStopLoss checks if stop-loss orders should be triggered
func (e *MatchingEngine) CheckAndTriggerStopLoss(ctx context.Context, pairID string, currentPrice decimal.Decimal) error {
	orders, err := e.orderRepo.GetStopLossOrders(ctx, pairID, currentPrice)
	if err != nil {
		return err
	}

	for _, order := range orders {
		shouldTrigger := false

		if order.Side == models.OrderSideSell && order.TriggerPrice.GreaterThanOrEqual(currentPrice) {
			shouldTrigger = true
		} else if order.Side == models.OrderSideBuy && order.TriggerPrice.LessThanOrEqual(currentPrice) {
			shouldTrigger = true
		}

		if shouldTrigger {
			if err := e.orderRepo.TriggerOrder(ctx, order.ID); err != nil {
				fmt.Printf("Failed to trigger stop-loss order %d: %v\n", order.ID, err)
				continue
			}

			resultChan := make(chan *MatchResult)
			matchReq := &MatchRequest{
				OrderID:           order.ID,
				PairID:            pairID,
				Side:              order.Side,
				Amount:            order.Amount.Sub(order.FilledAmount),
				Price:             order.Price,
				Type:              models.OrderTypeLimit,
				AmountInDecimals:  order.AmountInDecimals,
				AmountOutDecimals: order.AmountOutDecimals,
				ResultChan:        resultChan,
			}
			e.matchChan <- matchReq
			result := <-resultChan

			if result.Error != nil {
				fmt.Printf("Failed to match triggered order %d: %v\n", order.ID, result.Error)
			}
		}
	}

	return nil
}

// CheckAndTriggerTakeProfit checks if take-profit orders should be triggered
func (e *MatchingEngine) CheckAndTriggerTakeProfit(ctx context.Context, pairID string, currentPrice decimal.Decimal) error {
	orders, err := e.orderRepo.GetTakeProfitOrders(ctx, pairID, currentPrice)
	if err != nil {
		return err
	}

	for _, order := range orders {
		shouldTrigger := false

		if order.Side == models.OrderSideSell && order.TriggerPrice.LessThanOrEqual(currentPrice) {
			shouldTrigger = true
		} else if order.Side == models.OrderSideBuy && order.TriggerPrice.GreaterThanOrEqual(currentPrice) {
			shouldTrigger = true
		}

		if shouldTrigger {
			if err := e.orderRepo.TriggerOrder(ctx, order.ID); err != nil {
				fmt.Printf("Failed to trigger take-profit order %d: %v\n", order.ID, err)
				continue
			}

			resultChan := make(chan *MatchResult)
			matchReq := &MatchRequest{
				OrderID:           order.ID,
				PairID:            pairID,
				Side:              order.Side,
				Amount:            order.Amount.Sub(order.FilledAmount),
				Price:             order.Price,
				Type:              models.OrderTypeLimit,
				AmountInDecimals:  order.AmountInDecimals,
				AmountOutDecimals: order.AmountOutDecimals,
				ResultChan:        resultChan,
			}
			e.matchChan <- matchReq
			result := <-resultChan

			if result.Error != nil {
				fmt.Printf("Failed to match triggered order %d: %v\n", order.ID, result.Error)
			}
		}
	}

	return nil
}

// ValidatePostOnlyOrder checks if a post-only order can be placed
// Returns true if the order should be rejected (would fill immediately)
func (e *MatchingEngine) ValidatePostOnlyOrder(order *models.Order) bool {
	if !order.IsPostOnly {
		return false
	}

	ob := e.GetOrderBook(order.PairID)
	if ob == nil {
		return false
	}

	if order.Side == models.OrderSideBuy {
		// For buy orders, check if there's any ask at or below our price
		for _, level := range ob.Asks {
			if order.Price.GreaterThanOrEqual(level.Price) {
				return true // Would fill immediately - reject
			}
		}
	} else {
		// For sell orders, check if there's any bid at or above our price
		for _, level := range ob.Bids {
			if order.Price.LessThanOrEqual(level.Price) {
				return true // Would fill immediately - reject
			}
		}
	}

	return false
}

// ValidateTimeInForce validates time-in-force settings
func ValidateTimeInForce(timeInForce string, orderType models.OrderType) error {
	validTIF := map[string]bool{
		"GTC": true, // Good Till Cancel
		"IOC": true, // Immediate or Cancel
		"FOK": true, // Fill or Kill
		"GTD": true, // Good Till Date
	}

	if timeInForce == "" {
		return nil // Default to GTC
	}

	if !validTIF[timeInForce] {
		return fmt.Errorf("invalid time_in_force: %s", timeInForce)
	}

	// IOC and FOK only valid for limit/market orders
	if (timeInForce == "IOC" || timeInForce == "FOK") &&
		(orderType == models.OrderTypeStopLoss || orderType == models.OrderTypeTakeProfit) {
		return fmt.Errorf("time_in_force %s not valid for %s orders", timeInForce, orderType)
	}

	return nil
}

// ProcessTimeInForce applies time-in-force logic
func (e *MatchingEngine) ProcessTimeInForce(order *models.Order, result *MatchResult) {
	switch order.TimeInForce {
	case "IOC":
		// Immediate or Cancel - any unfilled amount is cancelled
		if result.Remaining.GreaterThan(decimal.Zero) {
			e.orderRepo.Cancel(context.Background(), order.ID)
		}
	case "FOK":
		// Fill or Kill - order must fill completely or not at all
		if result.Remaining.GreaterThan(decimal.Zero) {
			result.Fills = nil
			result.Status = models.OrderStatusCancelled
			e.orderRepo.Cancel(context.Background(), order.ID)
		}
	case "GTC", "GTD", "":
		// Good Till Cancel/Date - default behavior, leave as is
		break
	}
}

// Calculate fees based on configuration
func (e *MatchingEngine) CalculateFee(amount decimal.Decimal) decimal.Decimal {
	feeBps := decimal.NewFromFloat(0.3) // 0.3% fee
	return amount.Mul(feeBps).Div(decimal.NewFromInt(10000))
}

// RoundAmount rounds amount to appropriate precision
func RoundAmount(amount decimal.Decimal, decimals int) decimal.Decimal {
	pow := decimal.NewFromInt(int64(math.Pow10(decimals)))
	return amount.Mul(pow).Floor().Div(pow)
}
