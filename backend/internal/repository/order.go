package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"github.com/shopspring/decimal"
)

type OrderRepository struct {
	db    *db.DB
	redis *db.RedisClient
}

func NewOrderRepository(db *db.DB, redis *db.RedisClient) *OrderRepository {
	return &OrderRepository{db: db, redis: redis}
}

// Create inserts a new order
func (r *OrderRepository) Create(ctx context.Context, order *models.Order) error {
	return r.db.WithContext(ctx).Create(order).Error
}

// CreateBatch inserts multiple orders in a single transaction
func (r *OrderRepository) CreateBatch(ctx context.Context, orders []*models.Order) error {
	if len(orders) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Create(orders).Error
}

// GetByID retrieves an order by ID
func (r *OrderRepository) GetByID(ctx context.Context, id uint) (*models.Order, error) {
	var order models.Order
	if err := r.db.WithContext(ctx).First(&order, id).Error; err != nil {
		return nil, err
	}
	return &order, nil
}

// GetByHash retrieves an order by hash
func (r *OrderRepository) GetByHash(ctx context.Context, hash string) (*models.Order, error) {
	var order models.Order
	if err := r.db.WithContext(ctx).Where("order_hash = ?", hash).First(&order).Error; err != nil {
		return nil, err
	}
	return &order, nil
}

// GetByUserID retrieves orders for a user with pagination
func (r *OrderRepository) GetByUserID(ctx context.Context, userID uint, limit, offset int) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("created_at desc").
		Limit(limit).
		Offset(offset).
		Find(&orders).Error
	return orders, err
}

// GetActiveByPair retrieves all active, non-expired orders for a trading pair
func (r *OrderRepository) GetActiveByPair(ctx context.Context, pairID string, network models.Network, side models.OrderSide) ([]models.Order, error) {
	var orders []models.Order
	query := r.db.WithContext(ctx).
		Where(
			"pair_id = ? AND network = ? AND status IN ? AND (expiration IS NULL OR expiration = '0001-01-01 00:00:00' OR expiration > ?)",
			pairID, network, []models.OrderStatus{models.OrderStatusPending, models.OrderStatusPartial}, time.Now(),
		).
		Limit(1000)

	if side != "" {
		query = query.Where("side = ?", side)
	}

	err := query.Order("price desc").Find(&orders).Error
	return orders, err
}

// GetActiveByUser retrieves all active, non-expired orders for a user
func (r *OrderRepository) GetActiveByUser(ctx context.Context, userID uint) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where(
			"user_id = ? AND status IN ? AND (expiration IS NULL OR expiration = '0001-01-01 00:00:00' OR expiration > ?)",
			userID, []models.OrderStatus{models.OrderStatusPending, models.OrderStatusPartial}, time.Now(),
		).
		Order("created_at desc").
		Find(&orders).Error
	return orders, err
}

// Update updates an order
func (r *OrderRepository) Update(ctx context.Context, order *models.Order) error {
	order.UpdatedAt = time.Now()
	return r.db.WithContext(ctx).Save(order).Error
}

// Cancel cancels an order
func (r *OrderRepository) Cancel(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&models.Order{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":     models.OrderStatusCancelled,
			"updated_at": time.Now(),
		}).Error
}

// BatchCancel cancels multiple orders
func (r *OrderRepository) BatchCancel(ctx context.Context, ids []uint) error {
	return r.db.WithContext(ctx).
		Model(&models.Order{}).
		Where("id IN ?", ids).
		Updates(map[string]interface{}{
			"status":     models.OrderStatusCancelled,
			"updated_at": time.Now(),
		}).Error
}

// FillAmount updates the filled amount for an order
func (r *OrderRepository) FillAmount(ctx context.Context, id uint, filledAmount decimal.Decimal) error {
	var order models.Order
	if err := r.db.WithContext(ctx).First(&order, id).Error; err != nil {
		return err
	}

	newFilled := order.FilledAmount.Add(filledAmount)
	updatedAmount := order.Amount

	status := order.Status
	if newFilled.GreaterThanOrEqual(updatedAmount) {
		status = models.OrderStatusFilled
	} else {
		status = models.OrderStatusPartial
	}

	return r.db.WithContext(ctx).
		Model(&order).
		Updates(map[string]interface{}{
			"filled_amount": newFilled,
			"status":        status,
			"updated_at":    time.Now(),
		}).Error
}

// GetOrderBook generates order book for a pair
func (r *OrderRepository) GetOrderBook(ctx context.Context, pairID string, network models.Network, limit int) (*models.OrderBookResponse, error) {
	var asks, bids []models.Order

	// Get sell orders (asks) - sorted by price ascending
	r.db.WithContext(ctx).
		Where("pair_id = ? AND network = ? AND side = ? AND status IN ?",
			pairID, network, models.OrderSideSell, []models.OrderStatus{models.OrderStatusPending, models.OrderStatusPartial}).
		Order("price asc").
		Limit(limit).
		Find(&asks)

	// Get buy orders (bids) - sorted by price descending
	r.db.WithContext(ctx).
		Where("pair_id = ? AND network = ? AND side = ? AND status IN ?",
			pairID, network, models.OrderSideBuy, []models.OrderStatus{models.OrderStatusPending, models.OrderStatusPartial}).
		Order("price desc").
		Limit(limit).
		Find(&bids)

	// Convert to response format with proper decimal conversion
	askLevels := groupOrdersWithDecimals(asks)
	bidLevels := groupOrdersWithDecimals(bids)

	// Calculate mid price, spread, and spread percent
	midPrice, spread, spreadPercent := calculateOrderbookMetrics(askLevels, bidLevels)

	return &models.OrderBookResponse{
		PairID:        pairID,
		Asks:          askLevels,
		Bids:          bidLevels,
		Sequence:      time.Now().Unix(),
		MidPrice:      midPrice,
		Spread:        spread,
		SpreadPercent: spreadPercent,
	}, nil
}

// Calculate mid price, spread, and spread percentage from orderbook levels
func calculateOrderbookMetrics(asks, bids []models.OrderLevel) (midPrice, spread, spreadPercent float64) {
	if len(asks) == 0 || len(bids) == 0 {
		return 0, 0, 0
	}

	bestAsk := asks[0].Price.InexactFloat64()
	bestBid := bids[0].Price.InexactFloat64()

	if bestBid <= 0 || bestAsk <= 0 || bestBid > bestAsk {
		return 0, 0, 0
	}

	// Mid price: arithmetic mean of best bid and best ask
	midPrice = (bestBid + bestAsk) / 2

	// Spread: absolute difference between best ask and best bid
	spread = bestAsk - bestBid

	// Spread percent: spread relative to mid price (industry standard)
	if midPrice > 0 {
		spreadPercent = (spread / midPrice) * 100
	}

	return midPrice, spread, spreadPercent
}

// CalculateVolumeWeightedMidPrice calculates VWAP-style mid price using order book depth
// This gives more weight to orders closer to the spread
func CalculateVolumeWeightedMidPrice(asks, bids []models.OrderLevel, depth int) float64 {
	if len(asks) == 0 || len(bids) == 0 {
		return 0
	}

	// Use available depth, not exceeding what we have
	askDepth := len(asks)
	if askDepth > depth {
		askDepth = depth
	}
	bidDepth := len(bids)
	if bidDepth > depth {
		bidDepth = depth
	}

	var askTotalValue, askTotalAmount, bidTotalValue, bidTotalAmount float64

	// Calculate weighted averages for asks (from best ask going up)
	for i := 0; i < askDepth; i++ {
		price := asks[i].Price.InexactFloat64()
		amount := asks[i].Amount.InexactFloat64()
		total := asks[i].Total.InexactFloat64()

		if price > 0 && amount > 0 {
			askTotalValue += total
			askTotalAmount += amount
		}
	}

	// Calculate weighted averages for bids (from best bid going down)
	for i := 0; i < bidDepth; i++ {
		price := bids[i].Price.InexactFloat64()
		amount := bids[i].Amount.InexactFloat64()
		total := bids[i].Total.InexactFloat64()

		if price > 0 && amount > 0 {
			bidTotalValue += total
			bidTotalAmount += amount
		}
	}

	// Calculate VWAP for each side
	var askVWAP, bidVWAP float64
	if askTotalAmount > 0 {
		askVWAP = askTotalValue / askTotalAmount
	}
	if bidTotalAmount > 0 {
		bidVWAP = bidTotalValue / bidTotalAmount
	}

	// If we have both sides, take average; otherwise use available side
	if askVWAP > 0 && bidVWAP > 0 {
		return (askVWAP + bidVWAP) / 2
	} else if askVWAP > 0 {
		return askVWAP
	} else if bidVWAP > 0 {
		return bidVWAP
	}

	return 0
}

// Helper to group orders by price level with proper decimal conversion
// For the orderbook display:
//   - Amount = base token amount (what you're buying/selling)
//   - Total = quote token amount (what you're paying/receiving)
//
// For BUY orders:
//   - Amount = amount_out_min converted from raw to human-readable (base token decimals)
//   - Total = amount_in converted from raw to human-readable (quote token decimals)
//
// For SELL orders:
//   - Amount = amount converted from raw to human-readable (base token decimals)
//   - Total = amount * price (quote token amount)
func groupOrdersWithDecimals(orders []models.Order) []models.OrderLevel {
	if len(orders) == 0 {
		return nil
	}

	levelMap := make(map[string]models.OrderLevel)

	for _, order := range orders {
		priceStr := order.Price.String()

		// Calculate available amount in human-readable form
		var amountHuman, totalHuman decimal.Decimal

		if order.Side == models.OrderSideBuy {
			// For buy orders:
			// - Amount is what they're buying (amount_out_min in base token decimals)
			// - Total is what they're paying (amount_in in quote token decimals)
			amountRaw := order.AmountOutMin
			totalRaw := order.AmountIn

			amountHuman = convertFromWei(amountRaw, order.AmountOutDecimals)
			totalHuman = convertFromWei(totalRaw, order.AmountInDecimals)
		} else {
			// For sell orders:
			// - Amount is what they're selling (amount_in in base token decimals)
			// - Total is what they're receiving (amount * price in quote token decimals)
			availableRaw := order.Amount.Sub(order.FilledAmount)
			amountHuman = convertFromWei(availableRaw, order.AmountInDecimals)
			totalHuman = amountHuman.Mul(order.Price)
		}

		if level, exists := levelMap[priceStr]; exists {
			newAmount := level.Amount.Add(amountHuman)
			newTotal := level.Total.Add(totalHuman)
			level.Amount = newAmount
			level.Total = newTotal
			level.Orders++
			levelMap[priceStr] = level
		} else {
			levelMap[priceStr] = models.OrderLevel{
				Price:  order.Price,
				Amount: amountHuman,
				Total:  totalHuman,
				Orders: 1,
			}
		}
	}

	// Convert map to slice and sort by price (asks ascending, bids descending)
	levels := make([]models.OrderLevel, 0, len(levelMap))
	for _, level := range levelMap {
		if level.Amount.GreaterThan(decimal.Zero) {
			levels = append(levels, level)
		}
	}

	// Determine side from input orders (first order indicates side)
	side := models.OrderSideSell
	if len(orders) > 0 {
		side = orders[0].Side
	}

	if side == models.OrderSideBuy {
		sort.SliceStable(levels, func(i, j int) bool {
			return levels[i].Price.GreaterThan(levels[j].Price)
		})
	} else {
		sort.SliceStable(levels, func(i, j int) bool {
			return levels[i].Price.LessThan(levels[j].Price)
		})
	}

	return levels
}

// convertFromWei converts a raw token amount to human-readable decimal
// amount: raw amount in token's native decimals (e.g., 130000000000000 for 130000 CREPE with 9 decimals)
// decimals: number of decimals for the token
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

// GetRecentTrades retrieves recent trades for a pair
func (r *OrderRepository) GetRecentTrades(ctx context.Context, pairID string, network models.Network, limit int) ([]models.RecentTrade, error) {
	var fills []models.Fill
	err := r.db.WithContext(ctx).
		Where("pair_id = ? AND network = ? AND status IN ?", pairID, network, []string{"settled", ""}).
		Order("created_at desc").
		Limit(limit).
		Find(&fills).Error

	if err != nil {
		return nil, err
	}

	// Get pair to determine fallback decimals
	var pair models.Pair
	err = r.db.WithContext(ctx).Where("id = ?", pairID).First(&pair).Error
	fallbackDecimals := 18
	if err == nil {
		// Parse base token for decimals
		var baseTokenData map[string]interface{}
		if json.Unmarshal([]byte(pair.BaseToken), &baseTokenData) == nil {
			if symbol, ok := baseTokenData["symbol"].(string); ok && symbol == "CREPE" {
				fallbackDecimals = 9
			}
		}
	}

	trades := make([]models.RecentTrade, len(fills))
	for i, fill := range fills {
		var decimals int
		// Get decimals from the maker order
		var order models.Order
		err := r.db.WithContext(ctx).Where("id = ?", fill.MakerOrderID).First(&order).Error
		if err == nil {
			// For base token amount, get decimals based on side
			if fill.Side == models.OrderSideBuy {
				// Maker is selling base, so amount_out_decimals is base decimals
				decimals = order.AmountOutDecimals
			} else {
				// Maker is buying base, so amount_in_decimals is base decimals
				decimals = order.AmountInDecimals
			}
		} else {
			// Fallback based on pair
			decimals = fallbackDecimals
		}

		trades[i] = models.RecentTrade{
			ID:       fill.ID,
			Price:    fill.Price,
			Amount:   fill.Amount,
			Side:     fill.Side,
			Time:     fill.CreatedAt,
			TxHash:   fill.TxHash,
			Decimals: decimals,
		}
	}

	return trades, nil
}

// GetPendingCommit retrieves pending commit by hash
func (r *OrderRepository) GetPendingCommit(ctx context.Context, commitHash string) (*models.Order, error) {
	var order models.Order
	err := r.db.WithContext(ctx).
		Where("commit_hash = ? AND commit_revealed = ? AND commit_expired = ?",
			commitHash, false, false).
		First(&order).Error
	return &order, err
}

// MarkCommitRevealed marks a commit as revealed
func (r *OrderRepository) MarkCommitRevealed(ctx context.Context, commitHash string) error {
	return r.db.WithContext(ctx).
		Model(&models.Order{}).
		Where("commit_hash = ?", commitHash).
		Updates(map[string]interface{}{
			"commit_revealed": true,
			"updated_at":      time.Now(),
		}).Error
}

// GetExpiredOrders retrieves orders that have expired (increased batch size to handle high volume)
func (r *OrderRepository) GetExpiredOrders(ctx context.Context) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("expiration > '0001-01-01 00:00:00' AND expiration < ? AND status IN ?",
			time.Now(), []models.OrderStatus{models.OrderStatusPending, models.OrderStatusPartial}).
		Limit(500).
		Find(&orders).Error
	return orders, err
}

// BatchUpdateStatus batch updates order statuses
func (r *OrderRepository) BatchUpdateStatus(ctx context.Context, ids []uint, status models.OrderStatus) error {
	return r.db.WithContext(ctx).
		Model(&models.Order{}).
		Where("id IN ?", ids).
		Updates(map[string]interface{}{
			"status":     status,
			"updated_at": time.Now(),
		}).Error
}

// CacheOrder caches an order in Redis
func (r *OrderRepository) CacheOrder(ctx context.Context, order *models.Order) error {
	data, err := json.Marshal(order)
	if err != nil {
		return err
	}
	return r.redis.CacheOrder(ctx, fmt.Sprintf("%d", order.ID), string(data), 5*time.Minute)
}

// GetCachedOrder retrieves a cached order from Redis
func (r *OrderRepository) GetCachedOrder(ctx context.Context, id uint) (*models.Order, error) {
	data, err := r.redis.GetCachedOrder(ctx, fmt.Sprintf("%d", id))
	if err != nil {
		return nil, err
	}

	var order models.Order
	if err := json.Unmarshal([]byte(data), &order); err != nil {
		return nil, err
	}

	return &order, nil
}

// GetStopLossOrders retrieves stop-loss orders that should be triggered
func (r *OrderRepository) GetStopLossOrders(ctx context.Context, pairID string, currentPrice decimal.Decimal) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("pair_id = ? AND order_type = ? AND status = ? AND side = ? AND trigger_price <= ?",
			pairID, models.OrderTypeStopLoss, models.OrderStatusPending, models.OrderSideSell, currentPrice).
		Limit(100).
		Find(&orders).Error
	return orders, err
}

// GetTakeProfitOrders retrieves take-profit orders that should be triggered
func (r *OrderRepository) GetTakeProfitOrders(ctx context.Context, pairID string, currentPrice decimal.Decimal) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("pair_id = ? AND order_type = ? AND status = ? AND side = ? AND trigger_price <= ?",
			pairID, models.OrderTypeTakeProfit, models.OrderStatusPending, models.OrderSideBuy, currentPrice).
		Limit(100).
		Find(&orders).Error
	return orders, err
}

// TriggerOrder marks an order as triggered
func (r *OrderRepository) TriggerOrder(ctx context.Context, id uint) error {
	now := time.Now()
	return r.db.WithContext(ctx).
		Model(&models.Order{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":       models.OrderStatusTriggered,
			"triggered_at": now,
			"updated_at":   now,
		}).Error
}

// GetByUserIDFilter retrieves orders with filters
func (r *OrderRepository) GetByUserIDFilter(ctx context.Context, userID uint, limit, offset int, pairID, status string) ([]models.Order, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	var orders []models.Order
	query := r.db.WithContext(ctx).Where("user_id = ?", userID)

	if pairID != "" {
		query = query.Where("pair_id = ?", pairID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	} else {
		// By default, include all active statuses (excluding triggered since those go to history).
		// Also exclude ladder child orders and orders that have already passed their expiration time.
		query = query.Where(
			"status IN ? AND (ladder_parent_id IS NULL OR ladder_parent_id = 0) AND (expiration IS NULL OR expiration = '0001-01-01 00:00:00' OR expiration > ?)",
			[]models.OrderStatus{models.OrderStatusPending, models.OrderStatusPartial, models.OrderStatusOpen},
			time.Now(),
		)
	}

	err := query.Order("created_at desc").Limit(limit).Offset(offset).Find(&orders).Error
	return orders, err
}

// GetByIDs retrieves multiple orders by IDs
func (r *OrderRepository) GetByIDs(ctx context.Context, ids []uint) ([]models.Order, error) {
	if len(ids) == 0 {
		return []models.Order{}, nil
	}
	if len(ids) > 100 {
		ids = ids[:100]
	}
	var orders []models.Order
	err := r.db.WithContext(ctx).Where("id IN ?", ids).Find(&orders).Error
	return orders, err
}

// GetLadderChildren retrieves child orders of a ladder parent
func (r *OrderRepository) GetLadderChildren(ctx context.Context, parentID uint) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("ladder_parent_id = ? AND status IN ?", parentID, []models.OrderStatus{models.OrderStatusPending, models.OrderStatusPartial}).
		Limit(100).
		Find(&orders).Error
	return orders, err
}

// GetActiveOrdersByUser retrieves all active (pending/partial/open) orders for a user.
// Note: triggered orders go to history, ladder child orders are excluded,
// and orders that have passed their expiration time are excluded even if not yet marked expired.
func (r *OrderRepository) GetActiveOrdersByUser(ctx context.Context, userID uint) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where(
			"user_id = ? AND status IN ? AND (ladder_parent_id IS NULL OR ladder_parent_id = 0) AND (expiration IS NULL OR expiration = '0001-01-01 00:00:00' OR expiration > ?)",
			userID, []models.OrderStatus{models.OrderStatusPending, models.OrderStatusPartial, models.OrderStatusOpen},
			time.Now(),
		).
		Order("created_at desc").
		Limit(100).
		Find(&orders).Error
	return orders, err
}

// GetHistoryOrders retrieves filled, cancelled, expired, or triggered orders for a user
// Note: ladder child orders are excluded
func (r *OrderRepository) GetHistoryOrders(ctx context.Context, userID uint, limit, offset int) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND status IN ? AND (ladder_parent_id IS NULL OR ladder_parent_id = 0)", userID, []models.OrderStatus{models.OrderStatusFilled, models.OrderStatusCancelled, models.OrderStatusExpired, models.OrderStatusTriggered}).
		Order("created_at desc").
		Limit(limit).
		Offset(offset).
		Find(&orders).Error
	return orders, err
}
