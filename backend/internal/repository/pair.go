package repository

import (
	"context"
	"strings"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"github.com/shopspring/decimal"
)

type PairRepository struct {
	db    *db.DB
	redis *db.RedisClient
}

func NewPairRepository(db *db.DB, redis *db.RedisClient) *PairRepository {
	return &PairRepository{db: db, redis: redis}
}

func (r *PairRepository) GetByID(ctx context.Context, id string) (*models.Pair, error) {
	var pair models.Pair
	if err := r.db.WithContext(ctx).First(&pair, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &pair, nil
}

// GetByIDs retrieves multiple pairs by IDs
func (r *PairRepository) GetByIDs(ctx context.Context, ids []string) ([]models.Pair, error) {
	var pairs []models.Pair
	if err := r.db.WithContext(ctx).Where("id IN ?", ids).Find(&pairs).Error; err != nil {
		return nil, err
	}
	return pairs, nil
}

func (r *PairRepository) GetPairs(ctx context.Context, network string, limit int) ([]models.Pair, error) {
	var pairs []models.Pair
	query := r.db.WithContext(ctx).Order("created_at DESC").Limit(limit)

	if network != "" {
		networks := strings.Split(network, ",")
		for i, n := range networks {
			networks[i] = strings.TrimSpace(strings.ToLower(n))
		}
		if len(networks) > 1 {
			query = query.Where("network IN ?", networks)
		} else {
			query = query.Where("network = ?", networks[0])
		}
	}

	err := query.Find(&pairs).Error
	return pairs, err
}

func (r *PairRepository) GetAllActive(ctx context.Context, limit int) ([]models.Pair, error) {
	var pairs []models.Pair
	query := r.db.WithContext(ctx).Order("created_at desc")
	if limit > 0 {
		query = query.Limit(limit)
	}
	err := query.Find(&pairs).Error
	return pairs, err
}

func (r *PairRepository) GetByTokens(ctx context.Context, baseToken, quoteToken string) (*models.Pair, error) {
	var pair models.Pair
	err := r.db.WithContext(ctx).
		Where("(base_token = ? AND quote_token = ?) OR (base_token = ? AND quote_token = ?)",
			baseToken, quoteToken, quoteToken, baseToken).
		First(&pair).Error
	return &pair, err
}

func (r *PairRepository) GetStats(ctx context.Context, pairID string) (*models.TradeStats, error) {
	var stats models.TradeStats
	var fills []models.Fill

	// Get fills from the last 24 hours with settled status only
	twentyFourHoursAgo := time.Now().Add(-24 * time.Hour)
	err := r.db.WithContext(ctx).
		Where("pair_id = ? AND created_at >= ?", pairID, twentyFourHoursAgo).
		Where("status IN ?", []string{"settled", ""}).
		Order("created_at asc").
		Find(&fills).Error

	if err != nil {
		return nil, err
	}

	stats.PairID = pairID
	stats.Price = decimal.Zero
	stats.PriceChange24h = decimal.Zero
	stats.PriceHigh24h = decimal.Zero
	stats.PriceLow24h = decimal.Zero
	stats.Volume24h = decimal.Zero
	stats.Liquidity = decimal.Zero

	// Get the last known price from all time (not just 24 hours)
	var lastFill models.Fill
	err = r.db.WithContext(ctx).
		Where("pair_id = ?", pairID).
		Where("status IN ?", []string{"settled", ""}).
		Order("created_at desc").
		First(&lastFill).Error

	if err == nil {
		// Set the current price to the most recent fill price from any time
		stats.Price = lastFill.Price
		stats.LastTradeAt = &lastFill.CreatedAt
	} else if err.Error() != "record not found" {
		return nil, err
	}

	if len(fills) > 0 {
		// Initialize high and low with first 24h price
		stats.PriceHigh24h = fills[0].Price
		stats.PriceLow24h = fills[0].Price

		// Calculate 24h price change from first to last fill in the period
		if len(fills) > 1 {
			firstPrice := fills[0].Price
			lastPrice := fills[len(fills)-1].Price
			if firstPrice.GreaterThan(decimal.Zero) {
				stats.PriceChange24h = lastPrice.Sub(firstPrice).Div(firstPrice).Mul(decimal.NewFromInt(100))
			}
		}

		// Calculate 24h volume and find high/low prices from all fills in the period
		for _, fill := range fills {
			// Always use AmountIn for volume calculation
			volume := fill.AmountIn
			stats.Volume24h = stats.Volume24h.Add(volume)
			stats.Trades24h++

			// Track 24h high and low prices
			if fill.Price.GreaterThan(stats.PriceHigh24h) {
				stats.PriceHigh24h = fill.Price
			}
			if fill.Price.LessThan(stats.PriceLow24h) {
				stats.PriceLow24h = fill.Price
			}
		}
	}

	return &stats, nil
}

func (r *PairRepository) Search(ctx context.Context, query string) ([]models.Pair, error) {
	var pairs []models.Pair
	q := strings.ToLower(query)

	err := r.db.WithContext(ctx).
		Where(
			"LOWER(base_symbol) LIKE ? OR LOWER(quote_symbol) LIKE ? OR LOWER(id) LIKE ? OR LOWER(pair_address) LIKE ? OR LOWER(base_token) LIKE ? OR LOWER(quote_token) LIKE ?",
			"%"+q+"%", "%"+q+"%", "%"+q+"%", "%"+q+"%", "%"+q+"%", "%"+q+"%",
		).
		Limit(20).
		Find(&pairs).Error

	return pairs, err
}

func (r *PairRepository) GetTokenByAddress(ctx context.Context, address string) (*models.Token, error) {
	var token models.Token
	err := r.db.WithContext(ctx).First(&token, "address = ?", address).Error
	return &token, err
}

// GetTokensByAddresses retrieves multiple tokens by addresses
func (r *PairRepository) GetTokensByAddresses(ctx context.Context, addresses []string) ([]models.Token, error) {
	var tokens []models.Token
	if err := r.db.WithContext(ctx).Where("address IN ?", addresses).Find(&tokens).Error; err != nil {
		return nil, err
	}
	return tokens, nil
}

func (r *PairRepository) Create(ctx context.Context, pair *models.Pair) error {
	return r.db.WithContext(ctx).Create(pair).Error
}

func (r *PairRepository) Update(ctx context.Context, pair *models.Pair) error {
	return r.db.WithContext(ctx).Save(pair).Error
}

func (r *PairRepository) GetByNetwork(ctx context.Context, network string) ([]models.Pair, error) {
	var pairs []models.Pair
	err := r.db.WithContext(ctx).Where("network = ?", network).Find(&pairs).Error
	return pairs, err
}

// UpsertToken creates or updates a token
func (r *PairRepository) UpsertToken(ctx context.Context, token *models.Token) error {
	return r.db.WithContext(ctx).Where("address = ?", token.Address).Assign(*token).FirstOrCreate(token).Error
}

// UpdateLastTradePrice writes the price of the most recent fill to the pairs table.
// This is called synchronously after every fill is saved, so the UI always has a
// fresh "last traded price" to fall back to when the price-worker is stale.
func (r *PairRepository) UpdateLastTradePrice(ctx context.Context, pairID string, price decimal.Decimal) error {
	now := time.Now()
	return r.db.WithContext(ctx).
		Model(&models.Pair{}).
		Where("id = ?", pairID).
		Updates(map[string]interface{}{
			"last_trade_price": price,
			"last_trade_at":    now,
			"updated_at":       now,
		}).Error
}
