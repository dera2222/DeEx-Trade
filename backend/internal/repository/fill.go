package repository

import (
	"context"
	"log"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
)

type FillRepository struct {
	db    *db.DB
	redis *db.RedisClient
}

func NewFillRepository(db *db.DB, redis *db.RedisClient) *FillRepository {
	return &FillRepository{db: db, redis: redis}
}

func (r *FillRepository) Create(ctx context.Context, fill *models.Fill) error {
	return r.db.WithContext(ctx).Create(fill).Error
}

func (r *FillRepository) GetByID(ctx context.Context, id uint) (*models.Fill, error) {
	var fill models.Fill
	if err := r.db.WithContext(ctx).First(&fill, id).Error; err != nil {
		return nil, err
	}
	return &fill, nil
}

func (r *FillRepository) GetByUserID(ctx context.Context, userID uint, limit, offset int) ([]models.Fill, error) {
	var fills []models.Fill
	err := r.db.WithContext(ctx).
		Where("(maker = (SELECT address FROM users WHERE id = ?) OR taker = (SELECT address FROM users WHERE id = ?)) AND (status = ? OR status = ?)", userID, userID, "settled", "").
		Order("created_at desc").
		Limit(limit).
		Offset(offset).
		Find(&fills).Error
	return fills, err
}

func (r *FillRepository) GetByPair(ctx context.Context, pairID string, limit int) ([]models.Fill, error) {
	var fills []models.Fill
	err := r.db.WithContext(ctx).
		Where("pair_id = ?", pairID).
		Order("created_at desc").
		Limit(limit).
		Find(&fills).Error
	return fills, err
}

func (r *FillRepository) GetByOrderID(ctx context.Context, orderID uint) ([]models.Fill, error) {
	var fills []models.Fill
	err := r.db.WithContext(ctx).
		Where("maker_order_id = ? OR taker_order_id = ?", orderID, orderID).
		Order("created_at desc").
		Find(&fills).Error
	return fills, err
}

func (r *FillRepository) GetByTxHash(ctx context.Context, txHash string) (*models.Fill, error) {
	var fill models.Fill
	err := r.db.WithContext(ctx).
		Where("tx_hash = ? OR tx_hash_buy = ? OR tx_hash_sell = ?", txHash, txHash, txHash).
		First(&fill).Error
	return &fill, err
}

func (r *FillRepository) GetPendingSettlements(ctx context.Context, limit int) ([]models.Fill, error) {
	var fills []models.Fill
	err := r.db.WithContext(ctx).
		Where("status = ?", "pending").
		Order("created_at asc").
		Limit(limit).
		Find(&fills).Error
	return fills, err
}

func (r *FillRepository) Update(ctx context.Context, fill *models.Fill) error {
	return r.db.WithContext(ctx).Save(fill).Error
}

func (r *FillRepository) GetByMakerAddress(ctx context.Context, maker string, limit int) ([]models.Fill, error) {
	var fills []models.Fill
	err := r.db.WithContext(ctx).
		Where("(maker = ? OR taker = ?) AND (status = ? OR status = ?)", maker, maker, "settled", "").
		Order("created_at desc").
		Limit(limit).
		Find(&fills).Error
	return fills, err
}

func (r *FillRepository) MarkAsSettled(ctx context.Context, fillID uint, txHash, txHashBuy, txHashSell string, blockNumber uint64, gasUsed uint64) error {
	updates := map[string]interface{}{
		"tx_hash":      txHash,
		"block_number": blockNumber,
		"gas_used":     gasUsed,
		"status":       "settled",
	}
	if txHashBuy != "" {
		updates["tx_hash_buy"] = txHashBuy
	}
	if txHashSell != "" {
		updates["tx_hash_sell"] = txHashSell
	}
	return r.db.WithContext(ctx).
		Model(&models.Fill{}).
		Where("id = ?", fillID).
		Updates(updates).Error
}

func (r *FillRepository) GetCandles(ctx context.Context, pairID string, resolutionSec int, limit int) ([]models.Candle, error) {
	var dbCandles []models.Candle
	// Use LAG() to carry the previous candle's close as the open for the current candle.
	// This ensures consecutive candles connect properly even when there is only one fill
	// in a given interval (which would otherwise produce open=close=high=low doji lines).
	query := `
		WITH raw AS (
			SELECT 
				floor(extract(epoch from f.created_at) / $1) * $1 AS time,
				(array_agg(f.price::numeric ORDER BY f.created_at ASC))[1]  AS first_price,
				MAX(f.price::numeric)                                         AS high,
				MIN(f.price::numeric)                                         AS low,
				(array_agg(f.price::numeric ORDER BY f.created_at DESC))[1] AS close,
				SUM(CASE WHEN f.side = 'buy' THEN f.amount_in::numeric ELSE f.amount_out::numeric END)
					/ POWER(10, COALESCE(p.quote_token_decimals, 0))          AS volume
			FROM fills f
			LEFT JOIN pairs p ON f.pair_id = p.id
			WHERE f.pair_id = $2
			  AND (f.status = 'settled' OR f.status = '' OR f.status IS NULL)
			GROUP BY 1, p.quote_token_decimals
		)
		SELECT
			time,
			-- Open = previous candle's close (LAG), falling back to this candle's first fill price
			COALESCE(LAG(close) OVER (ORDER BY time), first_price) AS open,
			high,
			low,
			close,
			volume
		FROM raw
		ORDER BY time DESC
		LIMIT $3
	`
	err := r.db.WithContext(ctx).Raw(query, resolutionSec, pairID, limit).Scan(&dbCandles).Error
	if err != nil {
		return nil, err
	}

	// Reverse to ascending order for the chart
	for i, j := 0, len(dbCandles)-1; i < j; i, j = i+1, j-1 {
		dbCandles[i], dbCandles[j] = dbCandles[j], dbCandles[i]
	}

	log.Printf("[GetCandles] Found %d candles in DB for pair %s", len(dbCandles), pairID)

	if len(dbCandles) > limit {
		dbCandles = dbCandles[len(dbCandles)-limit:]
	}

	return dbCandles, nil
}

type CandleDebugInfo struct {
	Candles     []models.Candle `json:"candles"`
	DBCount     int             `json:"db_count"`
	FilledCount int             `json:"filled_count"`
	GapsFilled  int             `json:"gaps_filled"`
	StartTime   int64           `json:"start_time"`
	EndTime     int64           `json:"end_time"`
	Resolution  int             `json:"resolution"`
}

func (r *FillRepository) GetCandlesDebug(ctx context.Context, pairID string, resolutionSec int, limit int) (*CandleDebugInfo, error) {
	var dbCandles []models.Candle
	query := `
		WITH raw AS (
			SELECT 
				floor(extract(epoch from f.created_at) / $1) * $1 AS time,
				(array_agg(f.price::numeric ORDER BY f.created_at ASC))[1]  AS first_price,
				MAX(f.price::numeric)                                         AS high,
				MIN(f.price::numeric)                                         AS low,
				(array_agg(f.price::numeric ORDER BY f.created_at DESC))[1] AS close,
				SUM(CASE WHEN f.side = 'buy' THEN f.amount_in::numeric ELSE f.amount_out::numeric END)
					/ POWER(10, COALESCE(p.quote_token_decimals, 0))          AS volume
			FROM fills f
			LEFT JOIN pairs p ON f.pair_id = p.id
			WHERE f.pair_id = $2
			  AND (f.status = 'settled' OR f.status = '' OR f.status IS NULL)
			GROUP BY 1, p.quote_token_decimals
		)
		SELECT
			time,
			COALESCE(LAG(close) OVER (ORDER BY time), first_price) AS open,
			high,
			low,
			close,
			volume
		FROM raw
		ORDER BY time DESC
		LIMIT $3
	`
	err := r.db.WithContext(ctx).Raw(query, resolutionSec, pairID, limit).Scan(&dbCandles).Error
	if err != nil {
		return nil, err
	}

	if len(dbCandles) == 0 {
		return &CandleDebugInfo{Candles: []models.Candle{}, DBCount: 0}, nil
	}

	// Reverse to ascending order
	for i, j := 0, len(dbCandles)-1; i < j; i, j = i+1, j-1 {
		dbCandles[i], dbCandles[j] = dbCandles[j], dbCandles[i]
	}

	// Apply limit and return only real candles from database
	if len(dbCandles) > limit {
		dbCandles = dbCandles[len(dbCandles)-limit:]
	}

	nowSec := time.Now().Unix()
	lastTime := (nowSec / int64(resolutionSec)) * int64(resolutionSec)

	return &CandleDebugInfo{
		Candles:     dbCandles,
		DBCount:     len(dbCandles),
		FilledCount: len(dbCandles),
		GapsFilled:  0,
		StartTime:   lastTime,
		EndTime:     lastTime,
		Resolution:  resolutionSec,
	}, nil
}
