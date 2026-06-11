package repository

import (
	"context"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"gorm.io/gorm/clause"
)

type CandleRepository struct {
	db *db.DB
}

func NewCandleRepository(db *db.DB) *CandleRepository {
	return &CandleRepository{db: db}
}

func (r *CandleRepository) Upsert(ctx context.Context, candle *models.Candle) error {
	if candle.Currency == "" {
		candle.Currency = "usd"
	}
	candle.UpdatedAt = time.Now()
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "pair_id"},
			{Name: "time"},
			{Name: "resolution"},
			{Name: "currency"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"open", "high", "low", "close", "volume", "updated_at",
		}),
	}).Create(candle).Error
}

// GetByPair returns candles for a pair+resolution, optionally filtered by currency.
// currency = "" defaults to "usd" for backwards compatibility.
func (r *CandleRepository) GetByPair(ctx context.Context, pairID string, resolution int, limit int) ([]models.Candle, error) {
	return r.GetByPairAndCurrency(ctx, pairID, resolution, "usd", limit)
}

// GetByPairAndCurrency returns candles for a specific currency denomination.
// currency: "usd" | "token"
func (r *CandleRepository) GetByPairAndCurrency(ctx context.Context, pairID string, resolution int, currency string, limit int) ([]models.Candle, error) {
	if currency == "" {
		currency = "usd"
	}
	var candles []models.Candle
	err := r.db.WithContext(ctx).
		Where("pair_id = ? AND resolution = ? AND currency = ?", pairID, resolution, currency).
		Order("time DESC").
		Limit(limit).
		Find(&candles).Error

	// Return in ascending order for the chart
	for i, j := 0, len(candles)-1; i < j; i, j = i+1, j-1 {
		candles[i], candles[j] = candles[j], candles[i]
	}

	return candles, err
}

func (r *CandleRepository) GetLatest(ctx context.Context, pairID string, resolution int) (*models.Candle, error) {
	return r.GetLatestForCurrency(ctx, pairID, resolution, "usd")
}

func (r *CandleRepository) GetLatestForCurrency(ctx context.Context, pairID string, resolution int, currency string) (*models.Candle, error) {
	if currency == "" {
		currency = "usd"
	}
	var candle models.Candle
	err := r.db.WithContext(ctx).
		Where("pair_id = ? AND resolution = ? AND currency = ?", pairID, resolution, currency).
		Order("time DESC").
		First(&candle).Error
	if err != nil {
		return nil, err
	}
	return &candle, nil
}
