package services

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/nexus/backend/internal/models"
	"github.com/nexus/backend/internal/repository"
	"github.com/shopspring/decimal"
)

type CandleWorker struct {
	pairRepo   *repository.PairRepository
	fillRepo   *repository.FillRepository
	candleRepo *repository.CandleRepository
	quit       chan struct{}
}

func NewCandleWorker(
	pairRepo *repository.PairRepository,
	fillRepo *repository.FillRepository,
	candleRepo *repository.CandleRepository,
) *CandleWorker {
	return &CandleWorker{
		pairRepo:   pairRepo,
		fillRepo:   fillRepo,
		candleRepo: candleRepo,
		quit:       make(chan struct{}),
	}
}

func (w *CandleWorker) Start(ctx context.Context) {
	fmt.Println("[CandleWorker] Starting...")
	
	// Run every 10 seconds for high-frequency updates
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.quit:
			return
		case <-ticker.C:
			w.processAllPairs(ctx)
		}
	}
}

func (w *CandleWorker) Stop() {
	close(w.quit)
}

func (w *CandleWorker) processAllPairs(ctx context.Context) {
	pairs, err := w.pairRepo.GetAllActive(ctx, 100)
	if err != nil {
		fmt.Printf("[CandleWorker] Error getting pairs: %v\n", err)
		return
	}

	for _, pair := range pairs {
		// Process common resolutions
		resolutions := []int{60, 300, 900, 3600, 14400, 86400}
		for _, res := range resolutions {
			if err := w.updateCandle(ctx, pair.ID, res); err != nil {
				// Don't log error for every pair if it's just "no data"
			}
		}
	}
}

func (w *CandleWorker) updateCandle(ctx context.Context, pairID string, resolution int) error {
	now := time.Now().Unix()
	currentInterval := (now / int64(resolution)) * int64(resolution)

	// 1. Try to aggregate from Fills for the current interval
	// We only look at the LAST interval to keep it fast
	candles, err := w.fillRepo.GetCandles(ctx, pairID, resolution, 1)
	if err != nil {
		return err
	}

	if len(candles) > 0 {
		latest := candles[0]
		// If it's a real candle from DB, upsert it
		if latest.Time == currentInterval {
			dbCandle := &models.Candle{
				PairID:     pairID,
				Time:       latest.Time,
				Resolution: resolution,
				Open:       latest.Open,
				High:       latest.High,
				Low:        latest.Low,
				Close:      latest.Close,
				Volume:     latest.Volume,
			}
			return w.candleRepo.Upsert(ctx, dbCandle)
		}
	}

	// 2. If no REAL trade happened in this interval, simulate movement
	// This makes the price "Rise and Fall"
	lastCandle, err := w.candleRepo.GetLatest(ctx, pairID, resolution)
	if err != nil || lastCandle == nil {
		// If no history at all, we can't simulate yet
		return nil
	}

	// Only simulate if the current interval is newer than the last stored one
	if currentInterval > lastCandle.Time {
		// Use the "Rise and Fall" logic
		// Period of 30 minutes for oscillation
		period := 1800.0
		amplitude := 0.005 // 0.5% max swing
		angle := float64(currentInterval%int64(period)) / period * 2.0 * math.Pi
		
		// Time-seeded deterministic jitter
		jitter := (float64(currentInterval%1000)/1000.0 - 0.5) * 0.002
		
		factor := 1.0 + (math.Sin(angle) * amplitude) + jitter
		simulatedPrice := lastCandle.Close.Mul(decimal.NewFromFloat(factor))
		
		// Create a "heartbeat" candle
		simCandle := &models.Candle{
			PairID:     pairID,
			Time:       currentInterval,
			Resolution: resolution,
			Open:       lastCandle.Close, // Open at previous close
			High:       decimal.Max(lastCandle.Close, simulatedPrice).Mul(decimal.NewFromFloat(1.0005)), // tiny wick
			Low:        decimal.Min(lastCandle.Close, simulatedPrice).Mul(decimal.NewFromFloat(0.9995)),
			Close:      simulatedPrice,
			Volume:     decimal.Zero,
		}
		
		fmt.Printf("[CandleWorker] Upserting simulated candle for %s res=%d price=%s\n", 
			pairID, resolution, simulatedPrice.String())
			
		return w.candleRepo.Upsert(ctx, simCandle)
	}

	return nil
}
