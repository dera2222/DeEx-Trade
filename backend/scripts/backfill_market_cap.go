package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/db"
	"github.com/joho/godotenv"
	"gorm.io/gorm"
)

const (
	GECKO_API_BASE = "https://api.geckoterminal.com/api/v2"
	MAX_WORKERS    = 5
	BATCH_DELAY    = time.Second * 2
)

type GeckoPoolResponse struct {
	Data GeckoPoolData `json:"data"`
}

type GeckoPoolData struct {
	Attributes GeckoPoolAttributes `json:"attributes"`
}

type GeckoPoolAttributes struct {
	MarketCapUSD float64 `json:"market_cap_usd"`
}

type Pair struct {
	ID           string     `gorm:"primaryKey"`
	Network      string
	PoolAddress  string
	MarketCapUSD *float64
	MarketCap    *int64
	UpdatedAt    time.Time
}

func (Pair) TableName() string {
	return "pairs"
}

func fetchPoolDetails(network, poolAddress string) (*GeckoPoolResponse, error) {
	url := fmt.Sprintf("%s/networks/%s/pools/%s?include=base_token,quote_token", GECKO_API_BASE, network, poolAddress)
	
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result GeckoPoolResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func backfillMarketCaps(database *gorm.DB) {
	log.Println("Starting market cap backfill...")
	
	// Fetch all pairs
	var pairs []Pair
	if err := database.Where("pool_address IS NOT NULL AND pool_address != ''").Find(&pairs).Error; err != nil {
		log.Fatalf("Failed to fetch pairs: %v", err)
	}

	log.Printf("Found %d total pairs\n", len(pairs))

	// Filter pairs without market cap
	var pairsWithoutMarketCap []Pair
	for _, p := range pairs {
		if p.MarketCapUSD == nil || *p.MarketCapUSD == 0 {
			pairsWithoutMarketCap = append(pairsWithoutMarketCap, p)
		}
	}

	log.Printf("Found %d pairs without market cap data\n\n", len(pairsWithoutMarketCap))

	if len(pairsWithoutMarketCap) == 0 {
		log.Println("All pairs already have market cap data!")
		return
	}

	// Process pairs with worker pool
	type Result struct {
		ID           string
		MarketCapUSD float64
		Error        error
	}

	pairChan := make(chan Pair, len(pairsWithoutMarketCap))
	resultChan := make(chan Result, len(pairsWithoutMarketCap))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < MAX_WORKERS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pair := range pairChan {
				result := Result{ID: pair.ID}
				
				poolData, err := fetchPoolDetails(pair.Network, pair.PoolAddress)
				if err != nil {
					result.Error = err
					resultChan <- result
					continue
				}

				marketCapUSD := poolData.Data.Attributes.MarketCapUSD
				result.MarketCapUSD = marketCapUSD
				resultChan <- result
			}
		}()
	}

	// Send pairs to workers
	go func() {
		for _, pair := range pairsWithoutMarketCap {
			pairChan <- pair
		}
		close(pairChan)
	}()

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results and update database
	updated := 0
	failed := 0
	batchCount := 0

	for result := range resultChan {
		if result.Error != nil {
			log.Printf("✗ Error fetching %s: %v\n", result.ID, result.Error)
			failed++
			continue
		}

		// Update database
		marketCap := int64(result.MarketCapUSD)
		if err := database.Model(&Pair{}).Where("id = ?", result.ID).Updates(map[string]interface{}{
			"market_cap_usd": result.MarketCapUSD,
			"market_cap":     marketCap,
			"updated_at":     time.Now(),
		}).Error; err != nil {
			log.Printf("✗ Failed to update %s: %v\n", result.ID, err)
			failed++
			continue
		}

		log.Printf("✓ Updated %s: $%.2f\n", result.ID, result.MarketCapUSD)
		updated++
		batchCount++

		// Rate limiting
		if batchCount%10 == 0 {
			log.Println("Waiting to respect rate limits...")
			time.Sleep(BATCH_DELAY)
		}
	}

	log.Printf("\n✅ Backfill complete!\n")
	log.Printf("Updated: %d pairs\n", updated)
	log.Printf("Failed: %d pairs\n", failed)
	log.Printf("Total processed: %d pairs\n", updated+failed)
}

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// Load configuration
	cfg := config.Load()

	// Initialize database
	database, err := db.New(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	log.Println("Database connected successfully!")

	// Run backfill
	backfillMarketCaps(database.DB)
}
