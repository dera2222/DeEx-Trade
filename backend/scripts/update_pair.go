package main

import (
	"context"
	"log"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/repository"
	"github.com/joho/godotenv"
)

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

	// Create repository
	pairRepo := repository.NewPairRepository(database, nil)

	// Get the pair
	pair, err := pairRepo.GetByID(context.Background(), "base_0xdbc6998296caa1652a810dc8d3baf4a8294330f1")
	if err != nil {
		log.Fatalf("Failed to get pair: %v", err)
	}

	// Update the quote token to include decimals
	pair.QuoteToken = `{"logo": "https://coin-images.coingecko.com/coins/images/6319/large/USDC.png?1769615602", "name": "USD Coin", "symbol": "USDC", "address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", "decimals": 6}`

	// Update the pair
	err = pairRepo.Update(context.Background(), pair)
	if err != nil {
		log.Fatalf("Failed to update pair: %v", err)
	}

	log.Println("Successfully updated pair with decimals!")
}
