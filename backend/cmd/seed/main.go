package main

import (
	"log"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg := config.Load()

	database, err := db.New(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	log.Println("Database connected successfully!")

	pairs := []models.Pair{
		{
			ID:          "bsc_0x1b54cd932b6b751803c996cbef36280b53795f81",
			Network:     "bsc",
			BaseToken:   `{"address": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", "symbol": "WBNB", "name": "Wrapped BNB", "decimals": 18}`,
			QuoteToken:  `{"address": "0xe9e7cea3dedca5984780bafc599bd69add087d56", "symbol": "BUSD", "name": "BUSD Token", "decimals": 18}`,
			BaseSymbol:  "WBNB",
			QuoteSymbol: "BUSD",
			DEX:         "PancakeSwap",
			PoolAddress: "0x1b54cd932b6b751803c996cbef36280b53795f81",
			CreatedAt:   time.Now(),
		},
		{
			ID:          "bsc_0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
			Network:     "bsc",
			BaseToken:   `{"address": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82", "symbol": "CAKE", "name": "PancakeSwap Token", "decimals": 18}`,
			QuoteToken:  `{"address": "0xe9e7cea3dedca5984780bafc599bd69add087d56", "symbol": "BUSD", "name": "BUSD Token", "decimals": 18}`,
			BaseSymbol:  "CAKE",
			QuoteSymbol: "BUSD",
			DEX:         "PancakeSwap",
			PoolAddress: "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
			CreatedAt:   time.Now(),
		},
		{
			ID:          "bsc_0x1b54cd932b6b751803c996cbef36280b53795f82",
			Network:     "bsc",
			BaseToken:   `{"address": "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c", "symbol": "BTCB", "name": "BTCB Token", "decimals": 18}`,
			QuoteToken:  `{"address": "0xe9e7cea3dedca5984780bafc599bd69add087d56", "symbol": "BUSD", "name": "BUSD Token", "decimals": 18}`,
			BaseSymbol:  "BTCB",
			QuoteSymbol: "BUSD",
			DEX:         "PancakeSwap",
			PoolAddress: "0x1b54cd932b6b751803c996cbef36280b53795f82",
			CreatedAt:   time.Now(),
		},
		{
			ID:          "bsc_0x1b54cd932b6b751803c996cbef36280b53795f83",
			Network:     "bsc",
			BaseToken:   `{"address": "0x2170ed0880ac9a755fd29b2688956bd959f933f8", "symbol": "ETH", "name": "Ethereum Token", "decimals": 18}`,
			QuoteToken:  `{"address": "0xe9e7cea3dedca5984780bafc599bd69add087d56", "symbol": "BUSD", "name": "BUSD Token", "decimals": 18}`,
			BaseSymbol:  "ETH",
			QuoteSymbol: "BUSD",
			DEX:         "PancakeSwap",
			PoolAddress: "0x1b54cd932b6b751803c996cbef36280b53795f83",
			CreatedAt:   time.Now(),
		},
		{
			ID:          "base_0xf8efb5234b2ca948e51e126acc7b543d761afa5a4d183b4f9371ad2ae135341b",
			Network:     "base",
			BaseToken:   `{"address": "0x4200000000000000000000000000000000000006", "symbol": "WETH", "name": "Wrapped Ether", "decimals": 18}`,
			QuoteToken:  `{"address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", "symbol": "USDC", "name": "USD Coin", "decimals": 6}`,
			BaseSymbol:  "WETH",
			QuoteSymbol: "USDC",
			DEX:         "Uniswap v3",
			PoolAddress: "0xf8efb5234b2ca948e51e126acc7b543d761afa5a4d183b4f9371ad2ae135341b",
			CreatedAt:   time.Now(),
		},
		{
			ID:          "base_0xada0861fe7af31d338f7292df25683a6741a3b11fa42ce9e7fbdd530cb988f6c",
			Network:     "base",
			BaseToken:   `{"address": "0x4200000000000000000000000000000000000006", "symbol": "WETH", "name": "Wrapped Ether", "decimals": 18}`,
			QuoteToken:  `{"address": "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca", "symbol": "USDbC", "name": "USD Base Coin", "decimals": 6}`,
			BaseSymbol:  "WETH",
			QuoteSymbol: "USDbC",
			DEX:         "Uniswap v3",
			PoolAddress: "0xada0861fe7af31d338f7292df25683a6741a3b11fa42ce9e7fbdd530cb988f6c",
			CreatedAt:   time.Now(),
		},
	}

	tokens := []models.Token{
		{ID: "bsc_0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", Network: "bsc", Address: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", Symbol: "WBNB", Name: "Wrapped BNB"},
		{ID: "bsc_0xe9e7cea3dedca5984780bafc599bd69add087d56", Network: "bsc", Address: "0xe9e7cea3dedca5984780bafc599bd69add087d56", Symbol: "BUSD", Name: "BUSD Token"},
		{ID: "bsc_0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82", Network: "bsc", Address: "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82", Symbol: "CAKE", Name: "PancakeSwap Token"},
		{ID: "bsc_0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c", Network: "bsc", Address: "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c", Symbol: "BTCB", Name: "BTCB Token"},
		{ID: "bsc_0x2170ed0880ac9a755fd29b2688956bd959f933f8", Network: "bsc", Address: "0x2170ed0880ac9a755fd29b2688956bd959f933f8", Symbol: "ETH", Name: "Ethereum Token"},
		{ID: "base_0x4200000000000000000000000000000000000006", Network: "base", Address: "0x4200000000000000000000000000000000000006", Symbol: "WETH", Name: "Wrapped Ether"},
		{ID: "base_0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", Network: "base", Address: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", Symbol: "USDC", Name: "USD Coin"},
		{ID: "base_0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca", Network: "base", Address: "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca", Symbol: "USDbC", Name: "USD Base Coin"},
	}

	for _, token := range tokens {
		if err := database.Create(&token).Error; err != nil {
			log.Printf("Failed to insert token %s: %v", token.ID, err)
		} else {
			log.Printf("Inserted token %s", token.ID)
		}
	}

	for _, pair := range pairs {
		if err := database.Create(&pair).Error; err != nil {
			log.Printf("Failed to insert pair %s: %v", pair.ID, err)
		} else {
			log.Printf("Inserted pair %s", pair.ID)
		}
	}

	log.Println("Seeding completed!")
}
