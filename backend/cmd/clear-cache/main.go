package main

import (
	"context"
	"log"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/db"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg := config.Load()

	redis, err := db.NewRedis(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redis.Close()

	log.Println("Connected to Redis, clearing all pair cache...")

	if err := redis.ClearAllPairs(context.Background()); err != nil {
		log.Fatalf("Failed to clear cache: %v", err)
	}

	log.Println("Successfully cleared all pair cache from Redis")
}
