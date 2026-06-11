package db

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	*redis.Client
}

func NewRedis(cfg *config.Config) (*RedisClient, error) {
	var client *redis.Client

	// If Redis URL is provided (Upstash), parse and use it with TLS
	if cfg.RedisURL != "" {
		log.Printf("Connecting to Redis with URL: %s", cfg.RedisURL)
		// Parse redis://password@host:port or rediss://password@host:port
		opts, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
		}
		if strings.HasPrefix(strings.ToLower(cfg.RedisURL), "rediss://") || cfg.RedisTLS {
			opts.TLSConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		} else {
			opts.TLSConfig = nil
		}
		log.Printf("Redis parsed options: Addr=%s, Password set=%v, TLS=%v", opts.Addr, opts.Password != "", opts.TLSConfig != nil)
		client = redis.NewClient(opts)
	} else if cfg.RedisHost != "" && cfg.RedisPort > 0 {
		log.Printf("Connecting to Redis: host=%s port=%d tls=%v", cfg.RedisHost, cfg.RedisPort, cfg.RedisTLS)
		options := &redis.Options{
			Addr:     fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
			Password: cfg.RedisPassword,
			DB:       0,
		}
		if cfg.RedisTLS {
			options.TLSConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}
		client = redis.NewClient(options)
	} else {
		return nil, fmt.Errorf("no Redis configuration provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Println("Pinging Redis...")
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	log.Println("Connected to Redis successfully")
	return &RedisClient{client}, nil
}

func (r *RedisClient) Close() error {
	return r.Client.Close()
}

func (r *RedisClient) pairKey(pairID string) string {
	return fmt.Sprintf("pair:%s", pairID)
}

func (r *RedisClient) pairStatsKey(pairID string) string {
	return fmt.Sprintf("pair:%s:stats", pairID)
}

func (r *RedisClient) pairOrderbookKey(pairID string) string {
	return fmt.Sprintf("pair:%s:orderbook", pairID)
}

func (r *RedisClient) pairsAllKey(network string) string {
	if network == "" {
		return "pairs:all"
	}
	return fmt.Sprintf("pairs:all:%s", strings.ToLower(network))
}

// General pair cache
func (r *RedisClient) GetPair(ctx context.Context, pairID string) (string, error) {
	return r.Get(ctx, r.pairKey(pairID)).Result()
}

func (r *RedisClient) SetPair(ctx context.Context, pairID string, data string, expiration time.Duration) error {
	return r.Set(ctx, r.pairKey(pairID), data, expiration).Err()
}

func (r *RedisClient) GetPairStats(ctx context.Context, pairID string) (string, error) {
	return r.Get(ctx, r.pairStatsKey(pairID)).Result()
}

func (r *RedisClient) SetPairStats(ctx context.Context, pairID string, data string, expiration time.Duration) error {
	return r.Set(ctx, r.pairStatsKey(pairID), data, expiration).Err()
}

func (r *RedisClient) GetPairOrderbook(ctx context.Context, pairID string) (string, error) {
	return r.Get(ctx, r.pairOrderbookKey(pairID)).Result()
}

func (r *RedisClient) SetPairOrderbook(ctx context.Context, pairID string, data string, expiration time.Duration) error {
	return r.Set(ctx, r.pairOrderbookKey(pairID), data, expiration).Err()
}

func (r *RedisClient) GetPairsAll(ctx context.Context, network string) (string, error) {
	return r.Get(ctx, r.pairsAllKey(network)).Result()
}

func (r *RedisClient) SetPairsAll(ctx context.Context, network string, data string, expiration time.Duration) error {
	return r.Set(ctx, r.pairsAllKey(network), data, expiration).Err()
}

func (r *RedisClient) DeletePairsAll(ctx context.Context, network string) error {
	return r.Del(ctx, r.pairsAllKey(network)).Err()
}

func (r *RedisClient) DeletePair(ctx context.Context, pairID string) error {
	return r.Del(ctx, r.pairKey(pairID)).Err()
}

func (r *RedisClient) DeletePairOrderbook(ctx context.Context, pairID string) error {
	return r.Del(ctx, r.pairOrderbookKey(pairID)).Err()
}

func (r *RedisClient) DeletePairStats(ctx context.Context, pairID string) error {
	return r.Del(ctx, r.pairStatsKey(pairID)).Err()
}

func (r *RedisClient) TryAcquireLock(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return r.SetNX(ctx, key, "1", expiration).Result()
}

func (r *RedisClient) ReleaseLock(ctx context.Context, key string) error {
	return r.Del(ctx, key).Err()
}

// Orderbook caching
func (r *RedisClient) GetOrderbook(ctx context.Context, pairID string) (string, error) {
	return r.GetPairOrderbook(ctx, pairID)
}

func (r *RedisClient) SetOrderbook(ctx context.Context, pairID string, data string, expiration time.Duration) error {
	return r.SetPairOrderbook(ctx, pairID, data, expiration)
}

// Ticker caching
func (r *RedisClient) GetTicker(ctx context.Context, pairID string) (string, error) {
	return r.Get(ctx, fmt.Sprintf("ticker:%s", pairID)).Result()
}

func (r *RedisClient) SetTicker(ctx context.Context, pairID string, data string, expiration time.Duration) error {
	return r.Set(ctx, fmt.Sprintf("ticker:%s", pairID), data, expiration).Err()
}

// Pub/Sub for real-time updates
func (r *RedisClient) Publish(ctx context.Context, channel string, message string) error {
	return r.Client.Publish(ctx, channel, message).Err()
}

func (r *RedisClient) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return r.Client.Subscribe(ctx, channels...)
}

// Order state caching
func (r *RedisClient) CacheOrder(ctx context.Context, orderID string, data string, expiration time.Duration) error {
	return r.Set(ctx, fmt.Sprintf("order:%s", orderID), data, expiration).Err()
}

func (r *RedisClient) GetCachedOrder(ctx context.Context, orderID string) (string, error) {
	return r.Get(ctx, fmt.Sprintf("order:%s", orderID)).Result()
}

func (r *RedisClient) InvalidateOrderCache(ctx context.Context, orderID string) error {
	return r.Del(ctx, fmt.Sprintf("order:%s", orderID)).Err()
}

// User session caching
func (r *RedisClient) SetUserSession(ctx context.Context, userID string, sessionData string, expiration time.Duration) error {
	return r.Set(ctx, fmt.Sprintf("session:%s", userID), sessionData, expiration).Err()
}

func (r *RedisClient) GetUserSession(ctx context.Context, userID string) (string, error) {
	return r.Get(ctx, fmt.Sprintf("session:%s", userID)).Result()
}

func (r *RedisClient) DeleteUserSession(ctx context.Context, userID string) error {
	return r.Del(ctx, fmt.Sprintf("session:%s", userID)).Err()
}

// Rate limiting
func (r *RedisClient) IncrRateLimit(ctx context.Context, key string, expiration time.Duration) (int64, error) {
	pipe := r.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, expiration)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

func (r *RedisClient) GetRateLimit(ctx context.Context, key string) (int64, error) {
	val, err := r.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// ClearAllPairs deletes all pair-related cache keys
func (r *RedisClient) ClearAllPairs(ctx context.Context) error {
	// Use KEYS to find all pair-related keys
	// Note: KEYS is not recommended for production, but ok for debugging
	patterns := []string{"pair:*", "pairs:*"}
	
	for _, pattern := range patterns {
		keys, err := r.Keys(ctx, pattern).Result()
		if err != nil {
			return fmt.Errorf("failed to get keys for pattern %s: %w", pattern, err)
		}
		
		if len(keys) > 0 {
			if err := r.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("failed to delete keys for pattern %s: %w", pattern, err)
			}
			log.Printf("Deleted %d keys matching pattern %s", len(keys), pattern)
		}
	}
	
	return nil
}
