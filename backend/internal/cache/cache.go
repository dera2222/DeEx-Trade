package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"github.com/nexus/backend/internal/repository"
	"github.com/go-redis/redis/v8"
)

type PairResponseBuilder func(ctx context.Context, pair *models.Pair) ([]byte, error)

type CacheManager struct {
	redis        *db.RedisClient
	pairRepo     *repository.PairRepository
	orderRepo    *repository.OrderRepository
	refreshLimit int
}

const (
	cacheTTL            = 120 * time.Second
	cacheWorkerInterval = 3 * time.Second
	cacheLockTTL        = 2 * time.Second
	maxPairCacheWorkers = 20
)

func NewManager(redis *db.RedisClient, pairRepo *repository.PairRepository, orderRepo *repository.OrderRepository) *CacheManager {
	if redis == nil {
		return nil
	}
	return &CacheManager{
		redis:        redis,
		pairRepo:     pairRepo,
		orderRepo:    orderRepo,
		refreshLimit: 500,
	}
}

func (c *CacheManager) IsEnabled() bool {
	return c != nil && c.redis != nil
}

func (c *CacheManager) Start(ctx context.Context, builder PairResponseBuilder) {
	if !c.IsEnabled() {
		return
	}

	log.Println("[CacheWorker] starting active pair refresh worker")
	c.RefreshActivePairs(ctx, builder)

	ticker := time.NewTicker(cacheWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[CacheWorker] stopping cache worker")
			return
		case <-ticker.C:
			c.RefreshActivePairs(ctx, builder)
		}
	}
}

func (c *CacheManager) RefreshActivePairs(ctx context.Context, builder PairResponseBuilder) {
	pairs, err := c.pairRepo.GetAllActive(ctx, c.refreshLimit)
	if err != nil {
		log.Printf("[CacheWorker] failed to load active pairs: %v", err)
		return
	}
	log.Printf("[CacheWorker] refreshing cache for %d active pairs", len(pairs))

	responses := make([][]byte, 0, len(pairs))
	responseNetworks := make(map[string][][]byte)
	var mu sync.Mutex
	sem := make(chan struct{}, maxPairCacheWorkers)
	wg := sync.WaitGroup{}

	for _, pair := range pairs {
		pairCopy := pair
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			payload, err := builder(ctx, &pairCopy)
			if err != nil {
				log.Printf("[CacheWorker] failed to build pair response for %s: %v", pairCopy.ID, err)
				return
			}

			if err := c.CachePair(ctx, pairCopy.ID, payload); err != nil {
				log.Printf("[CacheWorker] failed to cache pair %s: %v", pairCopy.ID, err)
			}

			stats, err := c.pairRepo.GetStats(ctx, pairCopy.ID)
			if err == nil {
				if statBytes, err := json.Marshal(stats); err == nil {
					if err := c.CacheStats(ctx, pairCopy.ID, statBytes); err != nil {
						log.Printf("[CacheWorker] failed to cache stats %s: %v", pairCopy.ID, err)
					}
				}
			}

			if ob, err := c.orderRepo.GetOrderBook(ctx, pairCopy.ID, pairCopy.Network, 50); err == nil {
				if obBytes, err := json.Marshal(ob); err == nil {
					if err := c.CacheOrderbook(ctx, pairCopy.ID, obBytes); err != nil {
						log.Printf("[CacheWorker] failed to cache orderbook %s: %v", pairCopy.ID, err)
					}
				}
			}

			mu.Lock()
			responses = append(responses, payload)
			responseNetworks[string(pairCopy.Network)] = append(responseNetworks[string(pairCopy.Network)], payload)
			mu.Unlock()
		}()
	}

	wg.Wait()

	if len(responses) == 0 {
		log.Println("[CacheWorker] no pair responses built, skipping cache write")
		return
	}

	allBytes, err := json.Marshal(responses)
	if err != nil {
		log.Printf("[CacheWorker] failed to marshal pairs list: %v", err)
		return
	}

	if err := c.CachePairsAll(ctx, "", allBytes); err != nil {
		log.Printf("[CacheWorker] failed to cache pairs list: %v", err)
	} else {
		log.Printf("[CacheWorker] cached all pairs list (%d bytes)", len(allBytes))
	}

	for network, group := range responseNetworks {
		groupBytes, err := json.Marshal(group)
		if err != nil {
			log.Printf("[CacheWorker] failed to marshal network group %s: %v", network, err)
			continue
		}
		if err := c.CachePairsAll(ctx, network, groupBytes); err != nil {
			log.Printf("[CacheWorker] failed to cache network pairs %s: %v", network, err)
		}
	}
}

func (c *CacheManager) RefreshPair(ctx context.Context, pairID string, builder PairResponseBuilder) error {
	if !c.IsEnabled() {
		return nil
	}

	log.Printf("[Cache] refreshing pair cache for %s", pairID)
	lockKey := fmt.Sprintf("cache-lock:pair:%s", pairID)
	acquired, err := c.redis.TryAcquireLock(ctx, lockKey, cacheLockTTL)
	if err != nil {
		log.Printf("[Cache] failed to acquire lock for pair %s: %v", pairID, err)
		return err
	}
	if !acquired {
		return nil
	}
	defer func() {
		if err := c.redis.ReleaseLock(ctx, lockKey); err != nil {
			log.Printf("[Cache] failed to release lock for pair %s: %v", pairID, err)
		}
	}()

	pair, err := c.pairRepo.GetByID(ctx, pairID)
	if err != nil {
		return err
	}

	payload, err := builder(ctx, pair)
	if err != nil {
		return err
	}

	if err := c.CachePair(ctx, pairID, payload); err != nil {
		return err
	}

	stats, err := c.pairRepo.GetStats(ctx, pairID)
	if err == nil {
		if statBytes, err := json.Marshal(stats); err == nil {
			if err := c.CacheStats(ctx, pairID, statBytes); err != nil {
				return err
			}
		}
	}

	if ob, err := c.orderRepo.GetOrderBook(ctx, pairID, pair.Network, 50); err == nil {
		if obBytes, err := json.Marshal(ob); err == nil {
			if err := c.CacheOrderbook(ctx, pairID, obBytes); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *CacheManager) GetCachedPairs(ctx context.Context, network string) ([]byte, error) {
	val, err := c.redis.GetPairsAll(ctx, network)
	if err != nil {
		if err == redis.Nil {
			log.Printf("[Cache] MISS pairs network=%s", network)
		}
		return nil, err
	}
	return []byte(val), nil
}

func (c *CacheManager) CachePairsAll(ctx context.Context, network string, payload []byte) error {
	err := c.redis.SetPairsAll(ctx, network, string(payload), cacheTTL)
	return err
}

func (c *CacheManager) Ping(ctx context.Context) error {
	if !c.IsEnabled() {
		return fmt.Errorf("cache disabled")
	}
	return c.redis.Ping(ctx).Err()
}

func (c *CacheManager) GetCachedPair(ctx context.Context, pairID string) ([]byte, error) {
	val, err := c.redis.GetPair(ctx, pairID)
	if err != nil {
		if err == redis.Nil {
			log.Printf("[Cache] MISS pair=%s", pairID)
		}
		return nil, err
	}
	return []byte(val), nil
}

func (c *CacheManager) CachePair(ctx context.Context, pairID string, payload []byte) error {
	err := c.redis.SetPair(ctx, pairID, string(payload), cacheTTL)
	return err
}

func (c *CacheManager) GetCachedOrderbook(ctx context.Context, pairID string) ([]byte, error) {
	val, err := c.redis.GetPairOrderbook(ctx, pairID)
	if err != nil {
		if err == redis.Nil {
			log.Printf("[Cache] MISS orderbook=%s", pairID)
		}
		return nil, err
	}
	return []byte(val), nil
}

func (c *CacheManager) CacheOrderbook(ctx context.Context, pairID string, payload []byte) error {
	err := c.redis.SetPairOrderbook(ctx, pairID, string(payload), cacheTTL)
	return err
}

func (c *CacheManager) GetCachedStats(ctx context.Context, pairID string) ([]byte, error) {
	val, err := c.redis.GetPairStats(ctx, pairID)
	if err != nil {
		if err == redis.Nil {
			log.Printf("[Cache] MISS stats=%s", pairID)
		}
		return nil, err
	}
	return []byte(val), nil
}

func (c *CacheManager) CacheStats(ctx context.Context, pairID string, payload []byte) error {
	err := c.redis.SetPairStats(ctx, pairID, string(payload), cacheTTL)
	return err
}

func (c *CacheManager) DeleteCachedPairs(ctx context.Context, network string) error {
	return c.redis.DeletePairsAll(ctx, network)
}

func (c *CacheManager) DeleteCachedPair(ctx context.Context, pairID string) error {
	return c.redis.DeletePair(ctx, pairID)
}

func (c *CacheManager) DeleteCachedOrderbook(ctx context.Context, pairID string) error {
	return c.redis.DeletePairOrderbook(ctx, pairID)
}

func (c *CacheManager) DeleteCachedStats(ctx context.Context, pairID string) error {
	return c.redis.DeletePairStats(ctx, pairID)
}

func (c *CacheManager) ClearAllPairs(ctx context.Context) error {
	return c.redis.ClearAllPairs(ctx)
}

// GetRaw reads an arbitrary Redis key and returns its bytes.
// Used by buildPairResponse to read price-worker keys (pair:price:{id}).
func (c *CacheManager) GetRaw(ctx context.Context, key string) ([]byte, error) {
	if !c.IsEnabled() {
		return nil, fmt.Errorf("cache disabled")
	}
	val, err := c.redis.Get(ctx, key).Bytes()
	return val, err
}
