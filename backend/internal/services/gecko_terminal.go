package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/shopspring/decimal"
)

// GeckoTerminalService handles market data from Gecko Terminal API
type GeckoTerminalService struct {
	cfg      *config.Config
	httpClient *http.Client
}

// GeckoTerminalPool represents pool data from Gecko Terminal
type GeckoTerminalPool struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Attributes PoolAttributes  `json:"attributes"`
}

type PoolAttributes struct {
	Name              string          `json:"name"`
	Address           string          `json:"address"`
	PoolCreatedAt     time.Time       `json:"pool_created_at"`
	TokenPriceUSD     string          `json:"token_price_usd"`
	FdvUSD            string          `json:"fdv_usd"` // Fully Diluted Valuation (Market Cap)
	MarketCapUSD      string          `json:"market_cap_usd"`
	PriceChange24h    string          `json:"price_change_percentage_24h"`
	Volume24h         string          `json:"volume_usd_24h"`
	ReserveInUSD      string          `json:"reserve_in_usd"`
	BaseTokenPriceUSD string          `json:"base_token_price_usd"`
	BaseTokenPriceNative string       `json:"base_token_price_native_quoted"`
	QuoteTokenPriceUSD string         `json:"quote_token_price_usd"`
	QuoteTokenPriceNative string      `json:"quote_token_price_native_quoted"`
}

// GeckoTerminalResponse represents the API response
type GeckoTerminalResponse struct {
	Data GeckoTerminalPool `json:"data"`
}

// Token metadata from Gecko Terminal
type TokenMetadata struct {
	Address           string   `json:"address"`
	Name              string   `json:"name"`
	Symbol            string   `json:"symbol"`
	Decimals          int      `json:"decimals"`
	ImageThumb        string   `json:"image_thumb"`
	ImageSmall        string   `json:"image_small"`
	ImageLarge        string   `json:"image_large"`
	Description       string   `json:"description"`
	Websites          []string `json:"websites"`
	TwitterHandle     string   `json:"twitter_handle"`
	TelegramHandle    string   `json:"telegram_handle"`
	DiscordUrl        string   `json:"discord_url"`
	Categories        []string `json:"categories"`
	GTScore           float64  `json:"gt_score"`
	GTVerified        bool     `json:"gt_verified"`
	CoingeckoId       string   `json:"coingecko_id"`
}

// PoolInfoResponse represents the pool info API response with included token data
type PoolInfoResponse struct {
	Data     []interface{} `json:"data"`
	Included []interface{} `json:"included"`
}

// PoolInfo represents complete pool information with token details
type PoolInfo struct {
	BaseTokenDecimals  int
	QuoteTokenDecimals int
	BaseTokenMetadata  *TokenMetadata
	QuoteTokenMetadata *TokenMetadata
}

func NewGeckoTerminalService(cfg *config.Config) *GeckoTerminalService {
	return &GeckoTerminalService{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GetPoolMarketCap fetches market cap for a pool from Gecko Terminal
func (s *GeckoTerminalService) GetPoolMarketCap(ctx context.Context, network, poolAddress string) (decimal.Decimal, error) {
	// Map network names to Gecko Terminal network identifiers
	networkID := s.mapNetworkToGeckoTerminal(network)
	if networkID == "" {
		return decimal.Zero, fmt.Errorf("unsupported network: %s", network)
	}

	url := fmt.Sprintf("https://api.geckoterminal.com/api/v2/networks/%s/pools/%s", networkID, poolAddress)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers if needed (Gecko Terminal doesn't require API key for basic data)
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to fetch pool data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return decimal.Zero, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var apiResp GeckoTerminalResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return decimal.Zero, fmt.Errorf("failed to decode response: %w", err)
	}

	// Try market_cap_usd first, fallback to fdv_usd
	marketCapStr := apiResp.Data.Attributes.MarketCapUSD
	if marketCapStr == "" || marketCapStr == "0" {
		marketCapStr = apiResp.Data.Attributes.FdvUSD
	}

	if marketCapStr == "" || marketCapStr == "0" {
		return decimal.Zero, fmt.Errorf("no market cap data available for pool %s", poolAddress)
	}

	marketCap, err := decimal.NewFromString(marketCapStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse market cap: %w", err)
	}

	return marketCap, nil
}

// GetPoolInfo fetches complete pool information with token decimals and metadata
func (s *GeckoTerminalService) GetPoolInfo(ctx context.Context, network, poolAddress string) (*PoolInfo, error) {
	// Map network names to Gecko Terminal network identifiers
	networkID := s.mapNetworkToGeckoTerminal(network)
	if networkID == "" {
		return nil, fmt.Errorf("unsupported network: %s", network)
	}

	// Use the /info endpoint to get pool data with included tokens
	url := fmt.Sprintf("https://api.geckoterminal.com/api/v2/networks/%s/pools/%s/info?include=pool", networkID, poolAddress)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pool info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var apiResp PoolInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	poolInfo := &PoolInfo{}

	// Parse tokens from data array
	if len(apiResp.Data) > 0 {
		for _, item := range apiResp.Data {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			itemType, ok := itemMap["type"].(string)
			if !ok {
				continue
			}

			attributes, ok := itemMap["attributes"].(map[string]interface{})
			if !ok {
				continue
			}

			if itemType == "token" {
				decimals, _ := attributes["decimals"].(float64)
				tokenMetadata := &TokenMetadata{
					Address:        s.getStringFromMap(attributes, "address"),
					Name:           s.getStringFromMap(attributes, "name"),
					Symbol:         s.getStringFromMap(attributes, "symbol"),
					Decimals:       int(decimals),
					ImageThumb:     s.getStringFromMap(attributes, "image.thumb"),
					ImageSmall:     s.getStringFromMap(attributes, "image.small"),
					ImageLarge:     s.getStringFromMap(attributes, "image.large"),
					Description:    s.getStringFromMap(attributes, "description"),
					TwitterHandle:  s.getStringFromMap(attributes, "twitter_handle"),
					TelegramHandle: s.getStringFromMap(attributes, "telegram_handle"),
					DiscordUrl:     s.getStringFromMap(attributes, "discord_url"),
					CoingeckoId:    s.getStringFromMap(attributes, "coingecko_coin_id"),
					GTVerified:     s.getBoolFromMap(attributes, "gt_verified"),
				}

				// Parse GT score
				if gtScore, ok := attributes["gt_score"].(float64); ok {
					tokenMetadata.GTScore = gtScore
				}

				// Parse categories
				if categories, ok := attributes["categories"].([]interface{}); ok {
					for _, cat := range categories {
						if catStr, ok := cat.(string); ok {
							tokenMetadata.Categories = append(tokenMetadata.Categories, catStr)
						}
					}
				}

				// Parse websites
				if websites, ok := attributes["websites"].([]interface{}); ok {
					for _, website := range websites {
						if websiteStr, ok := website.(string); ok {
							tokenMetadata.Websites = append(tokenMetadata.Websites, websiteStr)
						}
					}
				}

				// Determine if this is base or quote token
				// The first token in the response is typically base token, second is quote
				if poolInfo.BaseTokenMetadata == nil {
					poolInfo.BaseTokenMetadata = tokenMetadata
					poolInfo.BaseTokenDecimals = tokenMetadata.Decimals
				} else if poolInfo.QuoteTokenMetadata == nil {
					poolInfo.QuoteTokenMetadata = tokenMetadata
					poolInfo.QuoteTokenDecimals = tokenMetadata.Decimals
				}
			}
		}
	}

	// Also parse included data for pool info
	if len(apiResp.Included) > 0 {
		for _, item := range apiResp.Included {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			itemType, ok := itemMap["type"].(string)
			if !ok {
				continue
			}

			// For now, we process pool data if needed
			_ = itemType
		}
	}

	if poolInfo.BaseTokenMetadata == nil || poolInfo.QuoteTokenMetadata == nil {
		return nil, fmt.Errorf("could not extract both token metadata from pool info")
	}

	return poolInfo, nil
}

// Helper functions to safely extract values from maps
func (s *GeckoTerminalService) getStringFromMap(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func (s *GeckoTerminalService) getBoolFromMap(m map[string]interface{}, key string) bool {
	if val, ok := m[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}

// mapNetworkToGeckoTerminal maps our network names to Gecko Terminal identifiers
func (s *GeckoTerminalService) mapNetworkToGeckoTerminal(network string) string {
	switch network {
	case "bsc":
		return "bsc"
	case "base":
		return "base"
	case "ethereum":
		return "eth"
	default:
		return ""
	}
}