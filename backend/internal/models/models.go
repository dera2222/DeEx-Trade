package models

import (
	"encoding/json"
	"time"

	"github.com/shopspring/decimal"
)

type Network string

const (
	NetworkBSC    Network = "bsc"
	NetworkBase   Network = "base"
	NetworkSolana Network = "solana"
)

const (
	DepositStatusPending  = "pending"
	DepositStatusCredited = "credited"
	DepositStatusFailed   = "failed"
)

type RefundStatus string

const (
	RefundStatusPending    RefundStatus = "pending"
	RefundStatusProcessing RefundStatus = "processing"
	RefundStatusCompleted  RefundStatus = "completed"
	RefundStatusFailed     RefundStatus = "failed"
)

type OrderSide string

const (
	OrderSideBuy  OrderSide = "buy"
	OrderSideSell OrderSide = "sell"
)

type OrderType string

const (
	OrderTypeLimit      OrderType = "limit"
	OrderTypeMarket     OrderType = "market"
	OrderTypeStopLoss   OrderType = "stop_loss"
	OrderTypeTakeProfit OrderType = "take_profit"
	OrderTypePostOnly   OrderType = "post_only"
)

type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "pending"
	OrderStatusPartial   OrderStatus = "partial"
	OrderStatusFilled    OrderStatus = "filled"
	OrderStatusCancelled OrderStatus = "cancelled"
	OrderStatusExpired   OrderStatus = "expired"
	OrderStatusTriggered OrderStatus = "triggered"
	OrderStatusOpen      OrderStatus = "open"
)

// User represents a registered user
type User struct {
	ID           uint      `json:"id" db:"id"`
	Address      string    `json:"address" db:"address"`
	Email        string    `json:"email,omitempty" db:"email"`
	Username     string    `json:"username,omitempty" db:"username"`
	ReferralCode string    `json:"referral_code" db:"referral_code"`
	ReferredBy   string    `json:"referred_by,omitempty" db:"referred_by"`
	Network      Network   `json:"network" db:"network"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" db:"updated_at"`
}

type UserBalance struct {
	ID        uint            `json:"id" db:"id"`
	UserID    uint            `json:"user_id" db:"user_id"`
	Network   Network         `json:"network" db:"network"`
	TokenMint string          `json:"token_mint" db:"token_mint"`
	Available decimal.Decimal `json:"available_balance" db:"available_balance"`
	Locked    decimal.Decimal `json:"locked_balance" db:"locked_balance"`
	Total     decimal.Decimal `json:"total_balance" db:"total_balance"`
	CreatedAt time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt time.Time       `json:"updated_at" db:"updated_at"`
}

type SolanaDeposit struct {
	ID          uint            `json:"id" gorm:"primaryKey"`
	UserID      uint            `json:"user_id" gorm:"index"`
	UserAddress string          `json:"user_address" gorm:"type:varchar(66);index"`
	Network     Network         `json:"network" gorm:"type:varchar(20);not null"`
	TokenMint   string          `json:"token_mint" gorm:"type:varchar(100);not null"`
	Amount      decimal.Decimal `json:"amount" gorm:"type:numeric(40,20);not null"`
	Memo        string          `json:"memo" gorm:"type:varchar(255);uniqueIndex;not null"`
	TxHash      string          `json:"tx_hash" gorm:"type:varchar(128);index"`
	Status      string          `json:"status" gorm:"type:varchar(20);not null;default:'pending'"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// RefundRequest represents a request to refund tokens to a user
type RefundRequest struct {
	ID          uint            `json:"id" gorm:"primaryKey"`
	OrderID     uint            `json:"order_id" gorm:"index"`
	UserID      uint            `json:"user_id" gorm:"index"`
	Network     Network         `json:"network" gorm:"type:varchar(20);not null"`
	TokenMint   string          `json:"token_mint" gorm:"type:varchar(100);not null"`
	Amount      decimal.Decimal `json:"amount" gorm:"type:numeric(40,20);not null"`
	UserAddress string          `json:"user_address" gorm:"type:varchar(66);not null"`
	Status      RefundStatus    `json:"status" gorm:"type:varchar(20);not null;default:'pending'"`
	TxHash      string          `json:"tx_hash" gorm:"type:varchar(128);index"`
	ErrorMsg    string          `json:"error_msg" gorm:"type:text"`
	RetryCount  int             `json:"retry_count" gorm:"default:0"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// Order represents a maker order stored in the database
type Order struct {
	ID               uint            `json:"id" db:"id"`
	OrderHash        string          `json:"order_hash" db:"order_hash"`
	UserID           uint            `json:"user_id" db:"user_id"`
	Network          Network         `json:"network" db:"network"`
	PairID           string          `json:"pair_id" db:"pair_id"`
	Side             OrderSide       `json:"side" db:"side"`
	OrderType        OrderType       `json:"order_type" db:"order_type"`
	Price            decimal.Decimal `json:"price" db:"price"`
	Amount           decimal.Decimal `json:"amount" db:"amount"`
	FilledAmount     decimal.Decimal `json:"filled_amount" db:"filled_amount"`
	AmountIn         decimal.Decimal `json:"amount_in" db:"amount_in"`           // Total amount to receive/sell (in token's native decimals)
	AmountOutMin     decimal.Decimal `json:"amount_out_min" db:"amount_out_min"` // Minimum acceptable receive (in token's native decimals)
	TokenIn          string          `json:"token_in" db:"token_in"`
	TokenOut         string          `json:"token_out" db:"token_out"`
	Receiver         string          `json:"receiver,omitempty" db:"receiver"`
	Maker            string          `json:"maker" db:"maker"`
	Signature        string          `json:"signature" db:"signature"`
	Expiration       time.Time       `json:"expiration" db:"expiration"`
	Nonce            uint64          `json:"nonce" db:"nonce"`
	Salt             uint64          `json:"salt" db:"salt"`
	Status           OrderStatus     `json:"status" db:"status"`
	IsLadder         bool            `json:"is_ladder" db:"is_ladder"`
	LadderLevels     int             `json:"ladder_levels,omitempty" db:"ladder_levels"`
	LadderPriceStart decimal.Decimal `json:"ladder_price_start,omitempty" db:"ladder_price_start"`
	LadderPriceEnd   decimal.Decimal `json:"ladder_price_end,omitempty" db:"ladder_price_end"`
	LadderParentID   *uint           `json:"ladder_parent_id,omitempty" db:"ladder_parent_id"`
	// Total amounts for ladder parent orders (display purposes only, not used for filling)
	LadderTotalAmountIn     decimal.Decimal `json:"ladder_total_amount_in,omitempty" db:"ladder_total_amount_in"`
	LadderTotalAmountOutMin decimal.Decimal `json:"ladder_total_amount_out_min,omitempty" db:"ladder_total_amount_out_min"`
	CommitHash              string          `json:"commit_hash,omitempty" db:"commit_hash"`
	CommitRevealed          bool            `json:"commit_revealed" db:"commit_revealed"`
	CommitExpired           bool            `json:"commit_expired" db:"commit_expired"`
	// Stop Loss / Take Profit fields
	TriggerPrice decimal.Decimal `json:"trigger_price,omitempty" db:"trigger_price"`   // Price at which order triggers
	TriggeredAt  *time.Time      `json:"triggered_at,omitempty" db:"triggered_at"`     // When order was triggered
	IsPostOnly   bool            `json:"is_post_only" db:"is_post_only"`               // Post-only: don't fill immediately
	ReduceOnly   bool            `json:"reduce_only" db:"reduce_only"`                 // Reduce only: only reduce position
	TimeInForce  string          `json:"time_in_force" db:"time_in_force"`             // GTC, IOC, FOK
	StopLossType string          `json:"stop_loss_type,omitempty" db:"stop_loss_type"` // stop_loss or take_profit
	// Deposit fields — only populated for Solana custody-model orders.
	// Use pointers so that EVM orders (no deposit) map to NULL in the DB,
	// avoiding the unique constraint on deposit_memo for empty strings.
	DepositMemo      *string         `json:"deposit_memo,omitempty" db:"deposit_memo"`
	DepositAmount    decimal.Decimal `json:"deposit_amount,omitempty" db:"deposit_amount"`
	DepositTokenMint *string         `json:"deposit_token_mint,omitempty" db:"deposit_token_mint"`
	DepositType      *string         `json:"deposit_type,omitempty" db:"deposit_type"`
	DepositTxHash    *string         `json:"deposit_tx_hash,omitempty" db:"deposit_tx_hash"`
	AmountInDecimals  int       `json:"amount_in_decimals" db:"amount_in_decimals"`
	AmountOutDecimals int       `json:"amount_out_decimals" db:"amount_out_decimals"` // Decimals of token_out
	CreatedAt         time.Time `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time `json:"updated_at" db:"updated_at"`
}

// Fill represents a completed trade/fill
type Fill struct {
	ID           uint            `json:"id" db:"id"`
	Network      Network         `json:"network" db:"network"`
	PairID       string          `json:"pair_id" db:"pair_id"`
	OrderID      uint            `json:"order_id" db:"order_id"`
	MakerOrderID uint            `json:"maker_order_id" db:"maker_order_id"`
	TakerOrderID uint            `json:"taker_order_id" db:"taker_order_id"`
	Maker        string          `json:"maker" db:"maker"`
	Taker        string          `json:"taker" db:"taker"`
	Side         OrderSide       `json:"side" db:"side"`
	Price        decimal.Decimal `json:"price" db:"price"`
	Amount       decimal.Decimal `json:"amount" db:"amount"`
	AmountIn     decimal.Decimal `json:"amount_in" db:"amount_in"`
	AmountOut    decimal.Decimal `json:"amount_out" db:"amount_out"`
	Fee          decimal.Decimal `json:"fee" db:"fee"`
	TokenIn      string          `json:"token_in" db:"token_in"`
	TokenOut     string          `json:"token_out" db:"token_out"`
	TxHash       string          `json:"tx_hash" db:"tx_hash"`
	TxHashBuy    string          `json:"tx_hash_buy" db:"tx_hash_buy"`
	TxHashSell   string          `json:"tx_hash_sell" db:"tx_hash_sell"`
	BlockNumber  uint64          `json:"block_number" db:"block_number"`
	GasUsed      uint64          `json:"gas_used" db:"gas_used"`
	Status       string          `json:"status" db:"status"` // pending, settled, failed
	CreatedAt    time.Time       `json:"created_at" db:"created_at"`
}

// Pair represents a trading pair
type Pair struct {
	ID                 string          `json:"id" gorm:"column:id"`
	Network            Network         `json:"network" gorm:"column:network"`
	BaseToken          string          `json:"base_token" gorm:"column:base_token"`
	QuoteToken         string          `json:"quote_token" gorm:"column:quote_token"`
	BaseSymbol         string          `json:"base_symbol" gorm:"column:base_symbol"`
	QuoteSymbol        string          `json:"quote_symbol" gorm:"column:quote_symbol"`
	DEX                string          `json:"dex" gorm:"column:dex_name"`
	PoolAddress        string          `json:"pool_address" gorm:"column:pool_address"`
	// Price columns — populated by price-worker microservice via GeckoTerminal
	Price              decimal.Decimal `json:"price"              gorm:"column:price;type:numeric(40,20)"`
	PriceUSD           decimal.Decimal `json:"price_usd"          gorm:"column:price_usd;type:numeric(40,20)"`
	PriceChange24h     decimal.Decimal `json:"price_change_24h"   gorm:"column:price_change_24h;type:numeric(20,6)"`
	PriceHigh24h       decimal.Decimal `json:"price_high_24h"     gorm:"column:price_high_24h;type:numeric(40,20)"`
	PriceLow24h        decimal.Decimal `json:"price_low_24h"      gorm:"column:price_low_24h;type:numeric(40,20)"`
	Volume24h          decimal.Decimal `json:"volume_24h"         gorm:"column:volume_24h;type:numeric(40,20)"`
	Volume24hUSD       decimal.Decimal `json:"volume_24h_usd"     gorm:"column:volume_24h_usd;type:numeric(40,20)"`
	Liquidity          decimal.Decimal `json:"liquidity"          gorm:"column:liquidity;type:numeric(40,20)"`
	LiquidityUSD       decimal.Decimal `json:"liquidity_usd"      gorm:"column:liquidity_usd;type:numeric(40,20)"`
	MarketCap          decimal.Decimal `json:"market_cap"         gorm:"column:market_cap;type:numeric(40,20)"`
	MarketCapUSD       decimal.Decimal `json:"market_cap_usd"     gorm:"column:market_cap_usd;type:numeric(40,20)"`
	TrendingScore      decimal.Decimal  `json:"trending_score"      gorm:"column:trending_score;type:numeric(10,4)"`
	// LastTradePrice is the fill price of the most recent trade on this DEX.
	// Written by the Go backend on every fill. The UI uses this instead of the
	// GeckoTerminal price when it is less than 5 minutes old.
	LastTradePrice     *decimal.Decimal `json:"last_trade_price"    gorm:"column:last_trade_price;type:numeric(40,20)"`
	LastTradeAt        *time.Time       `json:"last_trade_at"       gorm:"column:last_trade_at"`
	// Token metadata
	BaseTokenDecimals  int             `json:"base_token_decimals"  gorm:"column:base_token_decimals"`
	QuoteTokenDecimals int             `json:"quote_token_decimals" gorm:"column:quote_token_decimals"`
	BaseTokenInfo      json.RawMessage `json:"base_token_info"      gorm:"column:base_token_info;type:jsonb"`
	QuoteTokenInfo     json.RawMessage `json:"quote_token_info"     gorm:"column:quote_token_info;type:jsonb"`
	CreatedAt          time.Time       `json:"created_at"           gorm:"column:created_at"`
	UpdatedAt          *time.Time      `json:"updated_at"           gorm:"column:updated_at"`
}

// Token represents token info
type Token struct {
	ID        string    `json:"id" db:"id"`
	Network   Network   `json:"network" db:"network"`
	Address   string    `json:"address" db:"address"`
	Symbol    string    `json:"symbol" db:"symbol"`
	Name      string    `json:"name" db:"name"`
	Decimals  int       `json:"decimals" db:"decimals"`
	LogoURI   string    `json:"logo_uri" db:"logo_uri"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// TradeStats represents aggregated trading stats
type TradeStats struct {
	Network        Network         `json:"network"`
	PairID         string          `json:"pair_id"`
	Price          decimal.Decimal `json:"price"`
	PriceChange24h decimal.Decimal `json:"price_change_24h"`
	PriceHigh24h   decimal.Decimal `json:"price_high_24h"`
	PriceLow24h    decimal.Decimal `json:"price_low_24h"`
	Volume24h      decimal.Decimal `json:"volume_24h"`
	Liquidity      decimal.Decimal `json:"liquidity"`
	Trades24h      int             `json:"trades_24h"`
	LastTradeAt    *time.Time      `json:"last_trade_at"`
}

type Candle struct {
	PairID     string          `json:"pair_id"    gorm:"primaryKey;index"`
	Time       int64           `json:"time"       gorm:"primaryKey;index"`
	Resolution int             `json:"resolution" gorm:"primaryKey"`
	// Currency distinguishes USD-denominated vs quote-token-denominated candles.
	// 'usd'   = open/high/low/close are in USD  (from currency=usd GeckoTerminal endpoint)
	// 'token' = open/high/low/close are in the quote token (e.g. WBNB)
	Currency   string          `json:"currency"   gorm:"primaryKey;default:usd"`
	Open       decimal.Decimal `json:"open"       gorm:"type:numeric(40,20)"`
	High       decimal.Decimal `json:"high"       gorm:"type:numeric(40,20)"`
	Low        decimal.Decimal `json:"low"        gorm:"type:numeric(40,20)"`
	Close      decimal.Decimal `json:"close"      gorm:"type:numeric(40,20)"`
	Volume     decimal.Decimal `json:"volume"     gorm:"type:numeric(40,20)"`
	Source     string          `json:"source"     gorm:"default:gecko"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// API Response types
type CreateOrderRequest struct {
	Network      Network         `json:"network" binding:"required"`
	PairID       string          `json:"pair_id" binding:"required"`
	Side         OrderSide       `json:"side" binding:"required,oneof=buy sell"`
	OrderType    OrderType       `json:"order_type" binding:"required,oneof=limit market stop_loss take_profit post_only"`
	Price        decimal.Decimal `json:"price" binding:"required"`
	Amount       decimal.Decimal `json:"amount" binding:"required"` // Base token amount
	AmountIn     decimal.Decimal `json:"amount_in"`                 // TokenIn amount (quote for buy, base for sell)
	TokenIn      string          `json:"token_in"`
	TokenOut     string          `json:"token_out"`
	Receiver     string          `json:"receiver"`
	Nonce        *uint64         `json:"nonce"`
	Salt         *uint64         `json:"salt"`
	Expiration   time.Time       `json:"expiration" binding:"required"`
	IsLadder     bool            `json:"is_ladder"`
	LadderConfig *LadderConfig   `json:"ladder_config,omitempty"`
	TriggerPrice decimal.Decimal `json:"trigger_price,omitempty"`
	ReduceOnly   bool            `json:"reduce_only"`
	TimeInForce  string          `json:"time_in_force"`
	// Signed order fields
	Maker        string          `json:"maker"`
	Signature    string          `json:"signature"`
	OrderHash    string          `json:"order_hash"`
	AmountOutMin decimal.Decimal `json:"amount_out_min"`
	// Solana-only custody deposit fields.
	// Use pointers so JSON null → nil → NULL in DB (avoids UNIQUE constraint on empty string).
	DepositMemo      *string         `json:"deposit_memo,omitempty"`
	DepositAmount    decimal.Decimal `json:"deposit_amount,omitempty"`
	DepositTokenMint *string         `json:"deposit_token_mint,omitempty"`
	DepositType      *string         `json:"deposit_type,omitempty"`
	DepositTxHash    *string         `json:"deposit_tx_hash,omitempty"`
	AmountInDecimals  int `json:"amount_in_decimals"`
	AmountOutDecimals int `json:"amount_out_decimals"`
}

type LadderConfig struct {
	Levels     int             `json:"levels" binding:"required"`
	PriceStart decimal.Decimal `json:"price_start" binding:"required"`
	PriceEnd   decimal.Decimal `json:"price_end" binding:"required"`
}

type OrderResponse struct {
	Order      *Order `json:"order"`
	CommitHash string `json:"commit_hash,omitempty"`
	Secret     string `json:"secret,omitempty"`
}

// OrderDTO is a simplified version of Order for API responses (values as strings)
type OrderDTO struct {
	ID                      uint   `json:"id"`
	OrderHash               string `json:"order_hash"`
	UserID                  uint   `json:"user_id"`
	Network                 string `json:"network"`
	PairID                  string `json:"pair_id"`
	Side                    string `json:"side"`
	OrderType               string `json:"order_type"`
	Price                   string `json:"price"`
	Amount                  string `json:"amount"`
	FilledAmount            string `json:"filled_amount"`
	AmountIn                string `json:"amount_in"`
	AmountOutMin            string `json:"amount_out_min"`
	DepositMemo             string `json:"deposit_memo,omitempty"`
	DepositAmount           string `json:"deposit_amount,omitempty"`
	DepositTokenMint        string `json:"deposit_token_mint,omitempty"`
	DepositType             string `json:"deposit_type,omitempty"`
	DepositTxHash           string `json:"deposit_tx_hash,omitempty"`
	TokenIn                 string `json:"token_in"`
	TokenOut                string `json:"token_out"`
	TokenInDecimals         int    `json:"token_in_decimals"`
	TokenOutDecimals        int    `json:"token_out_decimals"`
	Maker                   string `json:"maker"`
	Nonce                   uint64 `json:"nonce"`
	Salt                    uint64 `json:"salt"`
	Status                  string `json:"status"`
	IsLadder                bool   `json:"is_ladder"`
	LadderLevels            *int   `json:"ladder_levels,omitempty"`
	LadderPriceStart        string `json:"ladder_price_start,omitempty"`
	LadderPriceEnd          string `json:"ladder_price_end,omitempty"`
	LadderParentID          *uint  `json:"ladder_parent_id,omitempty"`
	LadderTotalAmountIn     string `json:"ladder_total_amount_in,omitempty"`
	LadderTotalAmountOutMin string `json:"ladder_total_amount_out_min,omitempty"`
	TriggerPrice            string `json:"trigger_price,omitempty"`
	IsPostOnly              bool   `json:"is_post_only"`
	ReduceOnly              bool   `json:"reduce_only"`
	TimeInForce             string `json:"time_in_force"`
	StopLossType            string `json:"stop_loss_type,omitempty"`
	Expiration              string `json:"expiration"`
	CreatedAt               string `json:"created_at"`
	UpdatedAt               string `json:"updated_at"`
}

// OrderWithPair includes order data plus pair information for display
type OrderWithPair struct {
	Order             OrderDTO   `json:"order"`
	Pair              *PairInfo  `json:"pair,omitempty"`
	TokenInInfo       *TokenInfo `json:"token_in_info,omitempty"`
	TokenOutInfo      *TokenInfo `json:"token_out_info,omitempty"`
	AmountInHuman     string     `json:"amount_in_human"`
	AmountOutMinHuman string     `json:"amount_out_min_human"`
}

type PairInfo struct {
	ID          string `json:"id"`
	BaseSymbol  string `json:"base_symbol"`
	QuoteSymbol string `json:"quote_symbol"`
	BaseLogo    string `json:"base_logo,omitempty"`
	QuoteLogo   string `json:"quote_logo,omitempty"`
}

type TokenInfo struct {
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
}

type OrderBookResponse struct {
	PairID        string       `json:"pair_id"`
	Asks          []OrderLevel `json:"asks"`
	Bids          []OrderLevel `json:"bids"`
	Sequence      int64        `json:"sequence"`
	MidPrice      float64      `json:"mid_price"`
	Spread        float64      `json:"spread"`
	SpreadPercent float64      `json:"spread_percent"`
}

type OrderLevel struct {
	Price  decimal.Decimal `json:"price"`
	Amount decimal.Decimal `json:"amount"`
	Total  decimal.Decimal `json:"total"`
	Orders int             `json:"orders"`
}

type RecentTrade struct {
	ID       uint            `json:"id"`
	Price    decimal.Decimal `json:"price"`
	Amount   decimal.Decimal `json:"amount"`
	Side     OrderSide       `json:"side"`
	Time     time.Time       `json:"time"`
	TxHash   string          `json:"tx_hash"`
	Decimals int             `json:"decimals"`
}

type Ticker struct {
	PairID         string          `json:"pair_id"`
	LastPrice      decimal.Decimal `json:"last_price"`
	PriceChange24h decimal.Decimal `json:"price_change_24h"`
	PriceChangePct decimal.Decimal `json:"price_change_pct"`
	High24h        decimal.Decimal `json:"high_24h"`
	Low24h         decimal.Decimal `json:"low_24h"`
	Volume24h      decimal.Decimal `json:"volume_24h"`
	Trades24h      int             `json:"trades_24h"`
}
