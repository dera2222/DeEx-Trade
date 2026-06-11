-- Migration 008: Add price & market data columns to pairs table
-- Run this in Supabase SQL Editor before deploying the price-worker

ALTER TABLE pairs
  ADD COLUMN IF NOT EXISTS price           DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS price_usd       DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS price_change_24h DECIMAL(20, 6)  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS price_high_24h  DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS price_low_24h   DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS volume_24h      DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS volume_24h_usd  DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS liquidity       DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS liquidity_usd   DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS market_cap      DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS market_cap_usd  DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS trending_score  DECIMAL(10, 4)  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_trade_at   TIMESTAMP WITH TIME ZONE,
  ADD COLUMN IF NOT EXISTS updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  -- Extra metadata columns populated by the server/pair-discovery worker
  ADD COLUMN IF NOT EXISTS pool_address    VARCHAR(100),
  ADD COLUMN IF NOT EXISTS dex_name        VARCHAR(100),
  ADD COLUMN IF NOT EXISTS base_token_decimals  INTEGER DEFAULT 18,
  ADD COLUMN IF NOT EXISTS quote_token_decimals INTEGER DEFAULT 18,
  ADD COLUMN IF NOT EXISTS base_token_info  JSONB,
  ADD COLUMN IF NOT EXISTS quote_token_info JSONB;

-- Index for fast trending/sorted queries
CREATE INDEX IF NOT EXISTS idx_pairs_trending_score ON pairs(trending_score DESC);
CREATE INDEX IF NOT EXISTS idx_pairs_price          ON pairs(price DESC)         WHERE price > 0;
CREATE INDEX IF NOT EXISTS idx_pairs_updated_at     ON pairs(updated_at DESC);

-- Comment
COMMENT ON COLUMN pairs.price          IS 'Native price (base token in quote token units), set by price-worker';
COMMENT ON COLUMN pairs.price_usd      IS 'USD price of base token, set by price-worker from GeckoTerminal';
COMMENT ON COLUMN pairs.price_change_24h IS '24h % price change, set by price-worker';
COMMENT ON COLUMN pairs.trending_score  IS 'Computed trending score, set by price-worker';
