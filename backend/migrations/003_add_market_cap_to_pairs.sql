-- Add market cap columns to pairs table
ALTER TABLE pairs
ADD COLUMN IF NOT EXISTS market_cap BIGINT DEFAULT 0,
ADD COLUMN IF NOT EXISTS market_cap_usd NUMERIC(40, 2) DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_pairs_market_cap ON pairs(market_cap);
