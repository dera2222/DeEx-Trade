-- Migration 009: Add last_trade_price to pairs table
-- This column is written by the Go backend every time a fill is created.
-- The UI uses it to show the real traded price (your exchange) instead of
-- the GeckoTerminal reference price when recent trades exist.

ALTER TABLE pairs
  ADD COLUMN IF NOT EXISTS last_trade_price DECIMAL(40, 20) DEFAULT NULL;

-- Index so we can quickly find pairs with recent trades
CREATE INDEX IF NOT EXISTS idx_pairs_last_trade_at ON pairs(last_trade_at DESC NULLS LAST)
  WHERE last_trade_at IS NOT NULL;

COMMENT ON COLUMN pairs.last_trade_price IS
  'Price of the most recent fill on this DEX. Overrides gecko price in UI when < 5 minutes old.';
COMMENT ON COLUMN pairs.last_trade_at IS
  'Timestamp of the most recent fill. Used together with last_trade_price.';
