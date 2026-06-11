-- Migration: Add performance indexes for slow queries
-- Adds composite indexes to improve query performance for orderbook and stats

-- Index for orderbook queries: orders(pair_id, network, side, status, price DESC)
CREATE INDEX IF NOT EXISTS idx_orders_orderbook ON orders(pair_id, network, side, status, price DESC);

-- Index for fills stats queries: fills(pair_id, created_at DESC, status)
CREATE INDEX IF NOT EXISTS idx_fills_stats ON fills(pair_id, created_at DESC, status);

-- Index for recent trades: fills(pair_id, network, status, created_at DESC)
CREATE INDEX IF NOT EXISTS idx_fills_recent_trades ON fills(pair_id, network, status, created_at DESC);

-- Index for user orders queries: orders(user_id, status, created_at DESC)
CREATE INDEX IF NOT EXISTS idx_orders_user_status_created ON orders(user_id, status, created_at DESC);

-- Index for user history orders: orders(user_id, created_at DESC)
CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders(user_id, created_at DESC);