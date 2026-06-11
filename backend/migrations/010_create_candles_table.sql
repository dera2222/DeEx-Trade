-- Migration 010: Create candles table (or add missing columns if it already exists)
-- The table may already exist from GORM AutoMigrate — this migration is safe to re-run.

CREATE TABLE IF NOT EXISTS candles (
  pair_id     VARCHAR(100) NOT NULL REFERENCES pairs(id) ON DELETE CASCADE,
  resolution  INTEGER      NOT NULL,
  time        BIGINT       NOT NULL,
  open        DECIMAL(40, 20) NOT NULL,
  high        DECIMAL(40, 20) NOT NULL,
  low         DECIMAL(40, 20) NOT NULL,
  close       DECIMAL(40, 20) NOT NULL,
  volume      DECIMAL(40, 20) NOT NULL DEFAULT 0,
  updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

  PRIMARY KEY (pair_id, resolution, time)
);

-- Add source column if missing (table may have been created by GORM without it)
ALTER TABLE candles
  ADD COLUMN IF NOT EXISTS source VARCHAR(20) NOT NULL DEFAULT 'gecko';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_candles_pair_res_time
  ON candles (pair_id, resolution, time DESC);
