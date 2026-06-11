-- Migration 011: Add currency column to candles table
-- Allows storing both USD-denominated and quote-token-denominated candles
-- for the same pair+resolution+time bucket.

-- Step 1: Drop the old primary key
ALTER TABLE candles DROP CONSTRAINT IF EXISTS candles_pkey;

-- Step 2: Add the currency column (default 'usd' so existing rows stay valid)
ALTER TABLE candles
  ADD COLUMN IF NOT EXISTS currency VARCHAR(10) NOT NULL DEFAULT 'usd';

-- Step 3: Re-create primary key including currency
ALTER TABLE candles
  ADD PRIMARY KEY (pair_id, resolution, time, currency);

-- Step 4: Update the index to include currency
DROP INDEX IF EXISTS idx_candles_pair_res_time;
CREATE INDEX IF NOT EXISTS idx_candles_pair_res_time_cur
  ON candles (pair_id, resolution, currency, time DESC);
