-- Add separate transaction hash columns for Solana fills
ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS tx_hash_buy VARCHAR(100),
  ADD COLUMN IF NOT EXISTS tx_hash_sell VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_fills_tx_hash_buy ON fills(tx_hash_buy);
CREATE INDEX IF NOT EXISTS idx_fills_tx_hash_sell ON fills(tx_hash_sell);
