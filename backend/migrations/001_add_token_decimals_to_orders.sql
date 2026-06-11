-- Migration: Add token decimal fields to orders table
-- This adds support for tokens with non-18 decimals (e.g., CREPE with 9 decimals)

-- Add amount_in_decimals column (defaults to 18 for backward compatibility)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount_in_decimals INTEGER DEFAULT 18;

-- Add amount_out_decimals column (defaults to 18 for backward compatibility)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount_out_decimals INTEGER DEFAULT 18;

-- Add comment for documentation
COMMENT ON COLUMN orders.amount_in_decimals IS 'Decimals of the token_in (used for proper amount interpretation)';
COMMENT ON COLUMN orders.amount_out_decimals IS 'Decimals of the token_out (used for proper amount interpretation)';
