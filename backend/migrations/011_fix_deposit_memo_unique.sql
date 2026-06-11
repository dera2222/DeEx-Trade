-- Migration 011: Fix deposit_memo UNIQUE constraint for EVM orders
--
-- Problem: The original deposit_memo column has a blanket UNIQUE constraint.
-- EVM orders (BSC/Base) send deposit_memo = null/empty since they don't use
-- the Solana custody deposit model. When stored as an empty string '' by Go's
-- zero-value JSON unmarshaling, the second EVM order fails with:
--   ERROR: duplicate key value violates unique constraint "orders_deposit_memo_key"
--
-- Fix: Drop the blanket UNIQUE constraint and replace it with a partial unique
-- index that only enforces uniqueness when deposit_memo is a real non-empty value.
-- This allows unlimited EVM orders (memo = NULL or '') while still preventing
-- duplicate Solana deposit memos.

-- 1. Drop the existing blanket unique constraint
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_deposit_memo_key;

-- 2. Drop any existing full unique index on deposit_memo
DROP INDEX IF EXISTS orders_deposit_memo_key;
DROP INDEX IF EXISTS idx_orders_deposit_memo_unique;

-- 3. Create a partial unique index — only unique when memo is a real value
--    NULL and empty string ('') are excluded, so EVM orders can have many rows
--    with no memo without conflicting.
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_deposit_memo_nonempty
    ON orders (deposit_memo)
    WHERE deposit_memo IS NOT NULL AND deposit_memo != '';

-- 4. Also index for fast Solana deposit lookups (non-unique, covers all rows)
CREATE INDEX IF NOT EXISTS idx_orders_deposit_memo
    ON orders (deposit_memo)
    WHERE deposit_memo IS NOT NULL AND deposit_memo != '';
