-- Migration: Add token metadata JSON fields to pairs table
-- This migration adds JSONB columns to store complete token metadata for base and quote tokens

-- Add base_token_info and quote_token_info JSONB columns to pairs table
ALTER TABLE IF EXISTS pairs
ADD COLUMN IF NOT EXISTS base_token_info JSONB,
ADD COLUMN IF NOT EXISTS quote_token_info JSONB;

-- Create index on the JSON columns for better query performance
CREATE INDEX IF NOT EXISTS idx_pairs_base_token_info ON pairs USING GIN(base_token_info);
CREATE INDEX IF NOT EXISTS idx_pairs_quote_token_info ON pairs USING GIN(quote_token_info);
