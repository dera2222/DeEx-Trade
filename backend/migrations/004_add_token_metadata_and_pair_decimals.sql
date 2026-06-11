-- Migration: Add token metadata and pair token decimals
-- This migration adds:
-- 1. Base and quote token decimals to the pairs table
-- 2. Extended metadata fields to the tokens table for better token information

-- Create tokens table if it doesn't exist
CREATE TABLE IF NOT EXISTS tokens (
    address VARCHAR(100) NOT NULL,
    network VARCHAR(50) NOT NULL,
    name TEXT,
    symbol VARCHAR(50),
    decimals INTEGER,
    description TEXT,
    image_thumb TEXT,
    image_small TEXT,
    image_large TEXT,
    websites TEXT,
    twitter_handle VARCHAR(100),
    telegram_handle VARCHAR(100),
    discord_url TEXT,
    categories TEXT,
    gt_score NUMERIC(10, 6),
    gt_verified BOOLEAN DEFAULT FALSE,
    coingecko_id VARCHAR(100),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (address, network)
);

-- Add base_token_decimals and quote_token_decimals to pairs table
ALTER TABLE IF EXISTS pairs
ADD COLUMN IF NOT EXISTS base_token_decimals INTEGER DEFAULT 18,
ADD COLUMN IF NOT EXISTS quote_token_decimals INTEGER DEFAULT 18;

-- Extend tokens table with metadata fields
ALTER TABLE IF EXISTS tokens
ADD COLUMN IF NOT EXISTS description TEXT,
ADD COLUMN IF NOT EXISTS image_thumb TEXT,
ADD COLUMN IF NOT EXISTS image_small TEXT,
ADD COLUMN IF NOT EXISTS image_large TEXT,
ADD COLUMN IF NOT EXISTS websites TEXT,
ADD COLUMN IF NOT EXISTS twitter_handle VARCHAR(100),
ADD COLUMN IF NOT EXISTS telegram_handle VARCHAR(100),
ADD COLUMN IF NOT EXISTS discord_url TEXT,
ADD COLUMN IF NOT EXISTS categories TEXT,
ADD COLUMN IF NOT EXISTS gt_score NUMERIC(10, 6),
ADD COLUMN IF NOT EXISTS gt_verified BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS coingecko_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();

-- Add unique constraint on address if it doesn't exist
-- This constraint is needed for the upsert operation
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint 
    WHERE conname = 'tokens_address_network_key' 
    AND contype = 'u'
  ) THEN
    ALTER TABLE tokens ADD CONSTRAINT tokens_address_network_key UNIQUE(address, network);
  END IF;
END $$;

-- Create index on address for faster lookups
CREATE INDEX IF NOT EXISTS idx_tokens_address_network ON tokens(address, network);

-- Create index on coingecko_id for Coingecko lookups
CREATE INDEX IF NOT EXISTS idx_tokens_coingecko_id ON tokens(coingecko_id);
