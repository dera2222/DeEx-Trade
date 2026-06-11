-- ====================================================================
-- File: server/supabase-schema.sql
-- ====================================================================

-- Supabase Database Schema for CexDex Pairs

-- Create pairs table (simplified)
create table if not exists pairs (
  id text primary key,
  network text not null,
  pair_address text not null,
  dex_name text,
  dex text,
  base_token jsonb,
  quote_token jsonb,
  base_symbol text,
  quote_symbol text,
  pool_name text,
  pool_address text,
  created_at timestamp with time zone,
  indexed_at timestamp with time zone,
  updated_at timestamp with time zone default now()
);

-- Create indexes
create index IF NOT EXISTS pairs_network_idx on pairs(network);
create index IF NOT EXISTS pairs_created_at_idx on pairs(created_at desc);

-- Enable Row Level Security
alter table pairs enable row level security;

-- Policy for public read
create policy "Enable read access for all users" on pairs
  for select using (true);

-- Policy for write (service role)
create policy "Enable write access for service role" on pairs
  for all using (true);

-- Auto-update updated_at
create or replace function update_updated_at_column()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

create trigger update_pairs_updated_at
  before update on pairs
  for each row
  execute function update_updated_at_column();


-- ====================================================================
-- File: backend/schema.sql
-- ====================================================================

-- Database Schema for CexDex Backend (Go + GORM)
-- Run this in Supabase SQL Editor

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    address VARCHAR(66) UNIQUE NOT NULL,
    email VARCHAR(255),
    username VARCHAR(100),
    referral_code VARCHAR(50) UNIQUE,
    referred_by VARCHAR(66),
    network VARCHAR(20) NOT NULL DEFAULT 'bsc',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_address ON users(address);
CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code);

-- Pairs table
CREATE TABLE IF NOT EXISTS pairs (
    id VARCHAR(100) PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    base_token VARCHAR(66) NOT NULL,
    quote_token VARCHAR(66) NOT NULL,
    base_symbol VARCHAR(20),
    quote_symbol VARCHAR(20),
    dex VARCHAR(50),
    pool_address VARCHAR(66),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pairs_network ON pairs(network);
CREATE INDEX IF NOT EXISTS idx_pairs_base_token ON pairs(base_token);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_hash VARCHAR(100) UNIQUE,
    user_id INTEGER REFERENCES users(id),
    network VARCHAR(20) NOT NULL,
    pair_id VARCHAR(100) REFERENCES pairs(id),
    side VARCHAR(10) NOT NULL,
    order_type VARCHAR(20) NOT NULL DEFAULT 'limit',
    price DECIMAL(40, 20) NOT NULL,
    amount DECIMAL(40, 20) NOT NULL,
    filled_amount DECIMAL(40, 20) DEFAULT 0,
    amount_in DECIMAL(40, 20),
    amount_out_min DECIMAL(40, 20),
    token_in VARCHAR(66),
    token_out VARCHAR(66),
    receiver VARCHAR(66),
    maker VARCHAR(66),
    signature TEXT,
    deposit_memo VARCHAR(255) UNIQUE,
    deposit_amount DECIMAL(40, 20),
    deposit_token_mint VARCHAR(100),
    deposit_type VARCHAR(20),
    deposit_tx_hash VARCHAR(128),
    expiration TIMESTAMP WITH TIME ZONE NOT NULL,
    nonce BIGINT,
    salt BIGINT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    is_ladder BOOLEAN DEFAULT FALSE,
    ladder_levels INTEGER,
    ladder_price_start DECIMAL(40, 20),
    ladder_price_end DECIMAL(40, 20),
    ladder_parent_id INTEGER REFERENCES orders(id),
    commit_hash VARCHAR(100),
    commit_revealed BOOLEAN DEFAULT FALSE,
    commit_expired BOOLEAN DEFAULT FALSE,
    trigger_price DECIMAL(40, 20),
    triggered_at TIMESTAMP WITH TIME ZONE,
    is_post_only BOOLEAN DEFAULT FALSE,
    reduce_only BOOLEAN DEFAULT FALSE,
    time_in_force VARCHAR(10) DEFAULT 'GTC',
    stop_loss_type VARCHAR(20),
    amount_in_decimals INTEGER DEFAULT 18,
    amount_out_decimals INTEGER DEFAULT 18,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_pair_id ON orders(pair_id);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_network ON orders(network);
CREATE INDEX IF NOT EXISTS idx_orders_order_type ON orders(order_type);
CREATE INDEX IF NOT EXISTS idx_orders_trigger_price ON orders(trigger_price) WHERE order_type IN ('stop_loss', 'take_profit');

-- Fills table
CREATE TABLE IF NOT EXISTS fills (
    id SERIAL PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    pair_id VARCHAR(100) REFERENCES pairs(id),
    order_id INTEGER REFERENCES orders(id),
    maker_order_id INTEGER,
    maker VARCHAR(66) NOT NULL,
    taker VARCHAR(66) NOT NULL,
    side VARCHAR(10) NOT NULL,
    price DECIMAL(40, 20) NOT NULL,
    amount DECIMAL(40, 20) NOT NULL,
    amount_in DECIMAL(40, 20),
    amount_out DECIMAL(40, 20),
    fee DECIMAL(40, 20),
    token_in VARCHAR(66),
    token_out VARCHAR(66),
    tx_hash VARCHAR(100),
    tx_hash_buy VARCHAR(100),
    tx_hash_sell VARCHAR(100),
    block_number BIGINT,
    gas_used BIGINT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fills_pair_id ON fills(pair_id);
CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_fills_network ON fills(network);
CREATE INDEX IF NOT EXISTS idx_fills_created_at ON fills(created_at);

-- Tokens table
CREATE TABLE IF NOT EXISTS tokens (
    id VARCHAR(100) PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    address VARCHAR(66) UNIQUE NOT NULL,
    symbol VARCHAR(20),
    name VARCHAR(100),
    decimals INTEGER,
    logo_uri TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tokens_network ON tokens(network);
CREATE INDEX IF NOT EXISTS idx_tokens_address ON tokens(address);

-- Solana deposits table for memo-based custody deposit tracking
CREATE TABLE IF NOT EXISTS solana_deposits (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    user_address VARCHAR(66) NOT NULL,
    network VARCHAR(20) NOT NULL,
    token_mint VARCHAR(100) NOT NULL,
    amount DECIMAL(40, 20) NOT NULL,
    memo VARCHAR(255) UNIQUE NOT NULL,
    tx_hash VARCHAR(128) NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_solana_deposits_user_id ON solana_deposits(user_id);
CREATE INDEX IF NOT EXISTS idx_solana_deposits_tx_hash ON solana_deposits(tx_hash);

-- Enable Row Level Security (optional - can disable for easier dev)
-- ALTER TABLE users ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE fills ENABLE ROW LEVEL SECURITY;

-- Policy for public read (if RLS enabled)
-- CREATE POLICY "Public read access" ON users FOR SELECT USING (true);
-- CREATE POLICY "Public read access" ON orders FOR SELECT USING (true);
-- CREATE POLICY "Public read access" ON fills FOR SELECT USING (true);


-- ====================================================================
-- File: backend/schema-safe.sql
-- ====================================================================

-- Safe database schema for CexDex Backend
-- Run this in Supabase SQL Editor

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    address VARCHAR(66) UNIQUE NOT NULL,
    email VARCHAR(255),
    username VARCHAR(100),
    referral_code VARCHAR(50) UNIQUE,
    referred_by VARCHAR(66),
    network VARCHAR(20) NOT NULL DEFAULT 'bsc',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_address ON users(address);
CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code);

-- Pairs table
CREATE TABLE IF NOT EXISTS pairs (
    id VARCHAR(100) PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    base_token VARCHAR(66) NOT NULL,
    quote_token VARCHAR(66) NOT NULL,
    base_symbol VARCHAR(20),
    quote_symbol VARCHAR(20),
    dex VARCHAR(50),
    pool_address VARCHAR(66),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pairs_network ON pairs(network);
CREATE INDEX IF NOT EXISTS idx_pairs_base_token ON pairs(base_token);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_hash VARCHAR(100) UNIQUE,
    user_id INTEGER REFERENCES users(id),
    network VARCHAR(20) NOT NULL,
    pair_id VARCHAR(100) REFERENCES pairs(id),
    side VARCHAR(10) NOT NULL,
    order_type VARCHAR(20) NOT NULL DEFAULT 'limit',
    price DECIMAL(40, 20) NOT NULL,
    amount DECIMAL(40, 20) NOT NULL,
    filled_amount DECIMAL(40, 20) DEFAULT 0,
    amount_in DECIMAL(40, 20),
    amount_out_min DECIMAL(40, 20),
    token_in VARCHAR(66),
    token_out VARCHAR(66),
    receiver VARCHAR(66),
    maker VARCHAR(66),
    signature TEXT,
    deposit_memo VARCHAR(255) UNIQUE,
    deposit_amount DECIMAL(40, 20),
    deposit_token_mint VARCHAR(100),
    deposit_type VARCHAR(20),
    deposit_tx_hash VARCHAR(128),
    expiration TIMESTAMP WITH TIME ZONE NOT NULL,
    nonce BIGINT,
    salt BIGINT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    is_ladder BOOLEAN DEFAULT FALSE,
    ladder_levels INTEGER,
    ladder_price_start DECIMAL(40, 20),
    ladder_price_end DECIMAL(40, 20),
    ladder_parent_id INTEGER REFERENCES orders(id),
    commit_hash VARCHAR(100),
    commit_revealed BOOLEAN DEFAULT FALSE,
    commit_expired BOOLEAN DEFAULT FALSE,
    trigger_price DECIMAL(40, 20),
    triggered_at TIMESTAMP WITH TIME ZONE,
    is_post_only BOOLEAN DEFAULT FALSE,
    reduce_only BOOLEAN DEFAULT FALSE,
    time_in_force VARCHAR(10) DEFAULT 'GTC',
    stop_loss_type VARCHAR(20),
    amount_in_decimals INTEGER DEFAULT 18,
    amount_out_decimals INTEGER DEFAULT 18,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_pair_id ON orders(pair_id);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_network ON orders(network);
CREATE INDEX IF NOT EXISTS idx_orders_order_type ON orders(order_type);
CREATE INDEX IF NOT EXISTS idx_orders_deposit_memo ON orders(deposit_memo);
CREATE INDEX IF NOT EXISTS idx_orders_deposit_tx_hash ON orders(deposit_tx_hash);
CREATE INDEX IF NOT EXISTS idx_orders_trigger_price ON orders(trigger_price) WHERE order_type IN ('stop_loss', 'take_profit');

-- Fills table
CREATE TABLE IF NOT EXISTS fills (
    id SERIAL PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    pair_id VARCHAR(100) REFERENCES pairs(id),
    order_id INTEGER REFERENCES orders(id),
    maker_order_id INTEGER,
    maker VARCHAR(66) NOT NULL,
    taker VARCHAR(66) NOT NULL,
    side VARCHAR(10) NOT NULL,
    price DECIMAL(40, 20) NOT NULL,
    amount DECIMAL(40, 20) NOT NULL,
    amount_in DECIMAL(40, 20),
    amount_out DECIMAL(40, 20),
    fee DECIMAL(40, 20),
    token_in VARCHAR(66),
    token_out VARCHAR(66),
    tx_hash VARCHAR(100),
    tx_hash_buy VARCHAR(100),
    tx_hash_sell VARCHAR(100),
    block_number BIGINT,
    gas_used BIGINT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fills_pair_id ON fills(pair_id);
CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_fills_network ON fills(network);
CREATE INDEX IF NOT EXISTS idx_fills_created_at ON fills(created_at);

-- Tokens table
CREATE TABLE IF NOT EXISTS tokens (
    id VARCHAR(100) PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    address VARCHAR(66) UNIQUE NOT NULL,
    symbol VARCHAR(20),
    name VARCHAR(100),
    decimals INTEGER,
    logo_uri TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tokens_network ON tokens(network);
CREATE INDEX IF NOT EXISTS idx_tokens_address ON tokens(address);

-- Solana deposits table for memo-based custody deposit tracking
CREATE TABLE IF NOT EXISTS solana_deposits (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    user_address VARCHAR(66) NOT NULL,
    network VARCHAR(20) NOT NULL,
    token_mint VARCHAR(100) NOT NULL,
    amount DECIMAL(40, 20) NOT NULL,
    memo VARCHAR(255) UNIQUE NOT NULL,
    tx_hash VARCHAR(128) NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_solana_deposits_user_id ON solana_deposits(user_id);
CREATE INDEX IF NOT EXISTS idx_solana_deposits_tx_hash ON solana_deposits(tx_hash);

-- Candles table for persistent OHLCV data
CREATE TABLE IF NOT EXISTS candles (
    pair_id VARCHAR(100) NOT NULL,
    time BIGINT NOT NULL,
    resolution INTEGER NOT NULL,
    open DECIMAL(40, 20) NOT NULL,
    high DECIMAL(40, 20) NOT NULL,
    low DECIMAL(40, 20) NOT NULL,
    close DECIMAL(40, 20) NOT NULL,
    volume DECIMAL(40, 20) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (pair_id, time, resolution)
);

CREATE INDEX IF NOT EXISTS idx_candles_pair_time ON candles(pair_id, time DESC);


-- ====================================================================
-- File: src/backend/schema.sql
-- ====================================================================

-- Database Schema for CexDex Backend (Go + GORM)
-- Run this in Supabase SQL Editor

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    address VARCHAR(66) UNIQUE NOT NULL,
    email VARCHAR(255),
    username VARCHAR(100),
    referral_code VARCHAR(50) UNIQUE,
    referred_by VARCHAR(66),
    network VARCHAR(20) NOT NULL DEFAULT 'bsc',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_address ON users(address);
CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code);

-- Pairs table
CREATE TABLE IF NOT EXISTS pairs (
    id VARCHAR(100) PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    base_token VARCHAR(66) NOT NULL,
    quote_token VARCHAR(66) NOT NULL,
    base_symbol VARCHAR(20),
    quote_symbol VARCHAR(20),
    dex VARCHAR(50),
    pool_address VARCHAR(66),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pairs_network ON pairs(network);
CREATE INDEX IF NOT EXISTS idx_pairs_base_token ON pairs(base_token);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_hash VARCHAR(100) UNIQUE,
    user_id INTEGER REFERENCES users(id),
    network VARCHAR(20) NOT NULL,
    pair_id VARCHAR(100) REFERENCES pairs(id),
    side VARCHAR(10) NOT NULL,
    order_type VARCHAR(20) NOT NULL DEFAULT 'limit',
    price DECIMAL(40, 20) NOT NULL,
    amount DECIMAL(40, 20) NOT NULL,
    filled_amount DECIMAL(40, 20) DEFAULT 0,
    amount_in DECIMAL(40, 20),
    amount_out_min DECIMAL(40, 20),
    token_in VARCHAR(66),
    token_out VARCHAR(66),
    receiver VARCHAR(66),
    maker VARCHAR(66),
    signature TEXT,
    deposit_memo VARCHAR(255) UNIQUE,
    deposit_amount DECIMAL(40, 20),
    deposit_token_mint VARCHAR(100),
    deposit_type VARCHAR(20),
    deposit_tx_hash VARCHAR(128),
    expiration TIMESTAMP WITH TIME ZONE NOT NULL,
    nonce BIGINT,
    salt BIGINT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    is_ladder BOOLEAN DEFAULT FALSE,
    ladder_levels INTEGER,
    ladder_price_start DECIMAL(40, 20),
    ladder_price_end DECIMAL(40, 20),
    ladder_parent_id INTEGER REFERENCES orders(id),
    commit_hash VARCHAR(100),
    commit_revealed BOOLEAN DEFAULT FALSE,
    commit_expired BOOLEAN DEFAULT FALSE,
    trigger_price DECIMAL(40, 20),
    triggered_at TIMESTAMP WITH TIME ZONE,
    is_post_only BOOLEAN DEFAULT FALSE,
    reduce_only BOOLEAN DEFAULT FALSE,
    time_in_force VARCHAR(10) DEFAULT 'GTC',
    stop_loss_type VARCHAR(20),
    amount_in_decimals INTEGER DEFAULT 18,
    amount_out_decimals INTEGER DEFAULT 18,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_pair_id ON orders(pair_id);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_network ON orders(network);
CREATE INDEX IF NOT EXISTS idx_orders_order_type ON orders(order_type);
CREATE INDEX IF NOT EXISTS idx_orders_deposit_memo ON orders(deposit_memo);
CREATE INDEX IF NOT EXISTS idx_orders_deposit_tx_hash ON orders(deposit_tx_hash);
CREATE INDEX IF NOT EXISTS idx_orders_trigger_price ON orders(trigger_price) WHERE order_type IN ('stop_loss', 'take_profit');

-- Fills table
CREATE TABLE IF NOT EXISTS fills (
    id SERIAL PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    pair_id VARCHAR(100) REFERENCES pairs(id),
    order_id INTEGER REFERENCES orders(id),
    maker_order_id INTEGER,
    maker VARCHAR(66) NOT NULL,
    taker VARCHAR(66) NOT NULL,
    side VARCHAR(10) NOT NULL,
    price DECIMAL(40, 20) NOT NULL,
    amount DECIMAL(40, 20) NOT NULL,
    amount_in DECIMAL(40, 20),
    amount_out DECIMAL(40, 20),
    fee DECIMAL(40, 20),
    token_in VARCHAR(66),
    token_out VARCHAR(66),
    tx_hash VARCHAR(100),
    tx_hash_buy VARCHAR(100),
    tx_hash_sell VARCHAR(100),
    block_number BIGINT,
    gas_used BIGINT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fills_pair_id ON fills(pair_id);
CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_fills_network ON fills(network);
CREATE INDEX IF NOT EXISTS idx_fills_created_at ON fills(created_at);

-- Tokens table
CREATE TABLE IF NOT EXISTS tokens (
    id VARCHAR(100) PRIMARY KEY,
    network VARCHAR(20) NOT NULL,
    address VARCHAR(66) UNIQUE NOT NULL,
    symbol VARCHAR(20),
    name VARCHAR(100),
    decimals INTEGER,
    logo_uri TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tokens_network ON tokens(network);
CREATE INDEX IF NOT EXISTS idx_tokens_address ON tokens(address);

-- Enable Row Level Security (optional - can disable for easier dev)
-- ALTER TABLE users ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE fills ENABLE ROW LEVEL SECURITY;

-- Policy for public read (if RLS enabled)
-- CREATE POLICY "Public read access" ON users FOR SELECT USING (true);
-- CREATE POLICY "Public read access" ON orders FOR SELECT USING (true);
-- CREATE POLICY "Public read access" ON fills FOR SELECT USING (true);


-- ====================================================================
-- File: src/backend/migrations/001_add_token_decimals_to_orders.sql
-- ====================================================================

-- Migration: Add token decimal fields to orders table
-- This adds support for tokens with non-18 decimals (e.g., CREPE with 9 decimals)

-- Add amount_in_decimals column (defaults to 18 for backward compatibility)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount_in_decimals INTEGER DEFAULT 18;

-- Add amount_out_decimals column (defaults to 18 for backward compatibility)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount_out_decimals INTEGER DEFAULT 18;

-- Add comment for documentation
COMMENT ON COLUMN orders.amount_in_decimals IS 'Decimals of the token_in (used for proper amount interpretation)';
COMMENT ON COLUMN orders.amount_out_decimals IS 'Decimals of the token_out (used for proper amount interpretation)';


-- ====================================================================
-- File: src/backend/migrations/002_add_performance_indexes.sql
-- ====================================================================

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


-- ====================================================================
-- File: src/backend/migrations/003_add_market_cap_to_pairs.sql
-- ====================================================================

-- Add market cap columns to pairs table
ALTER TABLE pairs
ADD COLUMN IF NOT EXISTS market_cap BIGINT DEFAULT 0,
ADD COLUMN IF NOT EXISTS market_cap_usd NUMERIC(40, 2) DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_pairs_market_cap ON pairs(market_cap);


-- ====================================================================
-- File: src/backend/migrations/004_add_token_metadata_and_pair_decimals.sql
-- ====================================================================

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


-- ====================================================================
-- File: src/backend/migrations/005_add_token_info_json_to_pairs.sql
-- ====================================================================

-- Migration: Add token metadata JSON fields to pairs table
-- This migration adds JSONB columns to store complete token metadata for base and quote tokens

-- Add base_token_info and quote_token_info JSONB columns to pairs table
ALTER TABLE IF EXISTS pairs
ADD COLUMN IF NOT EXISTS base_token_info JSONB,
ADD COLUMN IF NOT EXISTS quote_token_info JSONB;

-- Create index on the JSON columns for better query performance
CREATE INDEX IF NOT EXISTS idx_pairs_base_token_info ON pairs USING GIN(base_token_info);
CREATE INDEX IF NOT EXISTS idx_pairs_quote_token_info ON pairs USING GIN(quote_token_info);


-- ====================================================================
-- File: src/backend/migrations/006_add_fill_tx_hash_columns.sql
-- ====================================================================

-- Add separate transaction hash columns for Solana fills
ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS tx_hash_buy VARCHAR(100),
  ADD COLUMN IF NOT EXISTS tx_hash_sell VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_fills_tx_hash_buy ON fills(tx_hash_buy);
CREATE INDEX IF NOT EXISTS idx_fills_tx_hash_sell ON fills(tx_hash_sell);


-- ====================================================================
-- File: backend/migrations/007_add_refund_requests_table.sql
-- ====================================================================

-- +migrate Up
CREATE TABLE IF NOT EXISTS refund_requests (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    network VARCHAR(20) NOT NULL,
    token_mint VARCHAR(100) NOT NULL,
    amount NUMERIC(40,20) NOT NULL,
    user_address VARCHAR(66) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    tx_hash VARCHAR(128),
    error_msg TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_refund_requests_order_id ON refund_requests(order_id);
CREATE INDEX IF NOT EXISTS idx_refund_requests_user_id ON refund_requests(user_id);
CREATE INDEX IF NOT EXISTS idx_refund_requests_status ON refund_requests(status);
CREATE INDEX IF NOT EXISTS idx_refund_requests_tx_hash ON refund_requests(tx_hash);
CREATE INDEX IF NOT EXISTS idx_refund_requests_created_at ON refund_requests(created_at);

-- +migrate Down
-- DROP TABLE IF EXISTS refund_requests;


-- ====================================================================
-- File: backend/migrations/008_add_price_market_data_to_pairs.sql
-- ====================================================================

-- Migration 008: Add price & market data columns to pairs table
-- Run this in Supabase SQL Editor before deploying the price-worker

ALTER TABLE pairs
  ADD COLUMN IF NOT EXISTS price           DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS price_usd       DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS price_change_24h DECIMAL(20, 6)  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS price_high_24h  DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS price_low_24h   DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS volume_24h      DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS volume_24h_usd  DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS liquidity       DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS liquidity_usd   DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS market_cap      DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS market_cap_usd  DECIMAL(40, 20) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS trending_score  DECIMAL(10, 4)  DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_trade_at   TIMESTAMP WITH TIME ZONE,
  ADD COLUMN IF NOT EXISTS updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS pool_address    VARCHAR(100),
  ADD COLUMN IF NOT EXISTS dex_name        VARCHAR(100),
  ADD COLUMN IF NOT EXISTS base_token_decimals  INTEGER DEFAULT 18,
  ADD COLUMN IF NOT EXISTS quote_token_decimals INTEGER DEFAULT 18,
  ADD COLUMN IF NOT EXISTS base_token_info  JSONB,
  ADD COLUMN IF NOT EXISTS quote_token_info JSONB;

-- Index for fast trending/sorted queries
CREATE INDEX IF NOT EXISTS idx_pairs_trending_score ON pairs(trending_score DESC);
CREATE INDEX IF NOT EXISTS idx_pairs_price          ON pairs(price DESC)         WHERE price > 0;
CREATE INDEX IF NOT EXISTS idx_pairs_updated_at     ON pairs(updated_at DESC);

-- Comment
COMMENT ON COLUMN pairs.price          IS 'Native price (base token in quote token units), set by price-worker';
COMMENT ON COLUMN pairs.price_usd      IS 'USD price of base token, set by price-worker from GeckoTerminal';
COMMENT ON COLUMN pairs.price_change_24h IS '24h % price change, set by price-worker';
COMMENT ON COLUMN pairs.trending_score  IS 'Computed trending score, set by price-worker';


-- ====================================================================
-- File: seed_pairs.sql
-- ====================================================================

-- Insert sample pairs for testing
-- BSC network pairs

-- WBNB/BUSD pair
INSERT INTO pairs (id, network, pair_address, base_token, quote_token, base_symbol, quote_symbol, dex, pool_address, created_at) VALUES
('bsc_0x1b54cd932b6b751803c996cbef36280b53795f81', 'bsc', '0x1b54cd932b6b751803c996cbef36280b53795f81', '{"address": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", "symbol": "WBNB", "name": "Wrapped BNB", "decimals": 18}', '{"address": "0xe9e7cea3dedca5984780bafc599bd69add087d56", "symbol": "BUSD", "name": "BUSD Token", "decimals": 18}', 'WBNB', 'BUSD', 'PancakeSwap', '0x1b54cd932b6b751803c996cbef36280b53795f81', NOW());

-- CAKE/BUSD pair
INSERT INTO pairs (id, network, pair_address, base_token, quote_token, base_symbol, quote_symbol, dex, pool_address, created_at) VALUES
('bsc_0xf0750c373ebbb3baeef7e03d8300caad1983d67c', 'bsc', '0xf0750c373ebbb3baeef7e03d8300caad1983d67c', '{"address": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82", "symbol": "CAKE", "name": "PancakeSwap Token", "decimals": 18}', '{"address": "0xe9e7cea3dedca5984780bafc599bd69add087d56", "symbol": "BUSD", "name": "BUSD Token", "decimals": 18}', 'CAKE', 'BUSD', 'PancakeSwap', '0xf0750c373ebbb3baeef7e03d8300caad1983d67c', NOW());

-- BTCB/BUSD pair
INSERT INTO pairs (id, network, pair_address, base_token, quote_token, base_symbol, quote_symbol, dex, pool_address, created_at) VALUES
('bsc_0x1b54cd932b6b751803c996cbef36280b53795f82', 'bsc', '0x1b54cd932b6b751803c996cbef36280b53795f82', '{"address": "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c", "symbol": "BTCB", "name": "BTCB Token", "decimals": 18}', '{"address": "0xe9e7cea3dedca5984780bafc599bd69add087d56", "symbol": "BUSD", "name": "BUSD Token", "decimals": 18}', 'BTCB', 'BUSD', 'PancakeSwap', '0x1b54cd932b6b751803c996cbef36280b53795f82', NOW());

-- ETH/BUSD pair
INSERT INTO pairs (id, network, pair_address, base_token, quote_token, base_symbol, quote_symbol, dex, pool_address, created_at) VALUES
('bsc_0x1b54cd932b6b751803c996cbef36280b53795f83', 'bsc', '0x1b54cd932b6b751803c996cbef36280b53795f83', '{"address": "0x2170ed0880ac9a755fd29b2688956bd959f933f8", "symbol": "ETH", "name": "Ethereum Token", "decimals": 18}', '{"address": "0xe9e7cea3dedca5984780bafc599bd69add087d56", "symbol": "BUSD", "name": "BUSD Token", "decimals": 18}', 'ETH', 'BUSD', 'PancakeSwap', '0x1b54cd932b6b751803c996cbef36280b53795f83', NOW());

-- Base network pairs (if needed)
INSERT INTO pairs (id, network, pair_address, base_token, quote_token, base_symbol, quote_symbol, dex, pool_address, created_at) VALUES
('base_0xf8efb5234b2ca948e51e126acc7b543d761afa5a4d183b4f9371ad2ae135341b', 'base', '0xf8efb5234b2ca948e51e126acc7b543d761afa5a4d183b4f9371ad2ae135341b', '{"address": "0x4200000000000000000000000000000000000006", "symbol": "WETH", "name": "Wrapped Ether", "decimals": 18}', '{"address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", "symbol": "USDC", "name": "USD Coin", "decimals": 6}', 'WETH', 'USDC', 'Uniswap v3', '0xf8efb5234b2ca948e51e126acc7b543d761afa5a4d183b4f9371ad2ae135341b', NOW());

INSERT INTO pairs (id, network, pair_address, base_token, quote_token, base_symbol, quote_symbol, dex, pool_address, created_at) VALUES
('base_0xada0861fe7af31d338f7292df25683a6741a3b11fa42ce9e7fbdd530cb988f6c', 'base', '0xada0861fe7af31d338f7292df25683a6741a3b11fa42ce9e7fbdd530cb988f6c', '{"address": "0x4200000000000000000000000000000000000006", "symbol": "WETH", "name": "Wrapped Ether", "decimals": 18}', '{"address": "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca", "symbol": "USDbC", "name": "USD Base Coin", "decimals": 6}', 'WETH', 'USDbC', 'Uniswap v3', '0xada0861fe7af31d338f7292df25683a6741a3b11fa42ce9e7fbdd530cb988f6c', NOW());


-- ====================================================================
-- File: backend/fix_sol_mint.sql
-- ====================================================================

UPDATE refund_requests SET token_mint = '11111111111111111111111111111111112' WHERE token_mint = 'SOL';
UPDATE solana_deposits SET token_mint = '11111111111111111111111111111111112' WHERE token_mint = 'SOL';
UPDATE orders SET deposit_token_mint = '11111111111111111111111111111111112' WHERE deposit_token_mint = 'SOL';
UPDATE refund_requests SET token_mint = '11111111111111111111111111111111112', status = 'pending', retry_count = 0, error_msg = NULL, tx_hash = NULL WHERE id = 3;
UPDATE refund_requests SET status = 'pending', retry_count = 0, error_msg = NULL, tx_hash = NULL WHERE id = 4;
