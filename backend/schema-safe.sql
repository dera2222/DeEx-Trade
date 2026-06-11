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
