# CexDex Backend (Go)

## Prerequisites
1. Install Go: https://go.dev/dl/
2. Supabase PostgreSQL account
3. Upstash Redis account

## Setup

### 1. Database Schema
Go to Supabase SQL Editor and run the contents of `schema.sql`

### 2. Environment Variables
The `.env` file is already configured with your Supabase and Upstash credentials.

### 3. Run the Backend
```bash
cd backend
go mod download
go build -o bin/api ./cmd/api
./bin/api
```

Or for development with auto-reload:
```bash
go run ./cmd/api
```

## API Endpoints

### Public
- `GET /health` - Health check
- `GET /api/v1/pairs` - List trading pairs
- `GET /api/v1/pairs/:id` - Get single pair
- `GET /api/v1/pairs/:id/orderbook` - Get orderbook
- `GET /api/v1/pairs/:id/trades` - Get recent trades
- `GET /api/v1/pairs/:id/ticker` - Get ticker
- `GET /api/v1/search?q=...` - Search pairs
- `GET /ws` - WebSocket for real-time updates

### Protected (Require JWT)
- `GET /api/v1/user/profile` - Get user profile
- `GET /api/v1/user/balances` - Get user balances
- `POST /api/v1/orders` - Create order
- `GET /api/v1/orders` - Get user orders
- `DELETE /api/v1/orders/:id` - Cancel order
- `POST /api/v1/orders/commit` - Commit order (commit-reveal)
- `POST /api/v1/orders/reveal` - Reveal order

## Order Types
- `limit` - Standard limit order
- `market` - Market order
- `stop_loss` - Stop loss order
- `take_profit` - Take profit order
- `post_only` - Post-only order (maker only)

## Features
- Matching engine with order book
- Stop-loss and take-profit triggers
- Commit-reveal for front-running protection
- Ladder orders (split into multiple price levels)
- Real-time WebSocket updates
- Redis caching for orderbook and tickers