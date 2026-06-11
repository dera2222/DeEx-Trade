package websocket

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var Upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Hub struct {
	// Register requests from clients
	Register chan *Client
	// Unregister requests from clients
	Unregister chan *Client
	// Broadcast to all clients
	Broadcast chan []byte
	// Pairs subscribed
	Pairs map[string]map[*Client]bool
	// Sequence for ordering
	sequence map[string]int64
	// Debug log history
	debugLogs []string
	// Mutex for thread safety
	mu sync.RWMutex
}

type Client struct {
	Hub    *Hub
	Conn   *websocket.Conn
	Send   chan []byte
	PairID string
}

type Message struct {
	Type    string      `json:"type"`
	PairID  string      `json:"pair_id,omitempty"`
	Payload interface{} `json:"payload,omitempty"`
}

type OrderbookUpdate struct {
	Asks     []Level `json:"asks"`
	Bids     []Level `json:"bids"`
	Sequence int64   `json:"sequence"`
}

type Level struct {
	Price  string `json:"price"`
	Amount string `json:"amount"`
	Orders int    `json:"orders"`
}

type TradeUpdate struct {
	ID           int64  `json:"id"`
	Price        string `json:"price"`
	PriceHuman   string `json:"price_human,omitempty"` // Human-readable price
	Amount       string `json:"amount"`
	AmountHuman  string `json:"amount_human,omitempty"` // Human-readable amount
	Side         string `json:"side"`
	Time         int64  `json:"time"`
	TxHash       string `json:"tx_hash"`
	TxHashBuy    string `json:"tx_hash_buy,omitempty"`
	TxHashSell   string `json:"tx_hash_sell,omitempty"`
	Decimals     int    `json:"decimals,omitempty"`      // Decimals for amount
	OrderID      int64  `json:"order_id,omitempty"`       // Maker order ID
	TakerOrderID int64  `json:"taker_order_id,omitempty"` // Taker order ID
}

type OrderUpdate struct {
	ID           int64  `json:"id"`
	Price        string `json:"price"`
	Amount       string `json:"amount"`
	FilledAmount string `json:"filled_amount"`
	Status       string `json:"status"`
	Side         string `json:"side"`
	PairID       string `json:"pair_id"`
}

type TickerUpdate struct {
	PairID         string `json:"pair_id"`
	LastPrice      string `json:"last_price"`
	PriceChange24h string `json:"price_change_24h"`
	Volume24h      string `json:"volume_24h"`               // Normalized (token units, not USD)
	Volume24hUSD   string `json:"volume_24h_usd,omitempty"` // USD value of 24h volume
	PriceUSD       string `json:"price_usd,omitempty"`
	PriceHigh24h   string `json:"price_high_24h,omitempty"`
	PriceLow24h    string `json:"price_low_24h,omitempty"`
	Liquidity      string `json:"liquidity,omitempty"`     // Normalized (token units)
	LiquidityUSD   string `json:"liquidity_usd,omitempty"` // USD value of liquidity
}

// PriceUpdate is a lightweight message broadcast on every fill.
// The UI uses this to flip from gecko price to last-trade price immediately
// without waiting for the next full pair refresh.
type PriceUpdate struct {
	PairID          string `json:"pair_id"`
	LastTradePrice  string `json:"last_trade_price"`
	LastTradeAt     int64  `json:"last_trade_at"` // Unix seconds
	Source          string `json:"source"`         // always "trade"
}

func NewHub() *Hub {
	return &Hub{
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan []byte),
		Pairs:      make(map[string]map[*Client]bool),
		sequence:   make(map[string]int64),
		debugLogs:  make([]string, 0, 200),
	}
}

func (h *Hub) addDebugLog(entry string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.debugLogs) >= 200 {
		h.debugLogs = h.debugLogs[1:]
	}
	h.debugLogs = append(h.debugLogs, entry)
}

func (h *Hub) GetDebugLogs() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	logs := make([]string, len(h.debugLogs))
	copy(logs, h.debugLogs)
	return logs
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			log.Printf("[WebSocket Hub] register client pair=%s", client.PairID)
			h.addDebugLog(fmt.Sprintf("register client pair=%s", client.PairID))
			if client.PairID == "all" {
				if h.Pairs["all"] == nil {
					h.Pairs["all"] = make(map[*Client]bool)
				}
				h.Pairs["all"][client] = true
			} else {
				if h.Pairs[client.PairID] == nil {
					h.Pairs[client.PairID] = make(map[*Client]bool)
				}
				h.Pairs[client.PairID][client] = true
			}

		case client := <-h.Unregister:
			log.Printf("[WebSocket Hub] unregister client pair=%s", client.PairID)
			h.addDebugLog(fmt.Sprintf("unregister client pair=%s", client.PairID))
			if client.PairID == "all" {
				if h.Pairs["all"] != nil {
					if _, ok := h.Pairs["all"][client]; ok {
						delete(h.Pairs["all"], client)
						close(client.Send)
					}
				}
			} else {
				if h.Pairs[client.PairID] != nil {
					if _, ok := h.Pairs[client.PairID][client]; ok {
						delete(h.Pairs[client.PairID], client)
						close(client.Send)
					}
				}
			}

		case message := <-h.Broadcast:
			for pairID, clients := range h.Pairs {
				if pairID == "all" {
					for client := range clients {
						select {
						case client.Send <- message:
						default:
							close(client.Send)
							delete(clients, client)
						}
					}
				}
			}
		}
	}
}

func (h *Hub) BroadcastToPair(pairID string, message []byte) {
	log.Printf("[WebSocket Hub] broadcast to pair=%s message=%s", pairID, message)
	h.addDebugLog(fmt.Sprintf("broadcast to pair=%s message=%s", pairID, message))
	h.mu.Lock()
	defer h.mu.Unlock()

	if clients, ok := h.Pairs[pairID]; ok {
		for client := range clients {
			select {
			case client.Send <- message:
			default:
				close(client.Send)
				delete(clients, client)
			}
		}
	}

	if clients, ok := h.Pairs["all"]; ok {
		for client := range clients {
			select {
			case client.Send <- message:
			default:
				close(client.Send)
				delete(clients, client)
			}
		}
	}
}

func (h *Hub) BroadcastOrderbookUpdate(pairID string) {
	h.sequence[pairID]++

	msg := Message{
		Type:   "orderbook",
		PairID: pairID,
		Payload: OrderbookUpdate{
			Sequence: h.sequence[pairID],
		},
	}

	data, _ := json.Marshal(msg)
	h.BroadcastToPair(pairID, data)
}

// BroadcastLiquidityUpdate sends a liquidity update for a pair
func (h *Hub) BroadcastLiquidityUpdate(pairID string, liquidity string, liquidityUSD string) {
	log.Printf("[WebSocket Hub] BroadcastLiquidityUpdate pair=%s liquidity=%s liquidityUSD=%s", pairID, liquidity, liquidityUSD)
	msg := Message{
		Type:   "liquidity",
		PairID: pairID,
		Payload: map[string]string{
			"liquidity":     liquidity,
			"liquidity_usd": liquidityUSD,
		},
	}

	data, _ := json.Marshal(msg)
	h.BroadcastToPair(pairID, data)
}

func (h *Hub) BroadcastTradeUpdate(pairID string, trade TradeUpdate) {
	log.Printf("[WebSocket Hub] BroadcastTradeUpdate pair=%s trade=%+v", pairID, trade)
	msg := Message{
		Type:    "trade",
		PairID:  pairID,
		Payload: trade,
	}

	data, _ := json.Marshal(msg)
	h.BroadcastToPair(pairID, data)
}

func (h *Hub) BroadcastTickerUpdate(ticker TickerUpdate) {
	log.Printf("[WebSocket Hub] BroadcastTickerUpdate pair=%s ticker=%+v", ticker.PairID, ticker)
	msg := Message{
		Type:    "ticker",
		PairID:  ticker.PairID,
		Payload: ticker,
	}

	data, _ := json.Marshal(msg)
	h.BroadcastToPair(ticker.PairID, data)
}

// BroadcastPriceUpdate sends a lightweight price update to all subscribers of a pair
// as soon as a fill is created — before on-chain settlement.
// The UI will switch from gecko price to this trade price immediately.
func (h *Hub) BroadcastPriceUpdate(update PriceUpdate) {
	msg := Message{
		Type:    "price_update",
		PairID:  update.PairID,
		Payload: update,
	}
	data, _ := json.Marshal(msg)
	h.BroadcastToPair(update.PairID, data)
}

// BroadcastOrderUpdate sends an order update for a specific order
func (h *Hub) BroadcastOrderUpdate(orderID int64, order interface{}) {
	msg := Message{
		Type:   "order_update",
		Payload: map[string]interface{}{
			"order_id": orderID,
			"order":    order,
		},
	}

	data, _ := json.Marshal(msg)
	// Broadcast to all clients since order updates are user-specific
	h.BroadcastToPair("all", data)
}

func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister <- c
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(512 * 1024)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		// Handle incoming messages (e.g., subscribe to pairs)
		var msg Message
		if err := json.Unmarshal(message, &msg); err == nil {
			log.Printf("[WebSocket Hub] client subscribe request pair=%s raw=%s", msg.PairID, string(message))
			if msg.Type == "subscribe" && msg.PairID != "" {
				c.Hub.Unregister <- c
				c.PairID = msg.PairID
				c.Hub.Register <- c
			}
		}
	}
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
