package orderbook

import (
	"replicated-clob/pkg/obs"
	"sync"

	"github.com/google/uuid"
)

// track orders in PTP
type Order struct {
	User       string    `json:"user"`
	ID         uuid.UUID `json:"orderId"`
	PriceLevel int64     `json:"priceLevel"` // store price in cents
	Amount     int64     `json:"amount"`
	IsBid      bool      `json:"isBid"`
}

type OrderbookLevel struct {
	Price  int64 // in cents
	Amount int64
	Orders []Order
}

type UserFill struct {
	Counterparty string `json:"counterparty"`
	Size         int64  `json:"size"`
	PriceLevel   int64  `json:"priceLevel"`
	IsMaker      bool   `json:"isMaker"`
}

type orderRef struct {
	isBid bool
	level *OrderbookLevel
	index int
}

type OrderBook struct {
	bids        orderLevelHeap
	asks        orderLevelHeap
	bidsByPrice map[int64]*OrderbookLevel
	asksByPrice map[int64]*OrderbookLevel
	ordersByID  map[uuid.UUID]orderRef
	fillsByUser map[string][]UserFill
	obs         *obs.Client
	mu          sync.RWMutex
}
