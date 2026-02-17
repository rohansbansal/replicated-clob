package schemas

type PostLimitRequest struct {
	User       string `json:"user"`
	PriceLevel int64  `json:"priceLevel"`
	Amount     int64  `json:"amount"`
	IsBid      bool   `json:"isBid"`
}

type PostLimitMatch struct {
	Size  int64 `json:"size"`
	Price int64 `json:"price"`
}

type PostLimitResponse struct {
	OrderID string           `json:"orderId"`
	Fills   []PostLimitMatch `json:"fills"`
}

type CancelLimitRequest struct {
	OrderID string `json:"orderId"`
}

type CancelLimitResponse struct {
	SizeCancelled int64
}

type OpenOrder struct {
	User       string `json:"user"`
	OrderID    string `json:"orderId"`
	PriceLevel int64  `json:"priceLevel"`
	Amount     int64  `json:"amount"`
	IsBid      bool   `json:"isBid"`
}

type OpenOrdersResponse struct {
	Orders []OpenOrder `json:"orders"`
}

type Fill struct {
	Counterparty string `json:"counterparty"`
	Size         int64  `json:"size"`
	PriceLevel   int64  `json:"priceLevel"`
	IsMaker      bool   `json:"isMaker"`
}

type FillsResponse struct {
	Fills []Fill `json:"fills"`
}

type NotLeaderResponse struct {
	Error  string `json:"error"`
	Leader string `json:"leader"`
}
