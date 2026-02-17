package handlers

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"replicated-clob/pkg/obs"
	"replicated-clob/pkg/orderbook"
	"replicated-clob/pkg/replica"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

func newTestHandlerApp() (*fiber.App, *orderbook.OrderBook, *obs.Client) {
	obsClient := &obs.Client{}
	// test without replicating
	rep := replica.NewCoordinator(replica.NodeRolePrimary, []string{}, "test-cluster")
	h := New(obsClient, rep)
	app := fiber.New()
	app.Post("/order/post", h.PostOrder)
	app.Post("/order/cancel", h.CancelOrder)
	app.Get("/orders/:userId", h.GetOpenOrders)
	app.Get("/fills/:userId", h.GetFillsForUser)
	return app, h.orderbook, obsClient
}

func TestPostOrderEndpoint(t *testing.T) {
	app, _, _ := newTestHandlerApp()

	req := httptest.NewRequest(
		"POST",
		"/order/post",
		bytes.NewReader([]byte(`{"user":"alice","priceLevel":101,"amount":5,"isBid":false}`)),
	)
	req.Header.Set("Content-Type", "application/json")
	res, err := app.Test(req)
	if err != nil {
		t.Fatalf("failed to call endpoint: %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	var response struct {
		OrderID string `json:"orderId"`
		Fills   []struct {
			Size  int64 `json:"size"`
			Price int64 `json:"price"`
		} `json:"fills"`
	}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if response.OrderID == "" {
		t.Fatalf("expected non-empty orderId")
	}
	if len(response.Fills) != 0 {
		t.Fatalf("expected 0 fills for fresh post, got %d", len(response.Fills))
	}
}

func TestPostOrderEndpointRejectsInvalidBody(t *testing.T) {
	app, _, _ := newTestHandlerApp()

	req := httptest.NewRequest("POST", "/order/post", bytes.NewReader([]byte(`{`)))
	req.Header.Set("Content-Type", "application/json")
	res, err := app.Test(req)
	if err != nil {
		t.Fatalf("failed to call endpoint: %v", err)
	}
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestCancelOrderEndpointRejectsInvalidID(t *testing.T) {
	app, _, _ := newTestHandlerApp()

	req := httptest.NewRequest(
		"POST",
		"/order/cancel",
		bytes.NewReader([]byte(`{"orderId":"abc"}`)),
	)
	req.Header.Set("Content-Type", "application/json")
	res, err := app.Test(req)
	if err != nil {
		t.Fatalf("failed to call endpoint: %v", err)
	}
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestCancelOrderEndpointCancelsRestingOrder(t *testing.T) {
	app, _, _ := newTestHandlerApp()
	postReq := httptest.NewRequest(
		"POST",
		"/order/post",
		bytes.NewReader([]byte(`{"user":"maker","priceLevel":120,"amount":3,"isBid":false}`)),
	)
	postReq.Header.Set("Content-Type", "application/json")
	postRes, err := app.Test(postReq)
	if err != nil {
		t.Fatalf("failed to post order: %v", err)
	}
	if postRes.StatusCode != 200 {
		t.Fatalf("expected post status 200, got %d", postRes.StatusCode)
	}

	var posted struct {
		OrderID string `json:"orderId"`
	}
	if err := json.NewDecoder(postRes.Body).Decode(&posted); err != nil {
		t.Fatalf("failed to decode post response: %v", err)
	}
	if _, err := uuid.Parse(posted.OrderID); err != nil {
		t.Fatalf("expected valid UUID orderId, got %q", posted.OrderID)
	}

	cancelPayload, err := json.Marshal(struct {
		OrderID string `json:"orderId"`
	}{
		OrderID: posted.OrderID,
	})
	if err != nil {
		t.Fatalf("failed to marshal cancel payload: %v", err)
	}

	// use the posted UUID in cancellation request
	cancelReq := httptest.NewRequest(
		"POST",
		"/order/cancel",
		bytes.NewReader(cancelPayload),
	)
	cancelReq.Header.Set("Content-Type", "application/json")
	cancelRes, err := app.Test(cancelReq)
	if err != nil {
		t.Fatalf("failed to cancel order: %v", err)
	}
	if cancelRes.StatusCode != 200 {
		t.Fatalf("expected cancel status 200, got %d", cancelRes.StatusCode)
	}

	var response struct {
		SizeCancelled int64 `json:"SizeCancelled"`
	}
	if err := json.NewDecoder(cancelRes.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode cancel response: %v", err)
	}
	if response.SizeCancelled != 3 {
		t.Fatalf("expected 3 cancelled, got %d", response.SizeCancelled)
	}
}

func TestCancelOrderEndpointNoMatchIdReturnsZero(t *testing.T) {
	app, _, _ := newTestHandlerApp()

	cancelReq := httptest.NewRequest(
		"POST",
		"/order/cancel",
		bytes.NewReader([]byte(`{"orderId":"00000000-0000-0000-0000-00000000abcd"}`)),
	)
	cancelReq.Header.Set("Content-Type", "application/json")
	cancelRes, err := app.Test(cancelReq)
	if err != nil {
		t.Fatalf("failed to cancel order: %v", err)
	}
	if cancelRes.StatusCode != 404 {
		t.Fatalf("expected cancel status 404, got %d", cancelRes.StatusCode)
	}

	var response struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(cancelRes.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode cancel response: %v", err)
	}
	if response.Error != "order not found" {
		t.Fatalf("expected not found error, got %q", response.Error)
	}
}

func TestGetOpenOrdersEndpointReturnsOpenOrdersForUser(t *testing.T) {
	app, _, _ := newTestHandlerApp()

	// Alice has one resting bid; Bob has one ask that will cross
	postAlice := httptest.NewRequest(
		"POST",
		"/order/post",
		bytes.NewReader([]byte(`{"user":"alice","priceLevel":101,"amount":5,"isBid":true}`)),
	)
	postAlice.Header.Set("Content-Type", "application/json")
	if _, err := app.Test(postAlice); err != nil {
		t.Fatalf("failed to post alice order: %v", err)
	}

	postBobAsk := httptest.NewRequest(
		"POST",
		"/order/post",
		bytes.NewReader([]byte(`{"user":"bob","priceLevel":100,"amount":4,"isBid":false}`)),
	)
	postBobAsk.Header.Set("Content-Type", "application/json")
	if _, err := app.Test(postBobAsk); err != nil {
		t.Fatalf("failed to post bob order: %v", err)
	}

	// This should fill against alice's bid fully and leave none for alice
	takerReq := httptest.NewRequest(
		"POST",
		"/order/post",
		bytes.NewReader([]byte(`{"user":"carol","priceLevel":101,"amount":5,"isBid":false}`)),
	)
	takerReq.Header.Set("Content-Type", "application/json")
	if _, err := app.Test(takerReq); err != nil {
		t.Fatalf("failed to post taker order: %v", err)
	}

	res, err := app.Test(httptest.NewRequest("GET", "/orders/alice", nil))
	if err != nil {
		t.Fatalf("failed to request alice orders: %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200 for alice orders, got %d", res.StatusCode)
	}

	var aliceOrdersResp struct {
		Orders []struct {
			User       string `json:"user"`
			OrderID    string `json:"orderId"`
			PriceLevel int64  `json:"priceLevel"`
			Amount     int64  `json:"amount"`
			IsBid      bool   `json:"isBid"`
		} `json:"orders"`
	}
	if err := json.NewDecoder(res.Body).Decode(&aliceOrdersResp); err != nil {
		t.Fatalf("failed to decode alice orders: %v", err)
	}
	if len(aliceOrdersResp.Orders) != 0 {
		t.Fatalf("expected alice to have no open orders, got %d", len(aliceOrdersResp.Orders))
	}

	res, err = app.Test(httptest.NewRequest("GET", "/orders/bob", nil))
	if err != nil {
		t.Fatalf("failed to request bob orders: %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200 for bob orders, got %d", res.StatusCode)
	}

	var bobOrdersResp struct {
		Orders []struct {
			User       string `json:"user"`
			OrderID    string `json:"orderId"`
			PriceLevel int64  `json:"priceLevel"`
			Amount     int64  `json:"amount"`
			IsBid      bool   `json:"isBid"`
		} `json:"orders"`
	}
	if err := json.NewDecoder(res.Body).Decode(&bobOrdersResp); err != nil {
		t.Fatalf("failed to decode bob orders: %v", err)
	}
	if len(bobOrdersResp.Orders) != 0 {
		t.Fatalf("expected bob to have 0 open orders, got %d", len(bobOrdersResp.Orders))
	}
}

func TestGetFillsEndpointReturnsHistoricalFills(t *testing.T) {
	app, _, _ := newTestHandlerApp()

	postAlice := httptest.NewRequest(
		"POST",
		"/order/post",
		bytes.NewReader([]byte(`{"user":"maker","priceLevel":100,"amount":4,"isBid":false}`)),
	)
	postAlice.Header.Set("Content-Type", "application/json")
	if _, err := app.Test(postAlice); err != nil {
		t.Fatalf("failed to post maker order: %v", err)
	}

	postTaker := httptest.NewRequest(
		"POST",
		"/order/post",
		bytes.NewReader([]byte(`{"user":"taker","priceLevel":100,"amount":3,"isBid":true}`)),
	)
	postTaker.Header.Set("Content-Type", "application/json")
	postRes, err := app.Test(postTaker)
	if err != nil {
		t.Fatalf("failed to post taker order: %v", err)
	}
	if postRes.StatusCode != 200 {
		t.Fatalf("expected taker post status 200, got %d", postRes.StatusCode)
	}

	res, err := app.Test(httptest.NewRequest("GET", "/fills/maker", nil))
	if err != nil {
		t.Fatalf("failed to request maker fills: %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected 200 for maker fills, got %d", res.StatusCode)
	}

	var makerFillsResp struct {
		Fills []struct {
			Counterparty string `json:"counterparty"`
			Size         int64  `json:"size"`
			PriceLevel   int64  `json:"priceLevel"`
			IsMaker      bool   `json:"isMaker"`
		} `json:"fills"`
	}
	if err := json.NewDecoder(res.Body).Decode(&makerFillsResp); err != nil {
		t.Fatalf("failed to decode maker fills: %v", err)
	}
	if len(makerFillsResp.Fills) != 1 {
		t.Fatalf("expected 1 fill for maker, got %d", len(makerFillsResp.Fills))
	}
	if makerFillsResp.Fills[0].Counterparty != "taker" || makerFillsResp.Fills[0].Size != 3 || makerFillsResp.Fills[0].PriceLevel != 100 {
		t.Fatalf("unexpected maker fill: %+v", makerFillsResp.Fills[0])
	}
}
