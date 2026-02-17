package orderbook

import (
	"context"
	"testing"

	"replicated-clob/schemas"

	"replicated-clob/pkg/obs"

	"github.com/google/uuid"
)

func TestPostLimitMatchesByPriceThenFIFO(t *testing.T) {
	ob := New(&obs.Client{})
	ctx := context.Background()

	// Best ask first: 99 then 100, with same-price FIFO.
	ob.PostLimit(ctx, "makerA", uuid.New(), 100, 2, false)
	ob.PostLimit(ctx, "makerB", uuid.New(), 99, 2, false)
	ob.PostLimit(ctx, "makerC", uuid.New(), 100, 3, false)

	resp := ob.PostLimit(ctx, "taker", uuid.New(), 105, 6, true)
	if len(resp.Fills) != 3 {
		t.Fatalf("expected 3 fill events, got %d", len(resp.Fills))
	}
	if matchedSize(resp.Fills) != 6 {
		t.Fatalf("expected 6 matched, got %d", matchedSize(resp.Fills))
	}

	fills := ob.FillsForUser(ctx, "taker")
	if len(fills) != 3 {
		t.Fatalf("expected 3 fill events, got %d", len(fills))
	}

	if fills[0].Counterparty != "makerB" || fills[0].Size != 2 || fills[0].PriceLevel != 99 {
		t.Fatalf("unexpected first fill: %+v", fills[0])
	}
	if fills[1].Counterparty != "makerA" || fills[1].Size != 2 || fills[1].PriceLevel != 100 {
		t.Fatalf("unexpected second fill: %+v", fills[1])
	}
	if fills[2].Counterparty != "makerC" || fills[2].Size != 2 || fills[2].PriceLevel != 100 {
		t.Fatalf("unexpected third fill: %+v", fills[2])
	}
}

func TestPostLimitAllowsSelfTradeAndTracksBothSides(t *testing.T) {
	ob := New(&obs.Client{})
	ctx := context.Background()

	ob.PostLimit(ctx, "alice", uuid.New(), 100, 4, false)
	resp := ob.PostLimit(ctx, "alice", uuid.New(), 101, 3, true)
	if matchedSize(resp.Fills) != 3 {
		t.Fatalf("expected 3 matched, got %d", matchedSize(resp.Fills))
	}

	fills := ob.FillsForUser(ctx, "alice")
	if len(fills) != 2 {
		t.Fatalf("expected 2 fill events for same user, got %d", len(fills))
	}

	if fills[0].Counterparty != "alice" || fills[0].IsMaker {
		t.Fatalf("expected first fill to be taker leg: %+v", fills[0])
	}
	if !fills[1].IsMaker || fills[1].Counterparty != "alice" {
		t.Fatalf("expected second fill to be maker leg: %+v", fills[1])
	}
}

func matchedSize(matches []schemas.PostLimitMatch) int64 {
	var total int64
	for _, match := range matches {
		total += match.Size
	}
	return total
}

func TestCancelLimitOrderByID(t *testing.T) {
	ob := New(&obs.Client{})
	ctx := context.Background()

	postResponse := ob.PostLimit(ctx, "maker", uuid.New(), 120, 7, false)
	if len(ob.asksByPrice) != 1 {
		t.Fatalf("expected one resting ask level")
	}
	orderID, err := uuid.Parse(postResponse.OrderID)
	if err != nil {
		t.Fatalf("unexpected invalid order id: %v", err)
	}

	resp, err := ob.CancelLimitOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.SizeCancelled != 7 {
		t.Fatalf("expected 7 cancelled, got %d", resp.SizeCancelled)
	}
	if len(ob.asksByPrice) != 0 {
		t.Fatalf("expected orderbook side to be empty after cancel")
	}
}

func TestPostLimitMatchesBestAskPriceFirst(t *testing.T) {
	ob := New(&obs.Client{})
	ctx := context.Background()

	ob.PostLimit(ctx, "makerA", uuid.New(), 105, 3, false)
	ob.PostLimit(ctx, "makerB", uuid.New(), 100, 2, false)

	resp := ob.PostLimit(ctx, "taker", uuid.New(), 105, 5, true)
	if len(resp.Fills) != 2 {
		t.Fatalf("expected 2 fill events, got %d", len(resp.Fills))
	}
	if resp.Fills[0].Price != 100 {
		t.Fatalf("expected first fill at price 100, got %d", resp.Fills[0].Price)
	}
	if resp.Fills[1].Price != 105 {
		t.Fatalf("expected second fill at price 105, got %d", resp.Fills[1].Price)
	}
	if matchedSize(resp.Fills) != 5 {
		t.Fatalf("expected 5 matched, got %d", matchedSize(resp.Fills))
	}
}

func TestCancelLimitOrderMissingIDReturnsZero(t *testing.T) {
	ob := New(&obs.Client{})
	ctx := context.Background()

	resp, err := ob.CancelLimitOrder(ctx, uuid.MustParse("00000000-0000-0000-0000-000000000000"))
	if err == nil {
		t.Fatalf("expected order not found error")
	}
	if err.Error() != "order not found" {
		t.Fatalf("expected order not found error, got %v", err)
	}
	if resp.SizeCancelled != 0 {
		t.Fatalf("expected 0 cancelled for missing id, got %d", resp.SizeCancelled)
	}
}
