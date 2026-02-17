package orderbook

import (
	"container/heap"
	"context"
	"errors"

	"replicated-clob/pkg/obs"
	"replicated-clob/schemas"

	"github.com/google/uuid"
)

func New(obs *obs.Client) *OrderBook {
	return &OrderBook{
		bids:        orderLevelHeap{isBid: true},
		asks:        orderLevelHeap{isBid: false},
		bidsByPrice: map[int64]*OrderbookLevel{},
		asksByPrice: map[int64]*OrderbookLevel{},
		ordersByID:  map[uuid.UUID]orderRef{},
		fillsByUser: map[string][]UserFill{},
		obs:         obs,
	}
}

func (ob *OrderBook) PostLimit(ctx context.Context, user string, orderID uuid.UUID, priceLevel int64, amount int64, isBid bool) schemas.PostLimitResponse {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	incoming := &Order{
		User:       user,
		ID:         orderID,
		PriceLevel: priceLevel,
		Amount:     amount,
		IsBid:      isBid,
	}

	var fills []schemas.PostLimitMatch
	if isBid {
		fills = ob.matchIncoming(ctx, incoming, &ob.asks, false, func(levelPrice int64) bool {
			return levelPrice <= incoming.PriceLevel
		})
		if incoming.Amount > 0 {
			ob.addOrder(incoming)
			ob.obs.LogInfo(ctx, "orderbook.post_limit.resting_order_added user=%s order_id=%s price=%d amount=%d", incoming.User, incoming.ID, incoming.PriceLevel, incoming.Amount)
		}
	} else {
		fills = ob.matchIncoming(ctx, incoming, &ob.bids, true, func(levelPrice int64) bool {
			return levelPrice >= incoming.PriceLevel
		})
		if incoming.Amount > 0 {
			ob.addOrder(incoming)
			ob.obs.LogInfo(ctx, "orderbook.post_limit.resting_order_added user=%s order_id=%s price=%d amount=%d", incoming.User, incoming.ID, incoming.PriceLevel, incoming.Amount)
		}
	}

	return schemas.PostLimitResponse{
		OrderID: incoming.ID.String(),
		Fills:   fills,
	}
}

func (ob *OrderBook) FillsForUser(ctx context.Context, user string) []UserFill {
	ob.obs.LogInfo(ctx, "orderbook.fills.query user=%s", user)

	ob.mu.RLock()
	defer ob.mu.RUnlock()

	fills := make([]UserFill, len(ob.fillsByUser[user]))
	copy(fills, ob.fillsByUser[user])
	return fills
}

func (ob *OrderBook) CancelLimitOrder(ctx context.Context, orderID uuid.UUID) (schemas.CancelLimitResponse, error) {
	ob.obs.LogInfo(ctx, "orderbook.cancel.start order_id=%s", orderID)

	ob.mu.Lock()
	defer ob.mu.Unlock()

	ref, ok := ob.ordersByID[orderID]
	if !ok {
		ob.obs.LogInfo(ctx, "orderbook.cancel.done order_id=%s size_cancelled=0", orderID)
		return schemas.CancelLimitResponse{}, errors.New("order not found")
	}

	sideLevels, sideMap := ob.bookSide(ref.isBid)
	removed, ok := ob.removeOrder(ref.isBid, ref.level, ref.index)
	if !ok {
		delete(ob.ordersByID, orderID)
		ob.obs.LogInfo(ctx, "orderbook.cancel.done order_id=%s size_cancelled=0", orderID)
		return schemas.CancelLimitResponse{}, errors.New("order not found")
	}

	if ref.level.Amount <= 0 {
		ob.removeLevel(sideLevels, sideMap, ref.level)
	}

	ob.obs.LogInfo(ctx, "orderbook.cancel.done order_id=%s size_cancelled=%d", orderID, removed.Amount)
	return schemas.CancelLimitResponse{
		SizeCancelled: removed.Amount,
	}, nil
}

func (ob *OrderBook) HasOrder(orderID uuid.UUID) bool {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	_, exists := ob.ordersByID[orderID]
	return exists
}

func (ob *OrderBook) matchIncoming(ctx context.Context, incoming *Order, opposite *orderLevelHeap, oppositeIsBid bool, canMatch func(price int64) bool) []schemas.PostLimitMatch {
	var fills []schemas.PostLimitMatch

	for opposite.Len() > 0 && incoming.Amount > 0 {
		level := opposite.Peek()
		if level == nil || !canMatch(level.Price) {
			break
		}

		if len(level.Orders) == 0 || level.Amount <= 0 {
			ob.removeLevel(opposite, ob.priceLevelsBySide(oppositeIsBid), level)
			continue
		}

		for len(level.Orders) > 0 && incoming.Amount > 0 {
			resting := &level.Orders[0]

			if resting.Amount <= 0 {
				ob.removeOrder(oppositeIsBid, level, 0)
				continue
			}

			matched := min(incoming.Amount, resting.Amount)
			if matched <= 0 {
				ob.removeOrder(oppositeIsBid, level, 0)
				continue
			}

			fills = append(fills, schemas.PostLimitMatch{
				Size:  matched,
				Price: level.Price,
			})
			incoming.Amount -= matched
			level.Amount -= matched
			resting.Amount -= matched

			ob.obs.LogInfo(
				ctx,
				"orderbook.match user=%s matched_with=%s side=%s price=%d matched=%d remaining_incoming=%d remaining_resting=%d level_remaining=%d",
				incoming.User,
				resting.User,
				takeSide(incoming.IsBid),
				level.Price,
				matched,
				incoming.Amount,
				resting.Amount,
				level.Amount,
			)
			ob.recordFill(incoming.User, resting.User, matched, level.Price, false)
			ob.recordFill(resting.User, incoming.User, matched, level.Price, true)

			if resting.Amount == 0 {
				ob.removeOrder(oppositeIsBid, level, 0)
			}
		}

		if level.Amount <= 0 {
			ob.removeLevel(opposite, ob.priceLevelsBySide(oppositeIsBid), level)
		}
	}

	return fills
}

func (ob *OrderBook) addOrder(order *Order) {
	sideLevels, _ := ob.bookSide(order.IsBid)

	priceLevels := ob.priceLevelsBySide(order.IsBid)
	level, ok := priceLevels[order.PriceLevel]
	if !ok {
		level = &OrderbookLevel{
			Price: order.PriceLevel,
		}
		priceLevels[order.PriceLevel] = level
		heap.Push(sideLevels, level)
	}

	level.Orders = append(level.Orders, *order)
	level.Amount += order.Amount
	ob.ordersByID[order.ID] = orderRef{
		isBid: order.IsBid,
		level: level,
		index: len(level.Orders) - 1,
	}
}

func (ob *OrderBook) removeLevel(sideLevels *orderLevelHeap, sidePriceMap map[int64]*OrderbookLevel, level *OrderbookLevel) {
	if level == nil {
		return
	}

	levelIdx := ob.indexOfLevel(sideLevels, level)
	if levelIdx >= 0 {
		heap.Remove(sideLevels, levelIdx)
	}
	delete(sidePriceMap, level.Price)
}

func (ob *OrderBook) indexOfLevel(sideLevels *orderLevelHeap, level *OrderbookLevel) int {
	for i, candidate := range sideLevels.levels {
		if candidate == level {
			return i
		}
	}

	return -1
}

func (ob *OrderBook) removeOrder(isBid bool, level *OrderbookLevel, orderIndex int) (Order, bool) {
	if len(level.Orders) == 0 || orderIndex < 0 || orderIndex >= len(level.Orders) {
		return Order{}, false
	}

	removed := level.Orders[orderIndex]
	delete(ob.ordersByID, removed.ID)

	copy(level.Orders[orderIndex:], level.Orders[orderIndex+1:])
	level.Orders = level.Orders[:len(level.Orders)-1]
	level.Amount -= removed.Amount
	if level.Amount < 0 {
		level.Amount = 0
	}

	for i := orderIndex; i < len(level.Orders); i++ {
		ob.ordersByID[level.Orders[i].ID] = orderRef{
			isBid: isBid,
			level: level,
			index: i,
		}
	}

	return removed, true
}

func (ob *OrderBook) bookSide(isBid bool) (*orderLevelHeap, map[int64]*OrderbookLevel) {
	if isBid {
		return &ob.bids, ob.bidsByPrice
	}
	return &ob.asks, ob.asksByPrice
}

func (ob *OrderBook) priceLevelsBySide(isBid bool) map[int64]*OrderbookLevel {
	if isBid {
		return ob.bidsByPrice
	}
	return ob.asksByPrice
}

func takeSide(isBid bool) string {
	if isBid {
		return "bid"
	}
	return "ask"
}

func (ob *OrderBook) recordFill(user, counterparty string, size, priceLevel int64, isMaker bool) {
	fill := UserFill{
		Counterparty: counterparty,
		Size:         size,
		PriceLevel:   priceLevel,
		IsMaker:      isMaker,
	}
	ob.fillsByUser[user] = append(ob.fillsByUser[user], fill)
}
