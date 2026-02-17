package orderbook

import (
	"context"
	"sort"
)

func (ob *OrderBook) OpenOrdersForUser(ctx context.Context, user string) []Order {
	ob.obs.LogInfo(ctx, "orderbook.open_orders.query user=%s", user)

	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if user == "" {
		return []Order{}
	}

	orders := make([]Order, 0)
	orders = append(orders, ob.collectOrdersForUser(user, true)...)
	orders = append(orders, ob.collectOrdersForUser(user, false)...)

	return orders
}

func (ob *OrderBook) collectOrdersForUser(user string, isBid bool) []Order {
	levels := ob.sortedLevels(isBid)
	orders := make([]Order, 0)

	for _, level := range levels {
		for _, order := range level.Orders {
			if order.User == user && order.Amount > 0 {
				orders = append(orders, order)
			}
		}
	}

	return orders
}

func (ob *OrderBook) sortedLevels(isBid bool) []*OrderbookLevel {
	var levels map[int64]*OrderbookLevel
	if isBid {
		levels = ob.bidsByPrice
	} else {
		levels = ob.asksByPrice
	}

	levelList := make([]*OrderbookLevel, 0, len(levels))
	for _, level := range levels {
		levelList = append(levelList, level)
	}

	sort.Slice(levelList, func(i, j int) bool {
		if isBid {
			return levelList[i].Price > levelList[j].Price
		}
		return levelList[i].Price < levelList[j].Price
	})

	return levelList
}
