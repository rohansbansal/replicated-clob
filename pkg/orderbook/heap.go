package orderbook

import "container/heap"

type orderLevelHeap struct {
	levels []*OrderbookLevel
	isBid  bool
}

func (h orderLevelHeap) Len() int {
	return len(h.levels)
}

func (h orderLevelHeap) Less(i, j int) bool {
	if h.isBid {
		return h.levels[i].Price > h.levels[j].Price
	}
	return h.levels[i].Price < h.levels[j].Price
}

func (h orderLevelHeap) Swap(i, j int) {
	h.levels[i], h.levels[j] = h.levels[j], h.levels[i]
}

func (h *orderLevelHeap) Push(x any) {
	h.levels = append(h.levels, x.(*OrderbookLevel))
}

func (h *orderLevelHeap) Pop() any {
	old := h.levels
	n := len(old)
	item := old[n-1]
	h.levels = old[:n-1]
	return item
}

func (h *orderLevelHeap) Peek() *OrderbookLevel {
	if h.Len() == 0 {
		return nil
	}
	return h.levels[0]
}

var _ heap.Interface = (*orderLevelHeap)(nil)
