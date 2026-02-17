package handlers

import (
	"replicated-clob/pkg/obs"
	"replicated-clob/pkg/orderbook"
	"replicated-clob/pkg/replica"
)

type Handler struct {
	orderbook *orderbook.OrderBook
	obs       *obs.Client
	replica   *replica.Coordinator
}

func New(obs *obs.Client, coordinator *replica.Coordinator) *Handler {
	localOrderbook := orderbook.New(obs)
	return &Handler{
		obs:       obs,
		orderbook: localOrderbook,
		replica:   coordinator,
	}
}
