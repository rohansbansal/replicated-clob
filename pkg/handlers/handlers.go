package handlers

import (
	"replicated-clob/pkg/obs"
	"replicated-clob/pkg/orderbook"
	"replicated-clob/pkg/replica"

	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	orderbook *orderbook.OrderBook
	obs       *obs.Client
	replica   *replica.Coordinator
	replication *replica.ReplicationManager
}

func New(obs *obs.Client, coordinator *replica.Coordinator) *Handler {
	orderbook := orderbook.New(obs)
	return &Handler{
		obs:       obs,
		orderbook: orderbook,
		replica:   coordinator,
		replication: replica.NewReplicationManager(coordinator, obs),
	}
}

func (h *Handler) RequireWriteAccess() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if !h.replica.CanAcceptWrite() {
			return temporaryRedirect(c, h.replica.Primary())
		}
		return c.Next()
	}
}
