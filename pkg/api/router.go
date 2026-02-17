package api

import (
	"replicated-clob/pkg/handlers"
	"replicated-clob/pkg/obs"

	"github.com/gofiber/fiber/v2"
)

func New(router fiber.Router, handler *handlers.Handler, obs *obs.Client) {
	router.Get("/", func(c *fiber.Ctx) error {
		return c.Status(200).SendString("Hello, World!")
	})

	orders := router.Group("/orders")
	orders.Post("/post", handler.PostOrder)
	orders.Post("/cancel", handler.CancelOrder)
	orders.Get("/:userId", handler.GetOpenOrders)

	fills := router.Group("/fills")
	fills.Get("/:userId", handler.GetFillsForUser)

	internal := router.Group("/internal")
	replicaRoutes := internal.Group("/replica")
	replicaRoutes.Post("/replicate", handler.ReplicateEntries)
	replicaRoutes.Get("/state", handler.GetReplicaState)
	replicaRoutes.Get("/sync", handler.GetReplicaSync)
}
