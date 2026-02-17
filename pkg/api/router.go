package api

import (
	"replicated-clob/pkg/handlers"
	"replicated-clob/pkg/obs"

	"github.com/gofiber/fiber/v2"
)

func New(router fiber.Router, handler *handlers.Handler, obs *obs.Client) {
	router.Use(requestIDMiddleware)

	// should use middleware to pull user_id instead of in request
	orders := router.Group("/orders")
	writeOrders := orders.Group("")
	writeOrders.Post("/post", handler.RequireWriteAccess(), handler.PostOrder)
	writeOrders.Post("/cancel", handler.RequireWriteAccess(), handler.CancelOrder)
	orders.Get("/:userId", handler.GetOpenOrders)

	fills := router.Group("/fills")
	fills.Get("/:userId", handler.GetFillsForUser)

	// should block requests outside of this cluster + have some secret key for this
	internal := router.Group("/internal")
	replicaRoutes := internal.Group("/replica")
	replicaRoutes.Post("/prepare", handler.PrepareEntries)
	replicaRoutes.Post("/commit", handler.CommitEntries)
	replicaRoutes.Get("/state", handler.GetReplicaState)
	replicaRoutes.Get("/sync", handler.GetReplicaSync)
}
