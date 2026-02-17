package handlers

import (
	"context"
	"errors"

	"replicated-clob/schemas"

	"github.com/gofiber/fiber/v2"
)

func (h *Handler) GetFillsForUser(c *fiber.Ctx) error {
	userID := c.Params("userId")
	if userID == "" {
		h.obs.LogErr(c.UserContext(), "fills.query: missing userId")
		return badRequest(c, errors.New("userId is required"))
	}

	ctx := c.UserContext()
	ctx = context.WithValue(ctx, "user", userID)
	h.obs.LogInfo(ctx, "fills.query: user=%s", userID)

	if err := h.ensureReplicaReadFreshness(ctx); err != nil {
		h.obs.LogErr(ctx, "fills.query: read freshness check failed: %v", err)
		return temporaryUnavailable(c, err)
	}

	resp := h.orderbook.FillsForUser(ctx, userID)

	fills := make([]schemas.Fill, 0, len(resp))
	for _, fill := range resp {
		fills = append(fills, schemas.Fill{
			Counterparty: fill.Counterparty,
			Size:         fill.Size,
			PriceLevel:   fill.PriceLevel,
			IsMaker:      fill.IsMaker,
		})
	}

	h.obs.LogInfo(ctx, "fills.query.done: user=%s count=%d", userID, len(fills))
	return jsonResponse(c, fiber.StatusOK, schemas.FillsResponse{
		Fills: fills,
	})
}
