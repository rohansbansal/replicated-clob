package handlers

import (
	"context"
	"errors"
	"fmt"

	"replicated-clob/pkg/replica"
	"replicated-clob/schemas"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

func (h *Handler) PostOrder(c *fiber.Ctx) error {
	var req schemas.PostLimitRequest
	ctx := c.UserContext()

	if err := c.BodyParser(&req); err != nil {
		h.obs.LogErr(ctx, "order.post: invalid request body (user=%s is_bid=%v)", req.User, req.IsBid)
		return badRequest(c, errors.New("invalid request body"))
	}
	if req.User == "" {
		h.obs.LogErr(ctx, "order.post: user missing")
		return badRequest(c, errors.New("user is required"))
	}
	if req.Amount <= 0 {
		h.obs.LogErr(ctx, "order.post: invalid amount user=%s amount=%d", req.User, req.Amount)
		return badRequest(c, errors.New("amount must be greater than 0"))
	}

	h.obs.LogInfo(ctx, "order.post: user=%s is_bid=%v price=%d amount=%d", req.User, req.IsBid, req.PriceLevel, req.Amount)

	h.replica.LockWritePipeline()
	defer h.replica.UnlockWritePipeline()

	orderId := uuid.New()
	replicaEntry := replica.ReplicationEntry{
		Seq:        h.replica.NextSequence(),
		OpID:       orderId.String(),
		Type:       replica.ReplicationWritePost,
		User:       req.User,
		OrderID:    orderId.String(),
		PriceLevel: req.PriceLevel,
		Amount:     req.Amount,
		IsBid:      req.IsBid,
	}

	// Prepare on primary and quorum peers before commit.
	if err := h.replication.PrepareEntry(ctx, replicaEntry); err != nil {
		h.obs.LogAlert(ctx, "order.post replication failed: seq=%d err=%v", replicaEntry.Seq, err)
		h.replica.RevertSequence(replicaEntry.Seq)
		return temporaryUnavailable(c, err)
	}

	if err := h.replication.CommitEntry(ctx, replicaEntry); err != nil {
		h.obs.LogAlert(ctx, "order.post commit replication failed: seq=%d err=%v", replicaEntry.Seq, err)
		h.replica.RevertSequence(replicaEntry.Seq)
		return temporaryUnavailable(c, err)
	}

	resp, err := h.applyPostReplication(ctx, replicaEntry)
	if err != nil {
		h.obs.LogErr(ctx, "order.post commit failed: seq=%d err=%v", replicaEntry.Seq, err)
		return internalServerError(c)
	}

	h.obs.LogInfo(ctx, "order.post done: user=%s fills=%d", req.User, len(resp.Fills))
	return jsonResponse(c, fiber.StatusOK, resp)
}

func (h *Handler) CancelOrder(c *fiber.Ctx) error {
	var req schemas.CancelLimitRequest
	ctx := c.UserContext()
	if err := c.BodyParser(&req); err != nil {
		h.obs.LogErr(ctx, "order.cancel: invalid request body")
		return badRequest(c, errors.New("invalid request body"))
	}
	if req.OrderID == "" {
		h.obs.LogErr(ctx, "order.cancel: order_id missing")
		return badRequest(c, errors.New("order_id is required"))
	}
	orderID, err := uuid.Parse(req.OrderID)
	if err != nil {
		h.obs.LogErr(ctx, "order.cancel: invalid order_id %q", req.OrderID)
		return badRequest(c, errors.New("order_id must be a UUID"))
	}
	if !h.orderbook.HasOrder(orderID) {
		h.obs.LogErr(ctx, "order.cancel failed: order_id=%s", req.OrderID)
		return notFound(c, errors.New("order not found"))
	}

	h.replica.LockWritePipeline()
	defer h.replica.UnlockWritePipeline()

	h.obs.LogInfo(ctx, "order.cancel: order_id=%s", req.OrderID)

	replicaEntry := replica.ReplicationEntry{
		Seq:     h.replica.NextSequence(),
		OpID:    req.OrderID,
		Type:    replica.ReplicationWriteCancel,
		OrderID: req.OrderID,
	}

	// Cancel validation happens before the local state change, and side effects are committed after quorum replication.
	if err := h.replication.PrepareEntry(ctx, replicaEntry); err != nil {
		h.obs.LogAlert(ctx, "order.cancel replication failed: seq=%d err=%v", replicaEntry.Seq, err)
		h.replica.RevertSequence(replicaEntry.Seq)
		return temporaryUnavailable(c, err)
	}
	if err := h.replication.CommitEntry(ctx, replicaEntry); err != nil {
		h.obs.LogAlert(ctx, "order.cancel commit replication failed: seq=%d err=%v", replicaEntry.Seq, err)
		h.replica.RevertSequence(replicaEntry.Seq)
		return temporaryUnavailable(c, err)
	}
	resp, err := h.applyCancelReplication(ctx, replicaEntry)
	if err != nil {
		h.obs.LogErr(ctx, "order.cancel commit failed: order_id=%s err=%v", req.OrderID, err)
		return internalServerError(c)
	}

	h.obs.LogInfo(ctx, "order.cancel done: order_id=%s size_cancelled=%d", req.OrderID, resp.SizeCancelled)
	return jsonResponse(c, fiber.StatusOK, resp)
}

func (h *Handler) GetOpenOrders(c *fiber.Ctx) error {
	userID := c.Params("userId")
	if userID == "" {
		h.obs.LogErr(c.UserContext(), "orders.query: missing userId")
		return badRequest(c, errors.New("userId is required"))
	}

	ctx := c.UserContext()
	ctx = context.WithValue(ctx, "user", userID)
	h.obs.LogInfo(ctx, "orders.query: user=%s", userID)

	if err := h.ensureReplicaReadFreshness(ctx); err != nil {
		h.obs.LogErr(ctx, "orders.query: read freshness check failed: %v", err)
		return temporaryUnavailable(c, err)
	}

	resp := h.orderbook.OpenOrdersForUser(ctx, userID)

	orders := make([]schemas.OpenOrder, 0, len(resp))
	for _, order := range resp {
		orders = append(orders, schemas.OpenOrder{
			User:       order.User,
			OrderID:    order.ID.String(),
			PriceLevel: order.PriceLevel,
			Amount:     order.Amount,
			IsBid:      order.IsBid,
		})
	}

	h.obs.LogInfo(ctx, "orders.query.done user=%s count=%d", userID, len(orders))
	return jsonResponse(c, fiber.StatusOK, schemas.OpenOrdersResponse{
		Orders: orders,
	})
}

func (h *Handler) applyPostReplication(ctx context.Context, entry replica.ReplicationEntry) (schemas.PostLimitResponse, error) {
	if entry.Type != replica.ReplicationWritePost {
		return schemas.PostLimitResponse{}, errors.New("replication entry is not post")
	}
	response := schemas.PostLimitResponse{
		OrderID: entry.OrderID,
	}
	seqApplied, err := h.replica.ApplyRemote(entry)
	if err != nil {
		return schemas.PostLimitResponse{}, err
	}
	if !seqApplied {
		return response, nil
	}

	if entry.User == "" {
		return schemas.PostLimitResponse{}, errors.New("replication entry missing user")
	}
	if entry.OrderID == "" {
		return schemas.PostLimitResponse{}, errors.New("replication entry missing orderId")
	}

	orderID, err := uuid.Parse(entry.OrderID)
	if err != nil {
		return schemas.PostLimitResponse{}, fmt.Errorf("replication entry invalid orderId: %w", err)
	}

	response = h.orderbook.PostLimit(
		ctx,
		entry.User,
		orderID,
		entry.PriceLevel,
		entry.Amount,
		entry.IsBid,
	)

	return response, nil
}

func (h *Handler) applyCancelReplication(ctx context.Context, entry replica.ReplicationEntry) (schemas.CancelLimitResponse, error) {
	response := schemas.CancelLimitResponse{}
	seqApplied, err := h.replica.ApplyRemote(entry)
	if err != nil {
		return schemas.CancelLimitResponse{}, err
	}
	if !seqApplied {
		return response, nil
	}

	orderID, err := uuid.Parse(entry.OrderID)
	if err != nil {
		return schemas.CancelLimitResponse{}, fmt.Errorf("replication entry invalid orderId: %w", err)
	}
	return h.orderbook.CancelLimitOrder(ctx, orderID)
}
