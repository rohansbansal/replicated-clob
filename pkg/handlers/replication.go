package handlers

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"replicated-clob/pkg/replica"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

func (h *Handler) CommitEntries(c *fiber.Ctx) error {
	var req replica.ReplicationRequest
	if err := c.BodyParser(&req); err != nil {
		h.obs.LogErr(c.UserContext(), "replica.commit: invalid request body: %v", err)
		return badRequest(c, errors.New("invalid request body"))
	}
	if len(req.Entries) == 0 {
		return badRequest(c, errors.New("entries are required"))
	}

	ctx := c.UserContext()
	for _, entry := range req.Entries {
		slotApplied, err := h.replication.ApplyRemoteEntry(ctx, entry, h.applyReplicationSideEffect)
		if err != nil {
			var gapErr *replica.SequenceGapError
			if errors.As(err, &gapErr) {
				h.obs.LogErr(ctx, "replica.commit: sequence gap required=%d received=%d", gapErr.Expected, gapErr.Received)
				return jsonResponse(c, fiber.StatusConflict, fiber.Map{
					"error":    "replication sequence gap",
					"required": gapErr.Expected,
					"received": gapErr.Received,
				})
			}

			h.obs.LogErr(ctx, "replica.commit: invalid entry seq=%d err=%v", entry.Seq, err)
			return badRequest(c, err)
		}
		if slotApplied {
			h.obs.LogInfo(ctx, "replica.commit: applied seq=%d type=%s", entry.Seq, entry.Type)
		}
	}

	return jsonResponse(c, fiber.StatusOK, replica.ReplicationResponse{
		Accepted: true,
		LastSeq:  h.replica.GetAppliedSeq(),
	})
}

func (h *Handler) GetReplicaState(c *fiber.Ctx) error {
	return jsonResponse(c, fiber.StatusOK, h.replica.State())
}

func (h *Handler) GetReplicaSync(c *fiber.Ctx) error {
	since := c.Query("since", "0")
	from, err := strconv.ParseInt(since, 10, 64)
	if err != nil {
		return badRequest(c, errors.New("invalid since query param"))
	}

	return jsonResponse(c, fiber.StatusOK, replica.ReplicaSyncResponse{
		Entries: h.replica.EntriesSince(from),
	})
}

func (h *Handler) ensureReplicaReadFreshness(ctx context.Context) error {
	entries, err := h.replication.GetReadRepairEntries(ctx)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	h.replica.LockWritePipeline()
	defer h.replica.UnlockWritePipeline()

	applied := 0
	for _, entry := range entries {
		if _, err := h.replication.PrepareRemoteEntry(entry); err != nil {
			return err
		}
		appliedEntry, err := h.replication.ApplyRemoteEntry(ctx, entry, h.applyReplicationSideEffect)
		if err != nil {
			return err
		}
		if appliedEntry {
			applied++
		}
	}
	h.obs.LogInfo(ctx, "replica.read: sync replay complete applied=%d local_seq=%d", applied, h.replica.GetAppliedSeq())
	return nil
}

func (h *Handler) PrepareEntries(c *fiber.Ctx) error {
	var req replica.ReplicationRequest
	if err := c.BodyParser(&req); err != nil {
		h.obs.LogErr(c.UserContext(), "replica.prepare: invalid request body: %v", err)
		return badRequest(c, errors.New("invalid request body"))
	}
	if len(req.Entries) == 0 {
		return badRequest(c, errors.New("entries are required"))
	}

	ctx := c.UserContext()
	for _, entry := range req.Entries {
		slotPrepared, err := h.replication.PrepareRemoteEntry(entry)
		if err != nil {
			var gapErr *replica.SequenceGapError
			if errors.As(err, &gapErr) {
				h.obs.LogErr(ctx, "replica.prepare: sequence gap required=%d received=%d", gapErr.Expected, gapErr.Received)
				return jsonResponse(c, fiber.StatusConflict, fiber.Map{
					"error":    "replication sequence gap",
					"required": gapErr.Expected,
					"received": gapErr.Received,
				})
			}

			h.obs.LogErr(ctx, "replica.prepare: invalid entry seq=%d err=%v", entry.Seq, err)
			return badRequest(c, err)
		}
		if slotPrepared {
			h.obs.LogInfo(ctx, "replica.prepare: stored seq=%d type=%s", entry.Seq, entry.Type)
		}
	}

	return jsonResponse(c, fiber.StatusOK, replica.ReplicationResponse{
		Accepted: true,
		LastSeq:  h.replica.GetAppliedSeq(),
	})
}

func (h *Handler) applyReplicationSideEffect(ctx context.Context, entry replica.ReplicationEntry) error {
	switch entry.Type {
	case replica.ReplicationWritePost:
		orderID, err := uuid.Parse(entry.OrderID)
		if err != nil {
			return fmt.Errorf("replication entry invalid orderId: %w", err)
		}
		h.orderbook.PostLimit(
			ctx,
			entry.User,
			orderID,
			entry.PriceLevel,
			entry.Amount,
			entry.IsBid,
		)
		return nil
	case replica.ReplicationWriteCancel:
		orderID, err := uuid.Parse(entry.OrderID)
		if err != nil {
			return fmt.Errorf("replication entry invalid orderId: %w", err)
		}
		_, err = h.orderbook.CancelLimitOrder(ctx, orderID)
		return err
	default:
		return fmt.Errorf("unsupported replication entry type: %s", entry.Type)
	}
}
