package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"replicated-clob/pkg/replica"
)

func (h *Handler) ReplicateEntries(c *fiber.Ctx) error {
	var req replica.ReplicationRequest
	if err := c.BodyParser(&req); err != nil {
		h.obs.LogErr(c.UserContext(), "replica.replicate: invalid request body: %v", err)
		return badRequest(c, errors.New("invalid request body"))
	}
	if len(req.Entries) == 0 {
		return badRequest(c, errors.New("entries are required"))
	}

	ctx := c.UserContext()
	for _, entry := range req.Entries {
		slotApplied, err := h.applyReplicationEntry(ctx, entry)
		if err != nil {
			var gapErr *replica.SequenceGapError
			if errors.As(err, &gapErr) {
				h.obs.LogErr(ctx, "replica.replicate: sequence gap required=%d received=%d", gapErr.Expected, gapErr.Received)
				return jsonResponse(c, fiber.StatusConflict, fiber.Map{
					"error":    "replication sequence gap",
					"required": gapErr.Expected,
					"received": gapErr.Received,
				})
			}

			h.obs.LogErr(ctx, "replica.replicate: invalid entry seq=%d err=%v", entry.Seq, err)
			return badRequest(c, err)
		}

		// slotApplied is false when this sequence was already applied earlier (duplicate replay).
		if slotApplied {
			h.obs.LogInfo(ctx, "replica.replicate: applied seq=%d type=%s", entry.Seq, entry.Type)
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
	// Read repair: choose the highest-applied peer after quorum state checks,
	// then catch up locally from that peer before serving the query.
	h.obs.LogInfo(ctx, "replica.read: starting freshness check")
	requiredAcks := h.replica.RequiredPeerAcks()
	if requiredAcks <= 0 {
		h.obs.LogInfo(ctx, "replica.read: quorum not required (single node)")
		return nil
	}

	peers := h.replica.Peers()
	if len(peers) == 0 {
		h.obs.LogInfo(ctx, "replica.read: no peers configured, skipping freshness check")
		return nil
	}

	h.obs.LogInfo(ctx, "replica.read: starting freshness check required=%d peer_count=%d", requiredAcks, len(peers))

	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	localApplied := h.replica.GetAppliedSeq()
	highestSeq := localApplied
	highestPeer := ""
	successes := 0

	for _, peer := range peers {
		state, ok := h.getReplicaState(timeoutCtx, peer)
		if !ok {
			continue
		}
		successes++
		h.obs.LogInfo(ctx, "replica.read: peer_state peer=%s applied=%d", peer, state.AppliedSeq)
		if state.AppliedSeq > highestSeq {
			highestSeq = state.AppliedSeq
			highestPeer = peer
		}
	}
	if successes < requiredAcks {
		h.obs.LogErr(ctx, "replica.read: state quorum not met required=%d got=%d", requiredAcks, successes)
		return fmt.Errorf("replica read quorum not met: required=%d got=%d", requiredAcks, successes)
	}
	if highestSeq <= localApplied || highestPeer == "" {
		h.obs.LogInfo(ctx, "replica.read: local state is fresh enough local_seq=%d", localApplied)
		return nil
	}

	h.obs.LogInfo(
		ctx,
		"replica.read: read repair needed local_seq=%d highest_peer=%s highest_seq=%d",
		localApplied,
		highestPeer,
		highestSeq,
	)

	entries, err := h.getReplicaSync(timeoutCtx, highestPeer, localApplied)
	if err != nil {
		return err
	}
	h.obs.LogInfo(ctx, "replica.read: sync fetched %d entries from peer=%s", len(entries), highestPeer)
	h.replica.LockReplicationLog()
	defer h.replica.UnlockReplicationLog()

	applied := 0
	for _, entry := range entries {
		appliedEntry, err := h.applyReplicationEntry(timeoutCtx, entry)
		if appliedEntry {
			applied++
		}
		if err != nil {
			return err
		}
	}
	h.obs.LogInfo(ctx, "replica.read: sync replay complete applied=%d local_seq=%d", applied, h.replica.GetAppliedSeq())
	return nil
}

// replicateEntries runs the prepare/commit pipeline for an outbound write batch.
// The coordinator only marks the sequence as committed when a peer responds with success.
func (h *Handler) replicateEntries(ctx context.Context, entries []replica.ReplicationEntry) error {
	requiredAcks := h.replica.RequiredPeerAcks()
	if requiredAcks <= 0 {
		return nil
	}

	peers := h.replica.Peers()
	if len(peers) == 0 {
		return nil
	}

	req := replica.ReplicationRequest{Entries: entries}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal replication payload: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	successes := 0
	for _, peer := range peers {
		// Send the request to each peer; each peer will only apply an entry once.
		if h.postReplicationRequest(timeoutCtx, peer, payload) {
			successes++
			if successes >= requiredAcks {
				return nil
			}
		}
	}

	return fmt.Errorf("replication quorum not met: required=%d got=%d", requiredAcks, successes)
}

func (h *Handler) getReplicaSync(ctx context.Context, peer string, since int64) ([]replica.ReplicationEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peer+"/internal/replica/sync?since="+strconv.FormatInt(since, 10), nil)
	if err != nil {
		return nil, fmt.Errorf("build sync request failed peer=%s err=%w", peer, err)
	}

	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read sync request failed peer=%s err=%w", peer, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(res.Body)
		if readErr == nil {
			h.obs.LogErr(ctx, "replica.read: peer sync rejected peer=%s status=%d body=%s", peer, res.StatusCode, string(body))
		}
		return nil, fmt.Errorf("peer sync rejected peer=%s status=%d", peer, res.StatusCode)
	}

	var response replica.ReplicaSyncResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("invalid sync response peer=%s err=%w", peer, err)
	}
	return response.Entries, nil
}

func (h *Handler) getReplicaState(ctx context.Context, peer string) (replica.ReplicaStateResponse, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peer+"/internal/replica/state", nil)
	if err != nil {
		h.obs.LogErr(ctx, "replica.read: invalid state request peer=%s err=%v", peer, err)
		return replica.ReplicaStateResponse{}, false
	}

	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		h.obs.LogErr(ctx, "replica.read: peer state request failed peer=%s err=%v", peer, err)
		return replica.ReplicaStateResponse{}, false
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		h.obs.LogErr(ctx, "replica.read: peer state returned peer=%s status=%d", peer, res.StatusCode)
		return replica.ReplicaStateResponse{}, false
	}

	var state replica.ReplicaStateResponse
	if err := json.NewDecoder(res.Body).Decode(&state); err != nil {
		h.obs.LogErr(ctx, "replica.read: invalid peer state response peer=%s err=%v", peer, err)
		return replica.ReplicaStateResponse{}, false
	}
	return state, true
}

// postReplicationRequest sends a prepared+committed entry batch to a peer.
// It intentionally returns only a boolean because each request is treated as one vote for quorum.
func (h *Handler) postReplicationRequest(ctx context.Context, peer string, payload []byte) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peer+"/internal/replica/replicate", bytes.NewReader(payload))
	if err != nil {
		h.obs.LogErr(ctx, "replica.replicate.request: build request failed peer=%s err=%v", peer, err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		h.obs.LogErr(ctx, "replica.replicate.request: peer failed peer=%s err=%v", peer, err)
		return false
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(res.Body)
		if readErr == nil {
			h.obs.LogErr(ctx, "replica.replicate.request: peer=%s status=%d body=%s", peer, res.StatusCode, string(body))
		} else {
			h.obs.LogErr(ctx, "replica.replicate.request: peer=%s status=%d body read error=%v", peer, res.StatusCode, readErr)
		}
		return false
	}

	var response replica.ReplicationResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		h.obs.LogErr(ctx, "replica.replicate.request: invalid peer response peer=%s err=%v", peer, err)
		return false
	}
	if !response.Accepted {
		h.obs.LogErr(ctx, "replica.replicate.request: peer rejected seq=%d", response.LastSeq)
		return false
	}

	return true
}

// applyReplicationEntry validates sequence ordering and applies idempotent side effects.
// The boolean return indicates whether this entry was newly applied (true) or skipped as duplicate (false).
func (h *Handler) applyReplicationEntry(ctx context.Context, entry replica.ReplicationEntry) (bool, error) {
	applied, err := h.replica.ApplyRemote(entry)
	if err != nil {
		return false, err
	}
	if !applied {
		return false, nil
	}

	switch entry.Type {
	case replica.ReplicationWritePost:
		if entry.User == "" {
			return false, errors.New("replication entry missing user")
		}
		if entry.OrderID == "" {
			return false, errors.New("replication entry missing orderId")
		}

		orderID, err := uuid.Parse(entry.OrderID)
		if err != nil {
			return false, fmt.Errorf("replication entry invalid orderId: %w", err)
		}
		h.orderbook.PostLimit(
			ctx,
			entry.User,
			orderID,
			entry.PriceLevel,
			entry.Amount,
			entry.IsBid,
		)
		return true, nil
	case replica.ReplicationWriteCancel:
		if entry.OrderID == "" {
			return false, errors.New("replication entry missing orderId")
		}

		orderID, err := uuid.Parse(entry.OrderID)
		if err != nil {
			return false, fmt.Errorf("replication entry invalid orderId: %w", err)
		}
		_, err = h.orderbook.CancelLimitOrder(ctx, orderID)
		if err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, fmt.Errorf("unsupported replication entry type: %s", entry.Type)
	}
}
