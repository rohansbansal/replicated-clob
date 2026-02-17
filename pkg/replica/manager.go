package replica

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"replicated-clob/pkg/obs"
	"strconv"
	"sync"
	"time"
)

type EntrySideEffect func(context.Context, ReplicationEntry) error

type ReplicationManager struct {
	coordinator *Coordinator
	obs         *obs.Client
	client      *http.Client
	timeout     time.Duration
}

func NewReplicationManager(c *Coordinator, obs *obs.Client) *ReplicationManager {
	return &ReplicationManager{
		coordinator: c,
		obs:         obs,
		client:      &http.Client{},
		timeout:     1 * time.Second,
	}
}

// PrepareEntry reserves a sequence, validates local ordering, and replicates prepare to peers.
func (m *ReplicationManager) PrepareEntry(ctx context.Context, entry ReplicationEntry) error {
	prepared, err := m.coordinator.PrepareRemote(entry)
	if err != nil {
		return err
	}

	if err := m.replicateEntries(ctx, []ReplicationEntry{entry}, "/prepare"); err != nil {
		if prepared {
			m.coordinator.RevertSequence(entry.Seq)
		}
		return err
	}

	return nil
}

// CommitEntry replicates commit intent to peers. The caller applies local side effects
// after successful quorum replication.
func (m *ReplicationManager) CommitEntry(ctx context.Context, entry ReplicationEntry) error {
	return m.replicateEntries(ctx, []ReplicationEntry{entry}, "/commit")
}

// ApplyRemoteEntry validates committed ordering in the coordinator and applies side effects if provided.
func (m *ReplicationManager) ApplyRemoteEntry(
	ctx context.Context,
	entry ReplicationEntry,
	onApply EntrySideEffect,
) (bool, error) {
	applied, err := m.coordinator.CommitRemote(entry)
	if err != nil {
		return false, err
	}
	if !applied {
		return false, nil
	}
	if onApply == nil {
		return true, nil
	}

	return true, onApply(ctx, entry)
}

// PrepareRemoteEntry applies prepare on this replica only.
func (m *ReplicationManager) PrepareRemoteEntry(entry ReplicationEntry) (bool, error) {
	return m.coordinator.PrepareRemote(entry)
}

// ReplicateEntries is a compatibility shim for existing callers; it runs prepare semantics.
func (m *ReplicationManager) ReplicateEntries(ctx context.Context, entries []ReplicationEntry) error {
	for _, entry := range entries {
		if err := m.PrepareEntry(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

// GetReadRepairEntries returns entries needed to repair local state before serving a read.
func (m *ReplicationManager) GetReadRepairEntries(ctx context.Context) ([]ReplicationEntry, error) {
	m.obs.LogInfo(ctx, "replica.read: starting freshness check")
	requiredAcks := m.coordinator.RequiredPeerAcks()
	if requiredAcks <= 0 {
		m.obs.LogInfo(ctx, "replica.read: quorum not required (single node)")
		return nil, nil
	}

	peers := m.coordinator.Peers()

	m.obs.LogInfo(ctx, "replica.read: starting freshness check required=%d peer_count=%d", requiredAcks, len(peers))

	timeoutCtx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	localApplied := m.coordinator.GetAppliedSeq()
	highestSeq := localApplied
	highestPeer := ""
	successes := 0

	for _, peer := range peers {
		state, ok := m.getReplicaState(timeoutCtx, peer)
		if !ok {
			continue
		}
		successes++
		m.obs.LogInfo(ctx, "replica.read: peer_state peer=%s applied=%d", peer, state.AppliedSeq)
		if state.AppliedSeq > highestSeq {
			highestSeq = state.AppliedSeq
			highestPeer = peer
		}
	}
	if successes < requiredAcks {
		m.obs.LogErr(ctx, "replica.read: state quorum not met required=%d got=%d", requiredAcks, successes)
		return nil, fmt.Errorf("replica read quorum not met: required=%d got=%d", requiredAcks, successes)
	}
	if highestSeq <= localApplied || highestPeer == "" {
		m.obs.LogInfo(ctx, "replica.read: local state is fresh enough local_seq=%d", localApplied)
		return nil, nil
	}

	m.obs.LogInfo(
		ctx,
		"replica.read: read repair needed local_seq=%d highest_peer=%s highest_seq=%d",
		localApplied,
		highestPeer,
		highestSeq,
	)

	entries, err := m.getReplicaSync(timeoutCtx, highestPeer, localApplied)
	if err != nil {
		return nil, err
	}
	m.obs.LogInfo(ctx, "replica.read: sync fetched %d entries from peer=%s", len(entries), highestPeer)

	return entries, nil
}

func (m *ReplicationManager) replicateEntries(ctx context.Context, entries []ReplicationEntry, phase string) error {
	requiredAcks := m.coordinator.RequiredPeerAcks()
	if requiredAcks <= 0 {
		return nil
	}

	peers := m.coordinator.Peers()
	request := ReplicationRequest{Entries: entries}
	payload, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal replication payload: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0

	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			if m.postReplicationRequest(timeoutCtx, p, phase, payload) {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	if successes >= requiredAcks {
		return nil
	}

	return fmt.Errorf("replication %s quorum not met: required=%d got=%d", phase, requiredAcks, successes)
}

func (m *ReplicationManager) getReplicaSync(ctx context.Context, peer string, since int64) ([]ReplicationEntry, error) {
	requestPath := "/sync?since=" + strconv.FormatInt(since, 10)
	var response ReplicaSyncResponse

	if err := m.doReplicaRequest(ctx, http.MethodGet, peer, requestPath, nil, &response); err != nil {
		return nil, err
	}

	return response.Entries, nil
}

func (m *ReplicationManager) getReplicaState(ctx context.Context, peer string) (ReplicaStateResponse, bool) {
	var state ReplicaStateResponse
	if err := m.doReplicaRequest(ctx, http.MethodGet, peer, "/state", nil, &state); err != nil {
		m.obs.LogErr(ctx, "replica.read: peer state request failed peer=%s err=%v", peer, err)
		return ReplicaStateResponse{}, false
	}
	return state, true
}

// postReplicationRequest sends a replica control request to a peer.
func (m *ReplicationManager) postReplicationRequest(
	ctx context.Context,
	peer string,
	phase string,
	payload []byte,
) bool {
	var response ReplicationResponse
	if err := m.doReplicaRequest(ctx, http.MethodPost, peer, phase, payload, &response); err != nil {
		m.obs.LogErr(ctx, "replica.replicate.request: peer=%s phase=%s err=%v", peer, phase, err)
		return false
	}
	if !response.Accepted {
		m.obs.LogErr(ctx, "replica.replicate.request: peer rejected seq=%d", response.LastSeq)
		return false
	}

	return true
}

func (m *ReplicationManager) doReplicaRequest(
	ctx context.Context,
	method string,
	peer string,
	path string,
	payload []byte,
	response any,
) error {
	url := peer + "/internal/replica" + path
	var body io.Reader
	if len(payload) > 0 {
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("build replica request failed peer=%s path=%s err=%w", peer, path, err)
	}
	if len(payload) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	m.setRequestIDHeader(req, ctx)

	res, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("replica request failed peer=%s path=%s err=%w", peer, path, err)
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read replica response failed peer=%s path=%s err=%w", peer, path, err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"replica response rejected peer=%s path=%s status=%d body=%s",
			peer,
			path,
			res.StatusCode,
			string(bodyBytes),
		)
	}

	if response == nil {
		return nil
	}
	if err := json.Unmarshal(bodyBytes, response); err != nil {
		return fmt.Errorf("invalid replica response peer=%s path=%s err=%w", peer, path, err)
	}

	return nil
}

func (m *ReplicationManager) setRequestIDHeader(req *http.Request, ctx context.Context) {
	requestID, ok := ctx.Value(RequestIDContextKey).(string)
	if !ok || requestID == "" {
		return
	}
	req.Header.Set(RequestIDHeader, requestID)
}
