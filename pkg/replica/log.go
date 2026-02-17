package replica

import (
	"errors"
	"fmt"
	"sort"
)

type SequenceGapError struct {
	Expected int64
	Received int64
}

func (e *SequenceGapError) Error() string {
	return fmt.Sprintf("replication sequence gap: expected %d got %d", e.Expected, e.Received)
}

func (c *Coordinator) NextSequence() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextSeq++
	return c.nextSeq
}

// ApplyRemote validates and applies one replicated entry.
//
// - Returns (false, nil) for duplicate entries that have already been applied.
// - Returns (true, nil) when this entry is exactly the next expected sequence.
// - Returns SequenceGapError when entries arrive out of order or with gaps.
// - Returns an error for nil coordinator usage.
func (c *Coordinator) ApplyRemote(entry ReplicationEntry) (bool, error) {
	if c == nil {
		return false, errors.New("nil coordinator")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry.Seq <= c.applied {
		return false, nil
	}

	expected := c.applied + 1
	if entry.Seq != expected {
		return false, &SequenceGapError{Expected: expected, Received: entry.Seq}
	}

	c.log[entry.Seq] = entry
	c.applied = entry.Seq
	if entry.Seq > c.nextSeq {
		c.nextSeq = entry.Seq
	}

	return true, nil
}

func (c *Coordinator) RevertSequence(seq int64) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if seq <= 0 || seq > c.nextSeq {
		return
	}
	if seq < c.nextSeq {
		return
	}

	// Revert only the most recent uncommitted local reservation.
	delete(c.log, seq)
	c.nextSeq = seq - 1
}

func (c *Coordinator) State() ReplicaStateResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return ReplicaStateResponse{
		Role:       c.role,
		LastSeq:    c.nextSeq,
		AppliedSeq: c.applied,
		PeerCount:  len(c.peers),
		Primary:    c.primary,
	}
}

func (c *Coordinator) GetAppliedSeq() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.applied
}

func (c *Coordinator) EntriesSince(seq int64) []ReplicationEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]int64, 0, len(c.log))
	for key := range c.log {
		if key > seq {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	entries := make([]ReplicationEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, c.log[key])
	}

	return entries
}
