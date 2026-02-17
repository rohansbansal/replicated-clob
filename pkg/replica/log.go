package replica

import (
	"fmt"
	"sort"
	"time"
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

func (c *Coordinator) SetPrepareTimeout(timeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.prepareTimeout = timeout
}

// PrepareRemote stores an entry as prepared and enforces strict sequence order.
//
// - Returns (false, nil) for duplicate prepares that are already known.
// - Returns (true, nil) when this entry becomes the next prepared sequence.
// - Returns SequenceGapError when entries arrive out of order or with gaps.
func (c *Coordinator) PrepareRemote(entry ReplicationEntry) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.expirePreparedLocked()
	if entry.Seq <= c.applied {
		return false, nil
	}

	expected := c.applied + 1
	if existing, ok := c.prepared[entry.Seq]; ok {
		if replicationEntriesEqual(existing, entry) {
			return false, nil
		}
		return false, fmt.Errorf("prepare entry mismatch at seq=%d", entry.Seq)
	}

	if entry.Seq != expected {
		return false, &SequenceGapError{Expected: expected, Received: entry.Seq}
	}

	c.prepared[entry.Seq] = entry
	c.preparedAt[entry.Seq] = time.Now()
	c.preparedSeq = entry.Seq
	return true, nil
}

// CommitRemote moves a prepared entry into committed state.
//
// - Returns (false, nil) for duplicate commits already applied.
// - Returns (true, nil) when this entry is committed in sequence.
// - Returns SequenceGapError when commits arrive out of order or with gaps.
func (c *Coordinator) CommitRemote(entry ReplicationEntry) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.expirePreparedLocked()
	if entry.Seq <= c.applied {
		return false, nil
	}

	expected := c.applied + 1
	if entry.Seq != expected {
		return false, &SequenceGapError{Expected: expected, Received: entry.Seq}
	}

	preparedEntry, ok := c.prepared[entry.Seq]
	if !ok {
		return false, fmt.Errorf("cannot commit seq=%d before prepare", entry.Seq)
	}
	if !replicationEntriesEqual(preparedEntry, entry) {
		return false, fmt.Errorf("commit entry mismatch for seq=%d", entry.Seq)
	}

	c.log[entry.Seq] = entry
	delete(c.prepared, entry.Seq)
	delete(c.preparedAt, entry.Seq)
	c.applied = entry.Seq
	if entry.Seq > c.nextSeq {
		c.nextSeq = entry.Seq
	}
	if c.preparedSeq == entry.Seq {
		c.preparedSeq = c.highestPreparedSeqLocked()
	}

	return true, nil
}

// ApplyRemote remains for compatibility with existing call sites and applies entries
// to committed state.
func (c *Coordinator) ApplyRemote(entry ReplicationEntry) (bool, error) {
	return c.CommitRemote(entry)
}

func (c *Coordinator) RevertSequence(seq int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if seq <= 0 || seq > c.nextSeq {
		return
	}
	if c.applied >= seq {
		return
	}

	// Revert only the most recent uncommitted local reservation.
	delete(c.prepared, seq)
	delete(c.preparedAt, seq)
	if c.preparedSeq == seq {
		c.preparedSeq = c.highestPreparedSeqLocked()
	}
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

func (c *Coordinator) expirePreparedLocked() {
	if c.prepareTimeout <= 0 || len(c.prepared) == 0 {
		return
	}
	now := time.Now()
	cutoff := now.Add(-c.prepareTimeout)
	for key, at := range c.preparedAt {
		if at.Before(cutoff) || at.Equal(cutoff) {
			delete(c.prepared, key)
			delete(c.preparedAt, key)
		}
	}
	c.preparedSeq = c.highestPreparedSeqLocked()
}

func (c *Coordinator) highestPreparedSeqLocked() int64 {
	highest := int64(0)
	for seq := range c.prepared {
		if seq > highest {
			highest = seq
		}
	}
	return highest
}

func replicationEntriesEqual(a, b ReplicationEntry) bool {
	return a.Seq == b.Seq &&
		a.OpID == b.OpID &&
		a.Type == b.Type &&
		a.User == b.User &&
		a.OrderID == b.OrderID &&
		a.PriceLevel == b.PriceLevel &&
		a.Amount == b.Amount &&
		a.IsBid == b.IsBid
}
