package replica

import (
	"strings"
	"testing"
	"time"
)

func testReplicationEntry(seq int64, orderID string) ReplicationEntry {
	return ReplicationEntry{
		Seq:        seq,
		OpID:       "op-" + orderID,
		Type:       ReplicationWritePost,
		User:       "alice",
		OrderID:    orderID,
		PriceLevel: 101,
		Amount:     5,
		IsBid:      true,
	}
}

func TestPrepareCommitOrdering(t *testing.T) {
	coordinator := NewCoordinator(NodeRolePrimary, []string{"peer1", "peer2"}, "test-cluster")

	entry1 := testReplicationEntry(1, "ord-1")
	if prepared, err := coordinator.PrepareRemote(entry1); err != nil {
		t.Fatalf("prepare entry1 unexpected error: %v", err)
	} else if !prepared {
		t.Fatalf("prepare entry1 should reserve new sequence")
	}

	if committed, err := coordinator.CommitRemote(entry1); err != nil {
		t.Fatalf("commit entry1 unexpected error: %v", err)
	} else if !committed {
		t.Fatalf("commit entry1 should apply")
	}

	entry3 := testReplicationEntry(3, "ord-3")
	if _, err := coordinator.PrepareRemote(entry3); err == nil {
		t.Fatalf("expected gap error for prepare seq3, got nil")
	} else if !strings.Contains(err.Error(), "expected 2") {
		t.Fatalf("expected sequence gap for next seq=2, got %v", err)
	}

	entry2 := testReplicationEntry(2, "ord-2")
	if _, err := coordinator.PrepareRemote(entry2); err != nil {
		t.Fatalf("prepare entry2 unexpected error: %v", err)
	}
	if _, err := coordinator.CommitRemote(entry2); err != nil {
		t.Fatalf("commit entry2 unexpected error: %v", err)
	}
}

func TestPrepareDuplicateSuppression(t *testing.T) {
	coordinator := NewCoordinator(NodeRolePrimary, []string{"peer1", "peer2"}, "test-cluster")

	entry := testReplicationEntry(1, "ord-dup")
	if prepared, err := coordinator.PrepareRemote(entry); err != nil {
		t.Fatalf("prepare unexpected error: %v", err)
	} else if !prepared {
		t.Fatalf("prepare should reserve sequence")
	}

	if prepared, err := coordinator.PrepareRemote(entry); err != nil {
		t.Fatalf("duplicate prepare unexpected error: %v", err)
	} else if prepared {
		t.Fatalf("duplicate prepare should not reapply sequence")
	}

	badEntry := testReplicationEntry(1, "ord-dup-different")
	badEntry.User = "bob"
	if _, err := coordinator.PrepareRemote(badEntry); err == nil {
		t.Fatalf("expected mismatch error for duplicate seq with different payload")
	}
}

func TestPrepareTimeoutThenReplay(t *testing.T) {
	now := time.Now()
	coordinator := NewCoordinator(NodeRolePrimary, []string{"peer1", "peer2"}, "test-cluster")
	coordinator.SetPrepareTimeout(20 * time.Millisecond)

	entry := testReplicationEntry(1, "ord-timeout")
	if prepared, err := coordinator.PrepareRemote(entry); err != nil {
		t.Fatalf("prepare unexpected error: %v", err)
	} else if !prepared {
		t.Fatalf("prepare should reserve sequence")
	}

	// advance time past timeout and ensure commit is rejected because prepare is expired
	now = now.Add(25 * time.Millisecond)
	if _, err := coordinator.CommitRemote(entry); err == nil {
		t.Fatalf("expected commit to fail for expired prepare")
	}

	// same sequence can be prepared again after prepare lease expires.
	if prepared, err := coordinator.PrepareRemote(entry); err != nil {
		t.Fatalf("prepare replay unexpected error: %v", err)
	} else if !prepared {
		t.Fatalf("prepare replay should reserve sequence after timeout")
	}

	if _, err := coordinator.CommitRemote(entry); err != nil {
		t.Fatalf("commit after prepare replay unexpected error: %v", err)
	}
}
