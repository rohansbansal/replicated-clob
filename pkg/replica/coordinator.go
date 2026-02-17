package replica

import "sync"

type Coordinator struct {
	role    NodeRole
	primary string
	peers   []string
	nextSeq int64
	applied int64
	log     map[int64]ReplicationEntry
	writeMu sync.Mutex
	mu      sync.RWMutex
}

func NewCoordinator(role NodeRole) *Coordinator {
	return &Coordinator{
		role:    role,
		log:     map[int64]ReplicationEntry{},
		applied: 0,
		nextSeq: 0,
	}
}

func (c *Coordinator) CanAcceptWrite() bool {
	return c.role.IsPrimary()
}

func (c *Coordinator) LockReplicationLog() {
	c.writeMu.Lock()
}

func (c *Coordinator) UnlockReplicationLog() {
	c.writeMu.Unlock()
}

func (c *Coordinator) Role() NodeRole {
	return c.role
}

func (c *Coordinator) RequiredPeerAcks() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := len(c.peers) + 1
	if total <= 1 {
		return 0
	}
	return total/2 + 1 - 1
}
