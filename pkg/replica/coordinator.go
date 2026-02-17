package replica

import (
	"sync"
	"time"
)

type Coordinator struct {
	role            NodeRole
	primary         string
	peers           []string
	nextSeq         int64
	preparedSeq     int64
	applied         int64
	log             map[int64]ReplicationEntry
	prepared        map[int64]ReplicationEntry
	preparedAt      map[int64]time.Time
	writePipelineMu sync.Mutex
	mu              sync.RWMutex
	prepareTimeout  time.Duration
}

func NewCoordinator(role NodeRole, peers []string, primary string) *Coordinator {
	coordinator := &Coordinator{
		role:           role,
		log:            map[int64]ReplicationEntry{},
		prepared:       map[int64]ReplicationEntry{},
		preparedAt:     map[int64]time.Time{},
		preparedSeq:    0,
		applied:        0,
		nextSeq:        0,
		prepareTimeout: 5 * time.Second,
		primary:        primary,
	}
	coordinator.SetPeers(peers)
	return coordinator
}

func (c *Coordinator) CanAcceptWrite() bool {
	return c.role.IsPrimary()
}

func (c *Coordinator) LockWritePipeline() {
	c.writePipelineMu.Lock()
}

func (c *Coordinator) UnlockWritePipeline() {
	c.writePipelineMu.Unlock()
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
