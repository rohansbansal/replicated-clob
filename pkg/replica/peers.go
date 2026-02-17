package replica

import "strings"

func (c *Coordinator) SetPeers(peers []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.peers = c.normalizePeerURLs(peers)
}

func (c *Coordinator) SetPrimary(primary string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.primary = strings.TrimRight(strings.TrimSpace(primary), "/")
}

func (c *Coordinator) Peers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	peers := make([]string, len(c.peers))
	copy(peers, c.peers)
	return peers
}

func (c *Coordinator) Primary() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.primary
}

func (c *Coordinator) normalizePeerURLs(peers []string) []string {
	normalized := make([]string, 0, len(peers))
	seen := map[string]struct{}{}
	for _, peer := range peers {
		trimmed := strings.TrimSpace(strings.TrimSuffix(peer, "/"))
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	return normalized
}
