package replica

type NodeRole string

const (
	NodeRolePrimary   NodeRole = "primary"
	NodeRoleSecondary NodeRole = "secondary"
)

func (r NodeRole) IsPrimary() bool {
	return r == NodeRolePrimary
}

type ReplicationWriteType string

const (
	ReplicationWritePost   ReplicationWriteType = "post_limit"
	ReplicationWriteCancel ReplicationWriteType = "cancel_limit"
)

type ReplicationEntry struct {
	Seq        int64                `json:"seq"`
	OpID       string               `json:"opId"`
	Type       ReplicationWriteType `json:"type"`
	User       string               `json:"user,omitempty"`
	OrderID    string               `json:"orderId"`
	PriceLevel int64                `json:"priceLevel,omitempty"`
	Amount     int64                `json:"amount,omitempty"`
	IsBid      bool                 `json:"isBid,omitempty"`
}

type ReplicationRequest struct {
	Entries []ReplicationEntry `json:"entries"`
}

type ReplicationResponse struct {
	Accepted bool  `json:"accepted"`
	LastSeq  int64 `json:"lastSeq"`
}

type ReplicaStateResponse struct {
	Role       NodeRole `json:"role"`
	LastSeq    int64    `json:"lastSeq"`
	AppliedSeq int64    `json:"appliedSeq"`
	PeerCount  int      `json:"peerCount"`
	Primary    string   `json:"primary"`
}

type ReplicaSyncResponse struct {
	Entries []ReplicationEntry `json:"entries"`
}

const (
	RequestIDHeader     = "X-Request-ID"
	RequestIDContextKey = "reqId"
)
