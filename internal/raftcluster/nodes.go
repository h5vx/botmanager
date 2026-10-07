package raftcluster

import (
	"fmt"
	"sort"
)

// NodeInfo is one cluster member's addresses as recorded in the replicated
// node registry. Raft itself only knows the Raft transport address; the
// gRPC address is what lets any node forward a write to the current
// leader and probe its peers.
type NodeInfo struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
}

// RegisterNodeCommand adds or replaces one entry of the node registry.
type RegisterNodeCommand struct {
	Node NodeInfo `json:"node"`
}

// UnregisterNodeCommand removes one entry of the node registry.
type UnregisterNodeCommand struct {
	ID string `json:"id"`
}

func (f *FSM) applyRegisterNode(c *RegisterNodeCommand) *ApplyResult {
	if c == nil || c.Node.ID == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: register_node: missing id", ErrInvalidCommand)}
	}
	f.state.nodes[c.Node.ID] = c.Node
	return &ApplyResult{}
}

func (f *FSM) applyUnregisterNode(c *UnregisterNodeCommand) *ApplyResult {
	if c == nil || c.ID == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: unregister_node: missing id", ErrInvalidCommand)}
	}
	delete(f.state.nodes, c.ID)
	return &ApplyResult{}
}

// ListNodes returns the node registry ordered by ID.
func (f *FSM) ListNodes() []NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out := make([]NodeInfo, 0, len(f.state.nodes))
	for _, n := range f.state.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// GetNode returns one registry entry and whether it exists.
func (f *FSM) GetNode(id string) (NodeInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n, ok := f.state.nodes[id]
	return n, ok
}
