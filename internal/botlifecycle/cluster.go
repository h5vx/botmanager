package botlifecycle

import "github.com/h5vx/botmanager/internal/raftcluster"

// ClusterView is the subset of *raftcluster.Node that Manager needs:
// leadership status/changes and the current bot state. It exists so tests
// can substitute a fake cluster instead of running a real Raft node — see
// the fakeCluster type in manager_test.go — while production wiring (in
// cmd/botmanager) simply passes a *raftcluster.Node, which
// satisfies this interface without any adapter.
type ClusterView interface {
	// IsLeader reports whether this node is currently the Raft leader
	// (only the leader runs bot processes).
	IsLeader() bool
	// ListBots returns every bot currently known to the replicated state.
	ListBots() []raftcluster.Bot
	// Subscribe returns a channel receiving this node's own leadership
	// changes, including the initial election, and a cancel function.
	Subscribe() (<-chan bool, func())
	// SubscribeApplied returns a channel that receives a value after every
	// committed command — Manager's signal to re-reconcile bot state
	// (lifecycle transitions) without raftcluster needing to know about bot
	// runners.
	SubscribeApplied() (<-chan struct{}, func())
}

var _ ClusterView = (*raftcluster.Node)(nil)
