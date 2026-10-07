package telegram

import (
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// ClusterView is the subset of *raftcluster.Node a Runner needs: apply
// commands (received updates, delivery statuses, bot state), read a bot's
// message history and its acknowledged getUpdates offset, and wake up when
// something changed instead of blindly polling raftcluster.
// Mirrors botlifecycle.ClusterView's pattern — a narrow interface so this
// package is testable without a real Raft node (see fakeCluster in
// runner_test.go).
type ClusterView interface {
	Apply(cmd raftcluster.Command, timeout time.Duration) (*raftcluster.ApplyResult, error)
	ListMessages(filter raftcluster.ListMessagesFilter, pageSize int, pageToken string) ([]raftcluster.Message, string, error)
	SubscribeApplied() (<-chan struct{}, func())
	PollOffset(botID string) int64
}

var _ ClusterView = (*raftcluster.Node)(nil)
