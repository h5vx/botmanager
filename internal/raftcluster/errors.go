package raftcluster

import "errors"

// Sentinel errors returned by FSM.Apply (wrapped into ApplyResult.Err) and
// by Node's convenience accessors. Compare with errors.Is.
var (
	// ErrNotLeader is returned by Node.Apply when called on a node that is
	// not currently the Raft leader — only the leader may propose commands.
	// Forwarding a write to the leader is the gRPC layer's job; this
	// package just refuses to Apply on a non-leader.
	ErrNotLeader = errors.New("raftcluster: this node is not the raft leader")

	// ErrBotNotFound is returned when a command references a bot ID that
	// does not exist in the current state.
	ErrBotNotFound = errors.New("raftcluster: bot not found")

	// ErrBotAlreadyExists is returned by CommandCreateBot when the given ID
	// is already present.
	ErrBotAlreadyExists = errors.New("raftcluster: bot already exists")

	// ErrMessageNotFound is returned when a command references an
	// idempotency_key that does not exist (or has been evicted by
	// retention, see doc.go).
	ErrMessageNotFound = errors.New("raftcluster: message not found")

	// ErrInvalidCommand is returned when a Command has zero or more than
	// one payload field set for its Type, or a required field is empty.
	ErrInvalidCommand = errors.New("raftcluster: invalid command")
)
