package raftcluster

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/hashicorp/raft"
)

// persistedState is the JSON-serializable form of fsmState used by
// Snapshot/Restore. Bots and per-bot message slices are ordered lists (not
// maps), so iteration order — and therefore message order within each bot
// — round-trips exactly.
type persistedState struct {
	Bots     []Bot                       `json:"bots"`
	Messages map[string][]Message        `json:"messages"`        // botID -> messages, oldest first
	Chats    map[string][]ChatMembership `json:"chats,omitempty"` // botID -> chat registry entries, order not meaningful
}

// Snapshot implements raft.FSM (embedded BoltDB storage as the Raft state
// machine, with snapshots). It takes a deep copy of the
// current state under a read lock so that Apply calls that happen while
// the returned raft.FSMSnapshot is later persisted (raft persists
// snapshots concurrently with continued log application) cannot mutate
// data the snapshot already captured.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	ps := persistedState{
		Bots:     make([]Bot, 0, len(f.state.bots)),
		Messages: make(map[string][]Message, len(f.state.messages)),
		Chats:    make(map[string][]ChatMembership, len(f.state.chats)),
	}
	for _, b := range f.state.bots {
		ps.Bots = append(ps.Bots, b.Clone())
	}
	sort.Slice(ps.Bots, func(i, j int) bool { return ps.Bots[i].ID < ps.Bots[j].ID })

	for botID, msgs := range f.state.messages {
		clones := make([]Message, len(msgs))
		for i, m := range msgs {
			clones[i] = m.Clone()
		}
		ps.Messages[botID] = clones
	}

	for botID, byChat := range f.state.chats {
		clones := make([]ChatMembership, 0, len(byChat))
		for _, entry := range byChat {
			clones = append(clones, entry.Clone())
		}
		ps.Chats[botID] = clones
	}

	return &fsmSnapshot{state: ps}, nil
}

// Restore implements raft.FSM: it replaces the entire state, used both
// when a follower installs a snapshot streamed from the leader and when a
// node restarts from its local snapshot store.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var ps persistedState
	if err := json.NewDecoder(rc).Decode(&ps); err != nil {
		return fmt.Errorf("raftcluster: restore: decode: %w", err)
	}

	newState := newFSMState()
	for _, b := range ps.Bots {
		bot := b
		newState.bots[bot.ID] = &bot
	}
	for botID, msgs := range ps.Messages {
		slice := make([]*Message, len(msgs))
		for i, m := range msgs {
			msg := m
			slice[i] = &msg
			newState.msgIndex[msg.IdempotencyKey] = &msg
		}
		newState.messages[botID] = slice
	}
	for botID, entries := range ps.Chats {
		byChat := make(map[int64]*ChatMembership, len(entries))
		for _, e := range entries {
			entry := e
			byChat[entry.ChatID] = &entry
		}
		newState.chats[botID] = byChat
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = newState

	return nil
}

// ExportSnapshot writes a JSON-encoded, point-in-time copy of the entire
// replicated state (every bot, every bot's message history) to w — the
// minimal data Maintenance.ExportState (backup) needs to stream. It
// reuses FSM.Snapshot(), the exact same consistent-copy-under-RLock
// mechanism Raft's own internal snapshotting uses, so this can never
// observe state half-way through a concurrent Apply; it just encodes that
// copy directly to w instead of handing it to raft.SnapshotSink. Streaming
// the resulting bytes to a client in bounded chunks (rather than buffering
// the whole encoded export) is the gRPC layer's job — see
// internal/rpcserver's Maintenance.ExportState.
func (n *Node) ExportSnapshot(w io.Writer) error {
	snap, err := n.fsm.Snapshot()
	if err != nil {
		return fmt.Errorf("raftcluster: export snapshot: %w", err)
	}
	defer snap.Release()

	fs, ok := snap.(*fsmSnapshot)
	if !ok {
		return fmt.Errorf("raftcluster: export snapshot: unexpected snapshot type %T", snap)
	}
	if err := json.NewEncoder(w).Encode(fs.state); err != nil {
		return fmt.Errorf("raftcluster: export snapshot: encode: %w", err)
	}
	return nil
}

// fsmSnapshot implements raft.FSMSnapshot over a persistedState captured
// at Snapshot() time.
type fsmSnapshot struct {
	state persistedState
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("raftcluster: snapshot: encode: %w", err)
	}
	if _, err := sink.Write(data); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("raftcluster: snapshot: write: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
