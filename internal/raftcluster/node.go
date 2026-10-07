package raftcluster

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Dependencies lets callers override the concrete Raft building blocks —
// transport, log/stable store, snapshot store. Production Open() leaves
// these nil and builds a real TCP transport plus BoltDB log/stable store
// and file snapshots rooted at Config.DataDir/Config.BindAddr; tests
// inject in-memory equivalents (raft.NewInmemTransport, raft.NewInmemStore,
// raft.NewInmemSnapshotStore) to run a real multi-node Raft cluster without
// touching the filesystem or network.
//
// This is also how the package stays honest about its scope: a
// single-node bootstrap (see Config.Bootstrap/BootstrapPeers) runs through
// the exact same Open()/Apply()/FSM code as a multi-node cluster — it is a
// special case of the general path, not a separate
// implementation that would need rewriting to add nodes later.
type Dependencies struct {
	Transport     raft.Transport
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
}

// Config configures one Raft node.
type Config struct {
	// NodeID is this node's Raft server ID (config `node.id`).
	NodeID string
	// DataDir holds the BoltDB log/stable store and file snapshots when
	// Deps does not override them (config `node.data_dir`).
	DataDir string
	// BindAddr is the TCP address this node's Raft transport listens and
	// is advertised on (config `node.raft_bind_addr`). Unused when
	// Deps.Transport is set.
	BindAddr string
	// Bootstrap, when true, initializes a brand-new cluster on first start
	// (raft.BootstrapCluster) — but only if this node has no existing Raft
	// state yet, so it is safe to leave true across restarts.
	Bootstrap bool
	// BootstrapPeers is the initial cluster configuration used when
	// Bootstrap is true. Empty means "single-node cluster containing only
	// this node" — the default (see Dependencies doc above). A
	// multi-node deployment passes the full server list here, identically
	// on every node being bootstrapped together.
	BootstrapPeers []raft.Server
	// MessageRetentionPerBot overrides DefaultMessageRetentionPerBot; <= 0
	// means "use the default".
	MessageRetentionPerBot int
	// RaftConfig overrides raft.DefaultConfig() as the base configuration;
	// LocalID is always overwritten from NodeID regardless of what is set
	// here. Tests use this to shrink election/heartbeat timeouts for fast,
	// deterministic runs and to silence hclog output.
	RaftConfig *raft.Config

	Deps Dependencies
}

// Node wraps one Raft server together with its FSM and exposes this
// package's public surface: apply commands, check/observe leadership, and
// read the replicated state. It is deliberately shaped so that the next
// step can implement BotAdmin/Messaging/Maintenance as thin gRPC adapters
// over these methods without reaching into raft.Raft directly.
type Node struct {
	id   string
	raft *raft.Raft
	fsm  *FSM

	leaderMu     sync.Mutex
	leaderChs    map[chan bool]struct{}
	stopLeaderFw chan struct{}
}

// Open builds and starts (or rejoins, once membership RPCs exist) one Raft
// node.
func Open(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("raftcluster: NodeID is required")
	}

	retention := cfg.MessageRetentionPerBot
	if retention <= 0 {
		retention = DefaultMessageRetentionPerBot
	}
	fsm := NewFSM(retention)

	raftCfg := cfg.RaftConfig
	if raftCfg == nil {
		raftCfg = raft.DefaultConfig()
	}
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)

	transport, err := buildTransport(cfg)
	if err != nil {
		return nil, err
	}
	logStore, stableStore, err := buildLogStores(cfg)
	if err != nil {
		return nil, err
	}
	snapStore, err := buildSnapshotStore(cfg)
	if err != nil {
		return nil, err
	}

	r, err := raft.NewRaft(raftCfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: new raft: %w", err)
	}

	if cfg.Bootstrap {
		if err := bootstrapIfNew(r, logStore, stableStore, snapStore, raftCfg.LocalID, transport, cfg.BootstrapPeers); err != nil {
			return nil, err
		}
	}

	n := &Node{
		id:           cfg.NodeID,
		raft:         r,
		fsm:          fsm,
		leaderChs:    make(map[chan bool]struct{}),
		stopLeaderFw: make(chan struct{}),
	}
	go n.forwardLeadership()

	return n, nil
}

func buildTransport(cfg Config) (raft.Transport, error) {
	if cfg.Deps.Transport != nil {
		return cfg.Deps.Transport, nil
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("raftcluster: BindAddr is required when Deps.Transport is not set")
	}
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: resolve bind addr %q: %w", cfg.BindAddr, err)
	}
	t, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: tcp transport: %w", err)
	}
	return t, nil
}

func buildLogStores(cfg Config) (raft.LogStore, raft.StableStore, error) {
	logStore, stableStore := cfg.Deps.LogStore, cfg.Deps.StableStore
	if logStore != nil && stableStore != nil {
		return logStore, stableStore, nil
	}
	if cfg.DataDir == "" {
		return nil, nil, fmt.Errorf("raftcluster: DataDir is required when Deps.LogStore/StableStore are not set")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("raftcluster: create data dir %q: %w", cfg.DataDir, err)
	}
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.bolt"))
	if err != nil {
		return nil, nil, fmt.Errorf("raftcluster: open boltdb: %w", err)
	}
	if logStore == nil {
		logStore = bolt
	}
	if stableStore == nil {
		stableStore = bolt
	}
	return logStore, stableStore, nil
}

func buildSnapshotStore(cfg Config) (raft.SnapshotStore, error) {
	if cfg.Deps.SnapshotStore != nil {
		return cfg.Deps.SnapshotStore, nil
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("raftcluster: DataDir is required when Deps.SnapshotStore is not set")
	}
	s, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: file snapshot store: %w", err)
	}
	return s, nil
}

// bootstrapIfNew calls raft.BootstrapCluster only if this node has no
// existing Raft state yet, so Config.Bootstrap = true is safe to leave set
// across restarts (a single-node bootstrap must not
// re-bootstrap — and re-clobber cluster membership — on every process
// start).
func bootstrapIfNew(r *raft.Raft, logStore raft.LogStore, stableStore raft.StableStore, snapStore raft.SnapshotStore, localID raft.ServerID, transport raft.Transport, peers []raft.Server) error {
	hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		return fmt.Errorf("raftcluster: check existing state: %w", err)
	}
	if hasState {
		return nil
	}

	servers := peers
	if len(servers) == 0 {
		servers = []raft.Server{{ID: localID, Address: transport.LocalAddr()}}
	}
	if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		return fmt.Errorf("raftcluster: bootstrap cluster: %w", err)
	}
	return nil
}

// forwardLeadership fans out raft.Raft's own LeaderCh() (a single channel
// with exactly one intended consumer) to every channel handed out by
// Subscribe. It runs for the lifetime of the Node.
func (n *Node) forwardLeadership() {
	for {
		select {
		case isLeader, ok := <-n.raft.LeaderCh():
			if !ok {
				return
			}
			n.leaderMu.Lock()
			for ch := range n.leaderChs {
				select {
				case ch <- isLeader:
				default:
					// не читают быстрее, чем меняется лидерство — заменяем
					// непрочитанное старое значение новым (актуальное
					// состояние важнее истории событий).
					select {
					case <-ch:
					default:
					}
					select {
					case ch <- isLeader:
					default:
					}
				}
			}
			n.leaderMu.Unlock()
		case <-n.stopLeaderFw:
			return
		}
	}
}

// Subscribe returns a channel that receives true when this node becomes
// the Raft leader and false when it loses leadership — including the
// initial election (only the leader runs bot processes; consumers
// such as botlifecycle.Manager need to learn about the very first
// election, not just later changes). The channel is buffered (size 1) with
// latest-value-wins delivery: a slow consumer sees the current leadership
// status, not a backlog of every historical flap. Call the returned cancel
// function to unsubscribe.
func (n *Node) Subscribe() (<-chan bool, func()) {
	ch := make(chan bool, 1)

	n.leaderMu.Lock()
	n.leaderChs[ch] = struct{}{}
	n.leaderMu.Unlock()

	cancel := func() {
		n.leaderMu.Lock()
		delete(n.leaderChs, ch)
		n.leaderMu.Unlock()
	}
	return ch, cancel
}

// SubscribeApplied returns a channel that receives a value after every
// committed Apply call, successful or rejected — "something changed,
// re-read state if you care" rather than the command itself. botlifecycle
// uses this to notice bot state changes (enabled/disabled/broken) on the
// leader without raftcluster needing to know anything about bot runners.
func (n *Node) SubscribeApplied() (<-chan struct{}, func()) {
	return n.fsm.SubscribeApplied()
}

// SubscribeEvents returns a channel receiving one FSM Event per
// successfully applied command (bot/message-specific detail, unlike
// SubscribeApplied's coarse signal) — see FSM.Event doc. Used by the gRPC
// layer's Messaging.Subscribe to produce targeted message_status_changed
// and bot_state_changed updates.
func (n *Node) SubscribeEvents() (<-chan Event, func()) {
	return n.fsm.SubscribeEvents()
}

// IsLeader reports whether this node is currently the Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the network address of the current leader as this
// node currently understands it (empty if unknown/no leader elected).
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the Raft server ID of the current leader as this node
// currently understands it (empty if unknown/no leader elected). Used by
// Maintenance.GetClusterStatus and by the gRPC layer's
// not-leader error (a deliberate simplification — see
// CLAUDE.md — instead of retransmitting a write to the leader,
// the gRPC layer returns an error naming it).
func (n *Node) LeaderID() string {
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// Configuration returns the current Raft cluster membership (every known
// server, including this node) as understood by the local Raft instance.
// Used by Maintenance.GetClusterStatus to list nodes — reads
// the real Raft configuration rather than hardcoding "just me", so the
// same code keeps working once a real multi-node deployment exists.
func (n *Node) Configuration() ([]raft.Server, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raftcluster: get configuration: %w", err)
	}
	return future.Configuration().Servers, nil
}

// ID returns this node's Raft server ID (config `node.id`).
func (n *Node) ID() string { return n.id }

// Apply proposes cmd to the Raft log and blocks until it is committed and
// applied to this node's FSM (or timeout/ErrNotLeader). Only the leader
// may call this successfully — ErrNotLeader otherwise; a replica that
// receives a write request is expected to forward it to the current
// leader rather than call Apply itself (that is the gRPC layer's job, not
// this package's).
func (n *Node) Apply(cmd Command, timeout time.Duration) (*ApplyResult, error) {
	if !n.IsLeader() {
		return nil, ErrNotLeader
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: encode command: %w", err)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raftcluster: apply: %w", err)
	}

	result, ok := future.Response().(*ApplyResult)
	if !ok {
		return nil, fmt.Errorf("raftcluster: apply: unexpected FSM response type %T", future.Response())
	}
	if result.Err != nil {
		return nil, result.Err
	}
	return result, nil
}

// ListBots returns every bot currently known to this node's (replicated)
// state, ordered by ID. Safe to call on any node, leader or follower —
// reads never go through Raft.
func (n *Node) ListBots() []Bot { return n.fsm.ListBots() }

// GetBot returns one bot and whether it exists.
func (n *Node) GetBot(id string) (Bot, bool) { return n.fsm.GetBot(id) }

// GetMessage looks up one message by idempotency key.
func (n *Node) GetMessage(idempotencyKey string) (Message, bool) {
	return n.fsm.GetMessage(idempotencyKey)
}

// ListMessages returns one page of a bot's message history. See
// ListMessagesFilter and FSM.ListMessages for the (intentionally minimal)
// filtering/pagination contract.
func (n *Node) ListMessages(filter ListMessagesFilter, pageSize int, pageToken string) ([]Message, string, error) {
	return n.fsm.ListMessages(filter, pageSize, pageToken)
}

// ListChatIDs returns the distinct chat IDs this bot has exchanged messages
// with, most-recently-active first. See FSM.ListChatIDs.
func (n *Node) ListChatIDs(botID string) []int64 {
	return n.fsm.ListChatIDs(botID)
}

// ListChatRegistry returns this bot's chat membership registry (built from
// my_chat_member events), most-recently-changed first. See
// FSM.ListChatRegistry.
func (n *Node) ListChatRegistry(botID string) []ChatMembership {
	return n.fsm.ListChatRegistry(botID)
}

// TransferLeadershipTo asks Raft to hand leadership to the server identified
// by id/address, using Raft's own leadership-transfer mechanism
// (raft.Raft.LeadershipTransferToServer) rather than stepping down and
// letting a new election pick whoever wins (администратор должен уметь вывести конкретный узел на обслуживание предсказуемо, не
// самодельными выборами). Blocks until the transfer completes or fails;
// ErrNotLeader if this node is not currently the leader (mirrors Apply's own
// check — the caller, not this package, retransmits to the real leader, see
// requireLeader in internal/rpcserver).
func (n *Node) TransferLeadershipTo(id, address string) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	future := n.raft.LeadershipTransferToServer(raft.ServerID(id), raft.ServerAddress(address))
	return future.Error()
}

// Shutdown stops the Raft node and the leadership-forwarding goroutine. It
// blocks until raft.Raft.Shutdown() completes.
func (n *Node) Shutdown() error {
	close(n.stopLeaderFw)
	return n.raft.Shutdown().Error()
}
