package raftcluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	// BindAddr is the TCP address this node's Raft transport listens on
	// (config `node.raft_bind_addr`). Unused when Deps.Transport is set.
	BindAddr string
	// AdvertiseAddr is the address other nodes dial to reach this node's
	// Raft transport (config `node.raft_advertise_addr`); empty = BindAddr.
	AdvertiseAddr string
	// TLS, when set, runs the Raft transport over mutual TLS. Nil means
	// plain TCP (development only).
	TLS *TransportTLS
	// TokenCipher, when set, encrypts bot tokens before they enter the Raft
	// log (see TokenCipher). Nil stores tokens in plaintext (development
	// only).
	TokenCipher *TokenCipher
	// Self is this node's registry entry (its gRPC address in particular).
	// Whenever this node becomes leader it makes sure Self and KnownPeers
	// are present in the replicated node registry.
	Self NodeInfo
	// KnownPeers are the other cluster members from static configuration
	// (config `raft.peers`), used to seed the node registry and, together
	// with Bootstrap, the initial Raft configuration.
	KnownPeers []NodeInfo
	// JournalRetention overrides DefaultJournalRetention; <= 0 = default.
	JournalRetention int
	// Logger receives background-task warnings; nil = slog.Default().
	Logger *slog.Logger
	// Bootstrap, when true, initializes a brand-new cluster on first start
	// (raft.BootstrapCluster) — but only if this node has no existing Raft
	// state yet, so it is safe to leave true across restarts.
	Bootstrap bool
	// BootstrapPeers is the initial cluster configuration used when
	// Bootstrap is true. Empty means "this node plus KnownPeers" — a single
	// node when KnownPeers is empty too. Every node bootstrapped together
	// must end up with the same server list.
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
// read the replicated state. internal/rpcserver implements
// BotAdmin/Messaging/Maintenance as thin gRPC adapters over these methods
// without reaching into raft.Raft directly.
type Node struct {
	id         string
	raft       *raft.Raft
	fsm        *FSM
	cipher     *TokenCipher
	self       NodeInfo
	knownPeers []NodeInfo
	logger     *slog.Logger

	leaderMu     sync.Mutex
	leaderChs    map[chan bool]struct{}
	stopLeaderFw chan struct{}
}

// Open builds and starts (or restarts) one Raft node. A node that is
// neither bootstrapped nor has existing state waits to be added to a
// running cluster (Node.AddVoter on the current leader).
func Open(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("raftcluster: NodeID is required")
	}

	retention := cfg.MessageRetentionPerBot
	if retention <= 0 {
		retention = DefaultMessageRetentionPerBot
	}
	fsm := NewFSM(retention)
	fsm.SetJournalRetention(cfg.JournalRetention)

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
		peers := cfg.BootstrapPeers
		if len(peers) == 0 && len(cfg.KnownPeers) > 0 {
			peers = []raft.Server{{ID: raftCfg.LocalID, Address: transport.LocalAddr()}}
			for _, p := range cfg.KnownPeers {
				if p.ID == cfg.NodeID {
					continue
				}
				peers = append(peers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.RaftAddr)})
			}
		}
		if err := bootstrapIfNew(r, logStore, stableStore, snapStore, raftCfg.LocalID, transport, peers); err != nil {
			return nil, err
		}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	self := cfg.Self
	self.ID = cfg.NodeID
	if self.RaftAddr == "" {
		self.RaftAddr = string(transport.LocalAddr())
	}

	n := &Node{
		id:           cfg.NodeID,
		raft:         r,
		fsm:          fsm,
		cipher:       cfg.TokenCipher,
		self:         self,
		knownPeers:   cfg.KnownPeers,
		logger:       logger,
		leaderChs:    make(map[chan bool]struct{}),
		stopLeaderFw: make(chan struct{}),
	}
	leaderCh, _ := n.Subscribe()
	go n.forwardLeadership()
	go n.registerOnLeadership(leaderCh)

	return n, nil
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

// ReadJournal returns up to limit journal entries with Seq > after — see
// FSM.ReadJournal. Works on any node: the journal is replicated.
func (n *Node) ReadJournal(after uint64, limit int) ([]JournalEntry, uint64, uint64) {
	return n.fsm.ReadJournal(after, limit)
}

// PollOffset returns the getUpdates offset recorded for botID.
func (n *Node) PollOffset(botID string) int64 { return n.fsm.PollOffset(botID) }

// ListNodes returns the replicated node registry, ordered by ID.
func (n *Node) ListNodes() []NodeInfo { return n.fsm.ListNodes() }

// GetNode returns one node registry entry.
func (n *Node) GetNode(id string) (NodeInfo, bool) { return n.fsm.GetNode(id) }

// LeaderInfo returns the registry entry of the current leader, if both the
// leader and its registry entry are known.
func (n *Node) LeaderInfo() (NodeInfo, bool) {
	id := n.LeaderID()
	if id == "" {
		return NodeInfo{}, false
	}
	return n.fsm.GetNode(id)
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
// currently understands it (empty if unknown/no leader elected).
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

	cmd, err := n.encryptTokens(cmd)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: encode command: %w", err)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		switch {
		case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost),
			errors.Is(err, raft.ErrLeadershipTransferInProgress), errors.Is(err, raft.ErrRaftShutdown):
			// Узел сейчас не может принять запись (не лидер, лидерство
			// уходит или ушло, узел выключается) — повторять нужно на
			// другом узле или чуть позже. Команды идемпотентны, поэтому
			// повтор безопасен даже при ErrLeadershipLost, когда исход
			// неизвестен.
			return nil, fmt.Errorf("%w: %v", ErrNotLeader, err)
		}
		return nil, fmt.Errorf("raftcluster: apply: %w", err)
	}

	result, ok := future.Response().(*ApplyResult)
	if !ok {
		return nil, fmt.Errorf("raftcluster: apply: unexpected FSM response type %T", future.Response())
	}
	if result.Err != nil {
		return nil, result.Err
	}
	if result.Bot != nil {
		b := n.decryptBot(*result.Bot)
		result.Bot = &b
	}
	return result, nil
}

// encryptTokens returns cmd with bot tokens encrypted by n.cipher (if any).
// The command's payload structs are copied, never mutated in place — they
// belong to the caller.
func (n *Node) encryptTokens(cmd Command) (Command, error) {
	if n.cipher == nil {
		return cmd, nil
	}
	switch {
	case cmd.CreateBot != nil:
		c := *cmd.CreateBot
		enc, err := n.cipher.Encrypt(c.Token)
		if err != nil {
			return cmd, err
		}
		c.Token = enc
		cmd.CreateBot = &c
	case cmd.UpdateBot != nil && cmd.UpdateBot.Token != nil:
		c := *cmd.UpdateBot
		enc, err := n.cipher.Encrypt(*c.Token)
		if err != nil {
			return cmd, err
		}
		c.Token = &enc
		cmd.UpdateBot = &c
	}
	return cmd, nil
}

// decryptBot returns b with its token decrypted. A token that cannot be
// decrypted (wrong key) is cleared and logged: the bot then fails to start
// with an obviously malformed token instead of sending ciphertext to
// Telegram.
func (n *Node) decryptBot(b Bot) Bot {
	if n.cipher == nil {
		return b
	}
	plain, err := n.cipher.Decrypt(b.Token)
	if err != nil {
		n.logger.Error("bot token cannot be decrypted",
			"event", "raftcluster.token_decrypt_failed", "bot_id", b.ID, "error", err.Error())
		plain = ""
	}
	b.Token = plain
	return b
}

// ListBots returns every bot currently known to this node's (replicated)
// state, ordered by ID, with tokens decrypted. Safe to call on any node,
// leader or follower — reads never go through Raft.
func (n *Node) ListBots() []Bot {
	bots := n.fsm.ListBots()
	for i := range bots {
		bots[i] = n.decryptBot(bots[i])
	}
	return bots
}

// GetBot returns one bot (token decrypted) and whether it exists.
func (n *Node) GetBot(id string) (Bot, bool) {
	b, ok := n.fsm.GetBot(id)
	if !ok {
		return Bot{}, false
	}
	return n.decryptBot(b), true
}

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
// letting a new election pick whoever wins (администратор должен уметь
// вывести конкретный узел на обслуживание предсказуемо, не самодельными
// выборами). Blocks until the transfer completes or fails; ErrNotLeader if
// this node is not currently the leader.
func (n *Node) TransferLeadershipTo(id, address string) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	future := n.raft.LeadershipTransferToServer(raft.ServerID(id), raft.ServerAddress(address))
	return future.Error()
}

// HandOffLeadership transfers leadership to another voter if this node is
// the leader of a multi-node cluster — called before a graceful shutdown,
// so the cluster gets a new leader right away instead of waiting for the
// heartbeat timeout to notice the old one is gone. No-op otherwise.
func (n *Node) HandOffLeadership() error {
	if !n.IsLeader() {
		return nil
	}
	servers, err := n.Configuration()
	if err != nil {
		return err
	}
	voters := 0
	for _, s := range servers {
		if s.Suffrage == raft.Voter {
			voters++
		}
	}
	if voters < 2 {
		return nil
	}
	return n.raft.LeadershipTransfer().Error()
}

// AddVoter adds a node to the cluster as a voting member and records its
// addresses in the node registry. The node must already be running (not
// bootstrapped) and reachable at info.RaftAddr. Leader only.
func (n *Node) AddVoter(info NodeInfo, timeout time.Duration) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	if info.ID == "" || info.RaftAddr == "" {
		return fmt.Errorf("%w: add_voter: id and raft address are required", ErrInvalidCommand)
	}
	if _, err := n.Apply(Command{Type: CommandRegisterNode, RegisterNode: &RegisterNodeCommand{Node: info}}, timeout); err != nil {
		return err
	}
	return n.raft.AddVoter(raft.ServerID(info.ID), raft.ServerAddress(info.RaftAddr), 0, timeout).Error()
}

// RemoveServer removes a node from the cluster and from the node
// registry. Removing the leader itself is allowed: Raft steps it down.
// Leader only.
func (n *Node) RemoveServer(id string, timeout time.Duration) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	if _, err := n.Apply(Command{Type: CommandUnregisterNode, UnregisterNode: &UnregisterNodeCommand{ID: id}}, timeout); err != nil {
		return err
	}
	return n.raft.RemoveServer(raft.ServerID(id), 0, timeout).Error()
}

// registerOnLeadership makes sure this node and its statically configured
// peers are present in the node registry every time this node becomes
// leader. Followers cannot write, so the leader registers everyone it
// knows about; a node added later via AddVoter is registered there.
func (n *Node) registerOnLeadership(leaderCh <-chan bool) {
	for {
		select {
		case isLeader := <-leaderCh:
			if !isLeader {
				continue
			}
			want := append([]NodeInfo{n.self}, n.knownPeers...)
			for _, info := range want {
				if info.ID == "" {
					continue
				}
				cur, ok := n.fsm.GetNode(info.ID)
				if ok && info.GRPCAddr == "" {
					info.GRPCAddr = cur.GRPCAddr // не затираем известный адрес пустым
				}
				if ok && cur == info {
					continue
				}
				cmd := Command{Type: CommandRegisterNode, RegisterNode: &RegisterNodeCommand{Node: info}}
				if _, err := n.Apply(cmd, 5*time.Second); err != nil {
					n.logger.Warn("node registration failed",
						"event", "raftcluster.register_node_failed", "node_id", info.ID, "error", err.Error())
				}
			}
		case <-n.stopLeaderFw:
			return
		}
	}
}

// Shutdown stops the Raft node and the leadership-forwarding goroutine. It
// blocks until raft.Raft.Shutdown() completes.
func (n *Node) Shutdown() error {
	close(n.stopLeaderFw)
	return n.raft.Shutdown().Error()
}
