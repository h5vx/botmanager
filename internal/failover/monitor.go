// Package failover moves Raft leadership away from a leader that has lost
// its connection to Telegram while the rest of the cluster still has one.
//
// Raft only replaces a leader that stops talking to the other nodes. A
// leader whose network path to Telegram (or its proxy) is broken keeps
// answering heartbeats, so without this package every bot would stay stuck
// in backoff on the one node that cannot reach Telegram. Monitor collects,
// per bot, whether this node reaches Telegram (it implements
// telegram.HealthReporter) and, while this node is leader, periodically
// decides whether the node itself is the problem:
//
//   - at least MinFailingBots running bots, and at least half of them, are
//     failing (FailureClassNode) and have had no response from Telegram at
//     all for FailureWindow, counted from their last response, not from
//     the first failure — a hung long poll reports its failure only when it
//     times out — a single bot failing alongside healthy ones points at
//     that bot, not at the node;
//   - this node's own check (Maintenance.PingTelegram on itself: a fresh
//     getMe) fails too, so one transient error never moves leadership;
//   - a peer is found that does reach Telegram for those same bots (its own
//     Maintenance.PingTelegram with the bot's token and effective proxy),
//     so a Telegram-wide outage or one bot's broken proxy never makes
//     leadership bounce between nodes that are all equally unable;
//   - no transfer happened within Cooldown.
//
// Then leadership is transferred to that peer with Raft's own leadership
// transfer, and the bots start on the new leader where the poll loop
// resumes from the replicated offset.
package failover

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// Cluster is the part of *raftcluster.Node the monitor needs.
type Cluster interface {
	IsLeader() bool
	ID() string
	ListNodes() []raftcluster.NodeInfo
	TransferLeadershipTo(id, address string) error
}

// PeerProber asks another node whether it reaches Telegram for one bot.
type PeerProber interface {
	ProbeTelegram(ctx context.Context, grpcAddr, botID string) (bool, error)
}

// Config tunes the monitor; zero values mean the defaults below.
type Config struct {
	// FailureWindow is how long a failing bot must have gone without any
	// response from Telegram before it counts (default 15s).
	FailureWindow time.Duration
	// MinFailingBots is the minimum number of failing bots (default 1).
	MinFailingBots int
	// Cooldown is the minimum time between two transfers started by this
	// node (default 5m).
	Cooldown time.Duration
	// CheckInterval is how often the decision is made (default 2s).
	CheckInterval time.Duration
	// ProbeTimeout bounds one probe of this node or a peer (default 5s).
	ProbeTimeout time.Duration
	// MaxProbedBots caps how many failing bots are checked on each peer
	// (default 3).
	MaxProbedBots int
}

func (c Config) withDefaults() Config {
	if c.FailureWindow <= 0 {
		c.FailureWindow = 15 * time.Second
	}
	if c.MinFailingBots <= 0 {
		c.MinFailingBots = 1
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 5 * time.Minute
	}
	if c.CheckInterval <= 0 {
		c.CheckInterval = 2 * time.Second
	}
	if c.ProbeTimeout <= 0 {
		c.ProbeTimeout = 5 * time.Second
	}
	if c.MaxProbedBots <= 0 {
		c.MaxProbedBots = 3
	}
	return c
}

// Monitor implements telegram.HealthReporter and runs the step-down
// decision. Create it with New and run Run in its own goroutine.
type Monitor struct {
	cfg     Config
	cluster Cluster
	prober  PeerProber
	logger  *slog.Logger
	now     func() time.Time

	mu           sync.Mutex
	failingSince map[string]time.Time // botID -> last response before the current failure streak (zero = reachable)
	lastResponse map[string]time.Time // botID -> last time Telegram answered
	lastTransfer time.Time
	lastAttempt  time.Time
}

// New builds a Monitor.
func New(cfg Config, cluster Cluster, prober PeerProber, logger *slog.Logger) *Monitor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Monitor{
		cfg:          cfg.withDefaults(),
		cluster:      cluster,
		prober:       prober,
		logger:       logger,
		now:          time.Now,
		failingSince: make(map[string]time.Time),
		lastResponse: make(map[string]time.Time),
	}
}

// TelegramReachable implements telegram.HealthReporter.
func (m *Monitor) TelegramReachable(botID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failingSince[botID] = time.Time{}
	m.lastResponse[botID] = m.now()
}

// TelegramUnreachable implements telegram.HealthReporter.
func (m *Monitor) TelegramUnreachable(botID string, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failingSince[botID].IsZero() {
		since := m.lastResponse[botID]
		if since.IsZero() {
			since = m.now()
		}
		m.failingSince[botID] = since
	}
}

// BotStopped implements telegram.HealthReporter.
func (m *Monitor) BotStopped(botID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failingSince, botID)
	delete(m.lastResponse, botID)
}

// Run checks every CheckInterval until ctx is done.
func (m *Monitor) Run(ctx context.Context) {
	t := time.NewTicker(m.cfg.CheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Check(ctx)
		}
	}
}

// failingBots returns the bots failing for at least FailureWindow, sorted,
// and the number of bots currently tracked.
func (m *Monitor) failingBots() (failing []string, tracked int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for botID, since := range m.failingSince {
		if !since.IsZero() && now.Sub(since) >= m.cfg.FailureWindow {
			failing = append(failing, botID)
		}
	}
	sort.Strings(failing)
	return failing, len(m.failingSince)
}

// Check makes one step-down decision; it reports whether leadership was
// handed over.
func (m *Monitor) Check(ctx context.Context) bool {
	if !m.cluster.IsLeader() {
		return false
	}
	failing, tracked := m.failingBots()
	if len(failing) < m.cfg.MinFailingBots || 2*len(failing) < tracked {
		return false
	}

	now := m.now()
	m.mu.Lock()
	if now.Sub(m.lastTransfer) < m.cfg.Cooldown || now.Sub(m.lastAttempt) < m.cfg.FailureWindow {
		m.mu.Unlock()
		return false
	}
	m.lastAttempt = now
	m.mu.Unlock()

	probed := failing
	if len(probed) > m.cfg.MaxProbedBots {
		probed = probed[:m.cfg.MaxProbedBots]
	}
	m.logger.Warn("this node does not reach telegram for its bots; looking for a peer that does",
		"event", "failover.node_unhealthy", "failing_bots", len(failing), "running_bots", tracked)

	self := m.cluster.ID()
	nodes := m.cluster.ListNodes()
	for _, n := range nodes {
		if n.ID == self && n.GRPCAddr != "" && m.peerReaches(ctx, n, probed) {
			m.logger.Info("bots failing, but this node's own check reaches telegram; keeping leadership",
				"event", "failover.self_check_ok", "failing_bots", len(failing))
			return false
		}
	}
	for _, peer := range nodes {
		if peer.ID == self || peer.GRPCAddr == "" || peer.RaftAddr == "" {
			continue
		}
		if !m.peerReaches(ctx, peer, probed) {
			continue
		}
		if err := m.cluster.TransferLeadershipTo(peer.ID, peer.RaftAddr); err != nil {
			m.logger.Error("leadership transfer failed",
				"event", "failover.transfer_failed", "target_node_id", peer.ID, "error", err.Error())
			continue
		}
		m.mu.Lock()
		m.lastTransfer = m.now()
		m.mu.Unlock()
		m.logger.Warn("leadership handed over to a node that reaches telegram",
			"event", "failover.leadership_transferred", "target_node_id", peer.ID)
		return true
	}
	m.logger.Warn("no peer reaches telegram for the failing bots; keeping leadership",
		"event", "failover.no_healthy_peer", "failing_bots", len(failing))
	return false
}

func (m *Monitor) peerReaches(ctx context.Context, peer raftcluster.NodeInfo, bots []string) bool {
	for _, botID := range bots {
		pctx, cancel := context.WithTimeout(ctx, m.cfg.ProbeTimeout)
		ok, err := m.prober.ProbeTelegram(pctx, peer.GRPCAddr, botID)
		cancel()
		if err != nil || !ok {
			return false
		}
	}
	return true
}
