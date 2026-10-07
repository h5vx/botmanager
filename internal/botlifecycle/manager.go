package botlifecycle

import (
	"context"
	"log/slog"
	"sync"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// Manager drives the bot lifecycle on top of a ClusterView: whenever
// this node is the Raft leader, it keeps exactly one running BotRunner per
// enabled bot; whenever it is not (including at startup, before any
// leader is known), it keeps none (only the leader runs bot processes).
// Manager holds no lifecycle state of its own — the source of
// truth is always raftcluster's replicated Bot.State, re-read on every
// reconcile — so it never needs to be told about a state change directly,
// only that *something* changed (see SubscribeApplied on ClusterView).
type Manager struct {
	cluster   ClusterView
	newRunner RunnerFactory
	logger    *slog.Logger

	mu      sync.Mutex
	running map[string]runnerHandle
	runCtx  context.Context
}

type runnerHandle struct {
	runner BotRunner
	cancel context.CancelFunc
}

// NewManager constructs a Manager. logger defaults to slog.Default() when
// nil.
func NewManager(cluster ClusterView, newRunner RunnerFactory, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		cluster:   cluster,
		newRunner: newRunner,
		logger:    logger,
		running:   make(map[string]runnerHandle),
	}
}

// Run subscribes to leadership and applied-command changes and reconciles
// running bot runners against the desired state until ctx is cancelled.
// It blocks — the caller runs it in its own goroutine (go manager.Run(ctx)).
func (m *Manager) Run(ctx context.Context) {
	leaderCh, cancelLeader := m.cluster.Subscribe()
	appliedCh, cancelApplied := m.cluster.SubscribeApplied()
	defer cancelLeader()
	defer cancelApplied()

	m.mu.Lock()
	m.runCtx = ctx
	m.mu.Unlock()

	// Покрывает случай, когда лидерство уже установлено к моменту вызова
	// Run (например, узел перезапустился в уже существующем кластере и
	// сразу снова стал лидером) — не полагаемся только на будущее событие
	// в leaderCh, которое к этому моменту уже могло быть доставлено и
	// потеряно, раз никто ещё не был подписан.
	m.reconcile()

	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return
		case _, ok := <-leaderCh:
			if !ok {
				m.stopAll()
				return
			}
			m.reconcile()
		case _, ok := <-appliedCh:
			if !ok {
				m.stopAll()
				return
			}
			m.reconcile()
		}
	}
}

// RunningBotIDs returns the IDs of bots this node currently has a runner
// for. Useful for tests and, later, metrics (botmanager_bots_running).
func (m *Manager) RunningBotIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make([]string, 0, len(m.running))
	for id := range m.running {
		ids = append(ids, id)
	}
	return ids
}

// reconcile makes the set of running runners match "leader and enabled"
// bots. Called on every leadership or applied-command
// notification; cheap and idempotent when nothing actually needs to
// change, so over-triggering it is harmless.
func (m *Manager) reconcile() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.cluster.IsLeader() {
		if len(m.running) > 0 {
			m.logger.Info("lost leadership, stopping all bot runners",
				"event", "botlifecycle.leadership_lost", "bot_count", len(m.running))
		}
		m.stopAllLocked()
		return
	}

	desired := make(map[string]raftcluster.Bot)
	for _, b := range m.cluster.ListBots() {
		if b.State == raftcluster.BotStateEnabled {
			desired[b.ID] = b
		}
	}

	for id, handle := range m.running {
		if _, ok := desired[id]; ok {
			continue
		}
		m.logger.Info("stopping bot runner",
			"event", "botlifecycle.runner_stop", "bot_id", id, "reason", "no longer enabled")
		handle.cancel()
		handle.runner.Stop()
		delete(m.running, id)
	}

	for id, bot := range desired {
		if _, ok := m.running[id]; ok {
			continue
		}
		m.startLocked(bot)
	}
}

func (m *Manager) startLocked(bot raftcluster.Bot) {
	runCtx := m.runCtx
	if runCtx == nil {
		runCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(runCtx)

	runner := m.newRunner()
	if err := runner.Start(ctx, bot); err != nil {
		m.logger.Error("bot runner start failed",
			"event", "botlifecycle.runner_start_failed", "bot_id", bot.ID, "error", err.Error())
		cancel()
		return
	}

	m.running[bot.ID] = runnerHandle{runner: runner, cancel: cancel}
	m.logger.Info("started bot runner", "event", "botlifecycle.runner_start", "bot_id", bot.ID)
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopAllLocked()
}

func (m *Manager) stopAllLocked() {
	for id, handle := range m.running {
		handle.cancel()
		handle.runner.Stop()
		delete(m.running, id)
	}
}
