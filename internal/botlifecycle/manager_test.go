package botlifecycle

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// fakeCluster is a minimal, hand-rolled ClusterView test double — not part
// of the package's public API, just a stand-in for a real Raft node so
// Manager's reconciliation logic can be tested without one.
type fakeCluster struct {
	mu       sync.Mutex
	isLeader bool
	bots     map[string]raftcluster.Bot

	leaderCh  chan bool
	appliedCh chan struct{}
}

var _ ClusterView = (*fakeCluster)(nil)

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		bots:      make(map[string]raftcluster.Bot),
		leaderCh:  make(chan bool, 1),
		appliedCh: make(chan struct{}, 1),
	}
}

func (c *fakeCluster) IsLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isLeader
}

func (c *fakeCluster) ListBots() []raftcluster.Bot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]raftcluster.Bot, 0, len(c.bots))
	for _, b := range c.bots {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *fakeCluster) Subscribe() (<-chan bool, func())            { return c.leaderCh, func() {} }
func (c *fakeCluster) SubscribeApplied() (<-chan struct{}, func()) { return c.appliedCh, func() {} }

// setLeader changes leadership and pushes a notification, same
// latest-value-wins delivery as raftcluster.Node.Subscribe.
func (c *fakeCluster) setLeader(v bool) {
	c.mu.Lock()
	c.isLeader = v
	c.mu.Unlock()

	select {
	case c.leaderCh <- v:
	default:
		select {
		case <-c.leaderCh:
		default:
		}
		select {
		case c.leaderCh <- v:
		default:
		}
	}
}

// putBot upserts a bot (simulating an applied raftcluster command) and
// notifies appliedCh.
func (c *fakeCluster) putBot(b raftcluster.Bot) {
	c.mu.Lock()
	c.bots[b.ID] = b
	c.mu.Unlock()

	select {
	case c.appliedCh <- struct{}{}:
	default:
	}
}

// fakeRunner is the BotRunner test double used across this file.
type fakeRunner struct {
	reg *runnerRegistry

	mu      sync.Mutex
	botID   string
	started bool
	stopped bool
}

func (r *fakeRunner) Start(ctx context.Context, bot raftcluster.Bot) error {
	r.mu.Lock()
	r.botID = bot.ID
	r.mu.Unlock()

	if r.reg.shouldFail(bot.ID) {
		return fmt.Errorf("fake start failure for %s", bot.ID)
	}

	r.mu.Lock()
	r.started = true
	r.mu.Unlock()

	r.reg.register(bot.ID, r)
	return nil
}

func (r *fakeRunner) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
}

func (r *fakeRunner) isStarted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started
}

func (r *fakeRunner) isStopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}

// runnerRegistry tracks the most recently successfully-started fakeRunner
// per bot ID, and lets tests force a Start failure for a given bot ID.
type runnerRegistry struct {
	mu      sync.Mutex
	byBot   map[string]*fakeRunner
	failFor map[string]bool
}

func newRunnerRegistry() *runnerRegistry {
	return &runnerRegistry{byBot: make(map[string]*fakeRunner), failFor: make(map[string]bool)}
}

func (reg *runnerRegistry) factory() RunnerFactory {
	return func() BotRunner { return &fakeRunner{reg: reg} }
}

func (reg *runnerRegistry) register(botID string, r *fakeRunner) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.byBot[botID] = r
}

func (reg *runnerRegistry) get(botID string) *fakeRunner {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	return reg.byBot[botID]
}

func (reg *runnerRegistry) failNextStart(botID string) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.failFor[botID] = true
}

func (reg *runnerRegistry) shouldFail(botID string) bool {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	return reg.failFor[botID]
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

func enabledBot(id string) raftcluster.Bot {
	return raftcluster.Bot{ID: id, State: raftcluster.BotStateEnabled}
}

func startManager(t *testing.T, cluster *fakeCluster, reg *runnerRegistry) *Manager {
	t.Helper()
	m := NewManager(cluster, reg.factory(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go m.Run(ctx)
	return m
}

func TestManager_PromotionStartsEnabledBotsOnly(t *testing.T) {
	cluster := newFakeCluster()
	cluster.putBot(enabledBot("bot-1"))
	cluster.putBot(raftcluster.Bot{ID: "bot-2", State: raftcluster.BotStateDisabled})
	reg := newRunnerRegistry()

	m := startManager(t, cluster, reg)

	// не лидер — раннеры не должны стартовать.
	time.Sleep(20 * time.Millisecond)
	if ids := m.RunningBotIDs(); len(ids) != 0 {
		t.Fatalf("RunningBotIDs before leadership = %v, want empty", ids)
	}

	cluster.setLeader(true)

	waitFor(t, time.Second, func() bool {
		ids := m.RunningBotIDs()
		return len(ids) == 1 && ids[0] == "bot-1"
	}, "bot-1 runner did not start after promotion")

	if reg.get("bot-2") != nil {
		t.Fatalf("disabled bot-2 got a runner")
	}
	if r := reg.get("bot-1"); r == nil || !r.isStarted() {
		t.Fatalf("bot-1 runner not started: %+v", r)
	}
}

func TestManager_LeadershipLossStopsAllRunners(t *testing.T) {
	cluster := newFakeCluster()
	cluster.putBot(enabledBot("bot-1"))
	reg := newRunnerRegistry()

	m := startManager(t, cluster, reg)
	cluster.setLeader(true)
	waitFor(t, time.Second, func() bool { return len(m.RunningBotIDs()) == 1 }, "bot-1 did not start")

	runner := reg.get("bot-1")

	cluster.setLeader(false)

	waitFor(t, time.Second, func() bool { return len(m.RunningBotIDs()) == 0 }, "runners not stopped after losing leadership")
	if !runner.isStopped() {
		t.Fatalf("bot-1 runner not stopped after losing leadership")
	}
}

func TestManager_BotDisabledOnLeaderStopsItsRunner(t *testing.T) {
	cluster := newFakeCluster()
	cluster.putBot(enabledBot("bot-1"))
	cluster.putBot(enabledBot("bot-2"))
	reg := newRunnerRegistry()

	m := startManager(t, cluster, reg)
	cluster.setLeader(true)
	waitFor(t, time.Second, func() bool { return len(m.RunningBotIDs()) == 2 }, "bots did not start")

	bot1Runner := reg.get("bot-1")

	cluster.putBot(raftcluster.Bot{ID: "bot-1", State: raftcluster.BotStateDisabled})

	waitFor(t, time.Second, func() bool {
		ids := m.RunningBotIDs()
		return len(ids) == 1 && ids[0] == "bot-2"
	}, "bot-1 runner not stopped after being disabled")
	if !bot1Runner.isStopped() {
		t.Fatalf("bot-1 runner not stopped")
	}
}

func TestManager_BotBrokenOnLeaderStopsItsRunner(t *testing.T) {
	cluster := newFakeCluster()
	cluster.putBot(enabledBot("bot-1"))
	reg := newRunnerRegistry()

	m := startManager(t, cluster, reg)
	cluster.setLeader(true)
	waitFor(t, time.Second, func() bool { return len(m.RunningBotIDs()) == 1 }, "bot-1 did not start")

	runner := reg.get("bot-1")

	cluster.putBot(raftcluster.Bot{
		ID: "bot-1", State: raftcluster.BotStateBroken,
		LastFailureClass: raftcluster.FailureClassBot, LastFailureReason: "401",
	})

	waitFor(t, time.Second, func() bool { return len(m.RunningBotIDs()) == 0 }, "bot-1 runner not stopped after breaking")
	if !runner.isStopped() {
		t.Fatalf("bot-1 runner not stopped after breaking")
	}
}

func TestManager_NonLeaderNeverStartsRunners(t *testing.T) {
	cluster := newFakeCluster()
	cluster.putBot(enabledBot("bot-1"))
	reg := newRunnerRegistry()

	m := startManager(t, cluster, reg)

	// даём Manager несколько циклов reconcile на то, чтобы ошибочно
	// стартовать раннер, если бы логика была неверной.
	for i := 0; i < 5; i++ {
		time.Sleep(10 * time.Millisecond)
		if ids := m.RunningBotIDs(); len(ids) != 0 {
			t.Fatalf("RunningBotIDs = %v while never having been leader", ids)
		}
	}
	if reg.get("bot-1") != nil {
		t.Fatalf("a runner was created for a node that was never leader")
	}
}

func TestManager_RunnerStartFailureIsNotRegisteredAsRunning(t *testing.T) {
	cluster := newFakeCluster()
	cluster.putBot(enabledBot("bot-1"))
	reg := newRunnerRegistry()
	reg.failNextStart("bot-1")

	m := startManager(t, cluster, reg)
	cluster.setLeader(true)

	// даём время на попытку старта, которая должна провалиться.
	time.Sleep(50 * time.Millisecond)
	if ids := m.RunningBotIDs(); len(ids) != 0 {
		t.Fatalf("RunningBotIDs = %v, want empty after failed start", ids)
	}
}
