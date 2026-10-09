package failover

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

type fakeCluster struct {
	leader      bool
	nodes       []raftcluster.NodeInfo
	transferred []string
	transferErr error
}

func (c *fakeCluster) IsLeader() bool                    { return c.leader }
func (c *fakeCluster) ID() string                        { return "n1" }
func (c *fakeCluster) ListNodes() []raftcluster.NodeInfo { return c.nodes }
func (c *fakeCluster) TransferLeadershipTo(id, _ string) error {
	if c.transferErr != nil {
		return c.transferErr
	}
	c.transferred = append(c.transferred, id)
	return nil
}

// fakeProber reports, per peer address, whether that peer reaches Telegram
// for each bot.
type fakeProber struct {
	mu     sync.Mutex
	reach  map[string]map[string]bool // addr -> botID -> ok
	probes []string
}

func (p *fakeProber) ProbeTelegram(_ context.Context, addr, botID string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probes = append(p.probes, addr+"/"+botID)
	byBot, ok := p.reach[addr]
	if !ok {
		return false, errors.New("peer unreachable")
	}
	return byBot[botID], nil
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func setup(cfg Config) (*Monitor, *fakeCluster, *fakeProber, *clock) {
	cl := &fakeCluster{leader: true, nodes: []raftcluster.NodeInfo{
		{ID: "n1", RaftAddr: "r1", GRPCAddr: "g1"},
		{ID: "n2", RaftAddr: "r2", GRPCAddr: "g2"},
		{ID: "n3", RaftAddr: "r3", GRPCAddr: "g3"},
	}}
	pr := &fakeProber{reach: map[string]map[string]bool{}}
	ck := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	m := New(cfg, cl, pr, nil)
	m.now = ck.now
	return m, cl, pr, ck
}

func TestCheck_TransfersToPeerThatReachesTelegram(t *testing.T) {
	m, cl, pr, ck := setup(Config{FailureWindow: time.Minute})
	pr.reach["g2"] = map[string]bool{"a": false, "b": true} // n2 тоже не видит бота a
	pr.reach["g3"] = map[string]bool{"a": true, "b": true}

	m.TelegramUnreachable("a", nil)
	m.TelegramUnreachable("b", nil)
	ck.add(30 * time.Second)
	if m.Check(context.Background()) {
		t.Fatal("transferred before the failure window elapsed")
	}
	ck.add(31 * time.Second)
	if !m.Check(context.Background()) {
		t.Fatal("no transfer after the failure window")
	}
	if len(cl.transferred) != 1 || cl.transferred[0] != "n3" {
		t.Fatalf("transferred to %v, want n3", cl.transferred)
	}

	// Cooldown: повторная проверка сразу ничего не делает.
	ck.add(2 * time.Minute)
	if m.Check(context.Background()) {
		t.Fatal("transferred again within cooldown")
	}
}

func TestCheck_SingleFailingBotAmongHealthyOnesIsNotANodeProblem(t *testing.T) {
	m, cl, pr, ck := setup(Config{FailureWindow: time.Minute})
	pr.reach["g2"] = map[string]bool{"a": true}
	m.TelegramUnreachable("a", nil)
	m.TelegramReachable("b")
	m.TelegramReachable("c")
	ck.add(2 * time.Minute)
	if m.Check(context.Background()) || len(cl.transferred) != 0 {
		t.Fatal("one failing bot out of three triggered a transfer")
	}
	if len(pr.probes) != 0 {
		t.Fatalf("peers probed: %v", pr.probes)
	}
}

func TestCheck_RecoveryResetsStreak(t *testing.T) {
	m, cl, pr, ck := setup(Config{FailureWindow: time.Minute})
	pr.reach["g2"] = map[string]bool{"a": true}
	m.TelegramUnreachable("a", nil)
	ck.add(50 * time.Second)
	m.TelegramReachable("a")
	m.TelegramUnreachable("a", nil)
	ck.add(50 * time.Second)
	if m.Check(context.Background()) || len(cl.transferred) != 0 {
		t.Fatal("streak was not reset by a successful call")
	}
}

func TestCheck_NoHealthyPeerKeepsLeadership(t *testing.T) {
	m, cl, pr, ck := setup(Config{FailureWindow: time.Minute})
	pr.reach["g2"] = map[string]bool{"a": false}
	// g3 недоступен вовсе
	m.TelegramUnreachable("a", nil)
	ck.add(2 * time.Minute)
	if m.Check(context.Background()) || len(cl.transferred) != 0 {
		t.Fatal("transferred although no peer reaches telegram")
	}
	// Следующая попытка — не раньше чем через FailureWindow.
	probes := len(pr.probes)
	ck.add(10 * time.Second)
	m.Check(context.Background())
	if len(pr.probes) != probes {
		t.Fatal("peers probed again too soon")
	}
	// Сосед ожил — передаём.
	pr.reach["g3"] = map[string]bool{"a": true}
	ck.add(time.Minute)
	if !m.Check(context.Background()) || cl.transferred[0] != "n3" {
		t.Fatalf("transferred = %v", cl.transferred)
	}
}

func TestCheck_FollowerAndStoppedBotsDoNothing(t *testing.T) {
	m, cl, pr, ck := setup(Config{FailureWindow: time.Minute})
	pr.reach["g2"] = map[string]bool{"a": true}
	m.TelegramUnreachable("a", nil)
	ck.add(2 * time.Minute)
	cl.leader = false
	if m.Check(context.Background()) {
		t.Fatal("follower transferred leadership")
	}
	cl.leader = true
	m.BotStopped("a")
	if m.Check(context.Background()) {
		t.Fatal("stopped bot still counted")
	}
}

func TestCheck_MinFailingBots(t *testing.T) {
	m, cl, pr, ck := setup(Config{FailureWindow: time.Minute, MinFailingBots: 2})
	pr.reach["g2"] = map[string]bool{"a": true, "b": true}
	m.TelegramUnreachable("a", nil)
	ck.add(2 * time.Minute)
	if m.Check(context.Background()) {
		t.Fatal("transferred with fewer than MinFailingBots failing")
	}
	m.TelegramUnreachable("b", nil)
	ck.add(2 * time.Minute)
	if !m.Check(context.Background()) || cl.transferred[0] != "n2" {
		t.Fatalf("transferred = %v", cl.transferred)
	}
}

func TestCheck_WindowCountsFromLastResponse(t *testing.T) {
	m, cl, pr, ck := setup(Config{FailureWindow: 15 * time.Second})
	pr.reach["g2"] = map[string]bool{"a": true}
	m.TelegramReachable("a")
	// Зависший long poll сообщает о сбое только по таймауту — через 35 с
	// после последнего ответа; окно к этому моменту уже истекло.
	ck.add(35 * time.Second)
	m.TelegramUnreachable("a", nil)
	if !m.Check(context.Background()) || cl.transferred[0] != "n2" {
		t.Fatalf("transferred = %v", cl.transferred)
	}
}

func TestCheck_SelfCheckSuccessKeepsLeadership(t *testing.T) {
	m, cl, pr, ck := setup(Config{FailureWindow: 15 * time.Second})
	pr.reach["g1"] = map[string]bool{"a": true} // свежий getMe с самого узла проходит
	pr.reach["g2"] = map[string]bool{"a": true}
	m.TelegramReachable("a")
	ck.add(30 * time.Second)
	m.TelegramUnreachable("a", nil)
	if m.Check(context.Background()) || len(cl.transferred) != 0 {
		t.Fatalf("one failed call moved leadership: %v", cl.transferred)
	}
	if len(pr.probes) != 1 || pr.probes[0] != "g1/a" {
		t.Fatalf("probes = %v, want only the self-check", pr.probes)
	}
}
