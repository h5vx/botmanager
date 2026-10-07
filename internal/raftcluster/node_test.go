package raftcluster

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// testRaftConfig shrinks election/heartbeat timeouts so the in-memory
// cluster tests below settle in well under a second instead of raft's
// multi-second defaults, and silences raft's own logging.
func testRaftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 25 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.Logger = hclog.NewNullLogger()
	return cfg
}

// newInmemCluster builds n Raft nodes wired together via
// raft.NewInmemTransport (no sockets) with in-memory log/stable/snapshot
// stores (no disk), bootstrapped together as one cluster. This is what
// lets a real multi-node Raft cluster be exercised in a unit test — see
// the Dependencies doc comment in node.go.
func newInmemCluster(t *testing.T, n int) []*Node {
	t.Helper()

	addrs := make([]raft.ServerAddress, n)
	transports := make([]*raft.InmemTransport, n)
	for i := 0; i < n; i++ {
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("node-%d", i)))
		addrs[i] = addr
		transports[i] = trans
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			transports[i].Connect(addrs[j], transports[j])
		}
	}

	servers := make([]raft.Server, n)
	for i := 0; i < n; i++ {
		servers[i] = raft.Server{ID: raft.ServerID(fmt.Sprintf("node-%d", i)), Address: addrs[i]}
	}

	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		store := raft.NewInmemStore() // serves as both LogStore and StableStore
		node, err := Open(Config{
			NodeID:                 fmt.Sprintf("node-%d", i),
			Bootstrap:              true,
			BootstrapPeers:         servers,
			MessageRetentionPerBot: 10,
			RaftConfig:             testRaftConfig(),
			Deps: Dependencies{
				Transport:     transports[i],
				LogStore:      store,
				StableStore:   store,
				SnapshotStore: raft.NewInmemSnapshotStore(),
			},
		})
		if err != nil {
			t.Fatalf("Open node %d: %v", i, err)
		}
		nodes[i] = node
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			_ = node.Shutdown()
		}
	})

	return nodes
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range nodes {
			if n.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within %s", timeout)
	return nil
}

// TestCluster_ElectsLeaderAndReplicates is the multi-node correctness
// check: a 3-node cluster elects exactly one leader, Apply on the
// leader replicates to every follower's FSM, and followers refuse Apply
// with ErrNotLeader (forwarding writes to the leader belongs to the gRPC
// layer, this package just refuses the wrong-node call).
func TestCluster_ElectsLeaderAndReplicates(t *testing.T) {
	nodes := newInmemCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	res, err := leader.Apply(Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{
		ID: "bot-1", DisplayName: "Cluster bot", Token: "tok", CreatedAt: t1(0),
	}}, time.Second)
	if err != nil {
		t.Fatalf("Apply on leader: %v", err)
	}
	if res.Bot == nil || res.Bot.ID != "bot-1" {
		t.Fatalf("res = %+v", res)
	}

	for i, n := range nodes {
		i, n := i, n
		waitFor(t, 2*time.Second, func() bool {
			b, ok := n.GetBot("bot-1")
			return ok && b.DisplayName == "Cluster bot" && b.State == BotStateDisabled
		}, fmt.Sprintf("node %d did not replicate bot-1", i))
	}

	refused := 0
	for _, n := range nodes {
		if n == leader {
			continue
		}
		_, err := n.Apply(Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-2", CreatedAt: t1(1)}}, time.Second)
		if !errors.Is(err, ErrNotLeader) {
			t.Errorf("follower Apply: err = %v, want ErrNotLeader", err)
			continue
		}
		refused++
	}
	if refused != len(nodes)-1 {
		t.Fatalf("refused = %d, want %d", refused, len(nodes)-1)
	}
}

// TestCluster_LeadershipSubscription_FiresOnInitialElection covers the
// premise that a node learns about becoming master through Subscribe,
// including the very first election (not just later leadership changes) —
// this is exactly the signal internal/botlifecycle.Manager relies on to
// start bot runners for the first time after startup.
func TestCluster_LeadershipSubscription_FiresOnInitialElection(t *testing.T) {
	nodes := newInmemCluster(t, 3)

	type outcome struct {
		received bool
		value    bool
	}
	results := make(chan outcome, len(nodes))
	for _, n := range nodes {
		ch, cancel := n.Subscribe()
		t.Cleanup(cancel)
		go func(ch <-chan bool) {
			select {
			case v := <-ch:
				results <- outcome{received: true, value: v}
			case <-time.After(500 * time.Millisecond):
				results <- outcome{}
			}
		}(ch)
	}

	trueCount := 0
	for range nodes {
		o := <-results
		if o.received && o.value {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Fatalf("trueCount = %d, want exactly 1 (the elected node observing its own initial election)", trueCount)
	}
}

// TestCluster_TransferLeadershipTo: the current
// leader can hand mastership to a specific named follower (not "whoever
// wins the next election") via Raft's own LeadershipTransferToServer, and
// the target actually becomes leader afterward.
func TestCluster_TransferLeadershipTo(t *testing.T) {
	nodes := newInmemCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	var target *Node
	for _, n := range nodes {
		if n != leader {
			target = n
			break
		}
	}
	if target == nil {
		t.Fatal("no non-leader node found in a 3-node cluster")
	}

	servers, err := leader.Configuration()
	if err != nil {
		t.Fatalf("Configuration: %v", err)
	}
	var targetAddr raft.ServerAddress
	for _, srv := range servers {
		if srv.ID == raft.ServerID(target.ID()) {
			targetAddr = srv.Address
		}
	}
	if targetAddr == "" {
		t.Fatalf("target node %s not found in configuration", target.ID())
	}

	if err := leader.TransferLeadershipTo(target.ID(), string(targetAddr)); err != nil {
		t.Fatalf("TransferLeadershipTo: %v", err)
	}

	waitFor(t, 2*time.Second, target.IsLeader, "target node did not become leader after transfer")
	waitFor(t, 2*time.Second, func() bool { return !leader.IsLeader() }, "old leader did not step down after transfer")
}

// TestNode_TransferLeadershipTo_NotLeader covers the follower-side guard:
// only the current leader may initiate a transfer.
func TestNode_TransferLeadershipTo_NotLeader(t *testing.T) {
	nodes := newInmemCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	err := follower.TransferLeadershipTo(leader.ID(), "node-does-not-matter")
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

// TestNode_SubscribeApplied_FiresAfterApply covers the mechanism
// internal/botlifecycle uses to notice bot state changes on the leader
// without raftcluster knowing anything about bot runners.
func TestNode_SubscribeApplied_FiresAfterApply(t *testing.T) {
	nodes := newInmemCluster(t, 1)
	leader := waitForLeader(t, nodes, 2*time.Second)

	ch, cancel := leader.SubscribeApplied()
	defer cancel()

	if _, err := leader.Apply(Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}}, time.Second); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("SubscribeApplied channel did not fire after Apply")
	}
}
