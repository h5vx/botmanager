package rpcserver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/raft"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

// newInmemMaintenanceCluster builds an n-node Raft cluster wired via
// raft.NewInmemTransport, the same recipe raftcluster/node_test.go's
// (unexported, package-private) newInmemCluster uses — this package cannot
// import that helper directly (same constraint integration_test.go's
// newTestNode documents), so it is rebuilt here from raftcluster's exported
// Open/Config/Dependencies surface. Returns the nodes and their transports
// (the latter needed by TestMaintenance_TransferLeadership_TransferFails to
// simulate an unreachable target).
func newInmemMaintenanceCluster(t *testing.T, n int) ([]*raftcluster.Node, []*raft.InmemTransport) {
	t.Helper()

	addrs := make([]raft.ServerAddress, n)
	transports := make([]*raft.InmemTransport, n)
	for i := 0; i < n; i++ {
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("m-node-%d", i)))
		addrs[i] = addr
		transports[i] = trans
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	servers := make([]raft.Server, n)
	for i := 0; i < n; i++ {
		servers[i] = raft.Server{ID: raft.ServerID(fmt.Sprintf("m-node-%d", i)), Address: addrs[i]}
	}

	nodes := make([]*raftcluster.Node, n)
	for i := 0; i < n; i++ {
		store := raft.NewInmemStore() // serves as both LogStore and StableStore
		node, err := raftcluster.Open(raftcluster.Config{
			NodeID:                 fmt.Sprintf("m-node-%d", i),
			Bootstrap:              true,
			BootstrapPeers:         servers,
			MessageRetentionPerBot: 10,
			RaftConfig:             testRaftConfig(),
			Deps: raftcluster.Dependencies{
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
	return nodes, transports
}

func waitForMaintenanceLeader(t *testing.T, nodes []*raftcluster.Node, timeout time.Duration) *raftcluster.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*raftcluster.Node
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
	t.Fatal("no single leader elected within timeout")
	return nil
}

func waitForCond(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

// TestMaintenance_TransferLeadership_UnknownNode covers the NOT_FOUND case
// target_node_id not part of the cluster.
func TestMaintenance_TransferLeadership_UnknownNode(t *testing.T) {
	node := newTestNode(t) // single node, always leader (see integration_test.go)
	srv := NewMaintenanceServer(node, raftcluster.ProxyConfig{}, "", 0, nil)

	_, err := srv.TransferLeadership(context.Background(), &botmanagerpb.TransferLeadershipRequest{TargetNodeId: "does-not-exist"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

// TestMaintenance_TransferLeadership_NotLeader covers the FAILED_PRECONDITION
// case: the RPC lands on a follower, which must name the real leader
// (requireLeader — same helper the write RPCs use).
func TestMaintenance_TransferLeadership_NotLeader(t *testing.T) {
	nodes, _ := newInmemMaintenanceCluster(t, 3)
	leader := waitForMaintenanceLeader(t, nodes, 2*time.Second)

	var follower *raftcluster.Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	srv := NewMaintenanceServer(follower, raftcluster.ProxyConfig{}, "", 0, nil)
	_, err := srv.TransferLeadership(context.Background(), &botmanagerpb.TransferLeadershipRequest{TargetNodeId: leader.ID()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

// TestMaintenance_TransferLeadership_Succeeds covers the happy path: the
// leader hands mastership to a named follower via Raft's own
// LeadershipTransferToServer, and the target actually becomes leader.
func TestMaintenance_TransferLeadership_Succeeds(t *testing.T) {
	nodes, _ := newInmemMaintenanceCluster(t, 3)
	leader := waitForMaintenanceLeader(t, nodes, 2*time.Second)

	var target *raftcluster.Node
	for _, n := range nodes {
		if n != leader {
			target = n
			break
		}
	}

	srv := NewMaintenanceServer(leader, raftcluster.ProxyConfig{}, "", 0, nil)
	if _, err := srv.TransferLeadership(context.Background(), &botmanagerpb.TransferLeadershipRequest{TargetNodeId: target.ID()}); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}

	waitForCond(t, 2*time.Second, target.IsLeader, "target node did not become leader after transfer")
	waitForCond(t, 2*time.Second, func() bool { return !leader.IsLeader() }, "old leader did not step down after transfer")
}

// TestMaintenance_TransferLeadership_TransferFails covers the UNAVAILABLE
// case: the target is a real cluster member (passes the NOT_FOUND check)
// but unreachable, so Raft's own transfer mechanism cannot complete it.
// Disconnecting the leader's in-memory transport from the target's address
// simulates that without needing a real network.
func TestMaintenance_TransferLeadership_TransferFails(t *testing.T) {
	nodes, transports := newInmemMaintenanceCluster(t, 3)
	leader := waitForMaintenanceLeader(t, nodes, 2*time.Second)

	leaderIdx, targetIdx := -1, -1
	for i, n := range nodes {
		if n == leader {
			leaderIdx = i
		} else if targetIdx == -1 {
			targetIdx = i
		}
	}
	if leaderIdx == -1 || targetIdx == -1 {
		t.Fatal("could not identify leader/target indices")
	}
	targetID := nodes[targetIdx].ID()

	// Cut the leader's transport connection to the target — TimeoutNow (the
	// RPC hashicorp/raft's leadership transfer sends the target) can no
	// longer be delivered.
	targetAddr := raft.ServerAddress(fmt.Sprintf("m-node-%d", targetIdx))
	transports[leaderIdx].Disconnect(targetAddr)

	srv := NewMaintenanceServer(leader, raftcluster.ProxyConfig{}, "", 0, nil)
	_, err := srv.TransferLeadership(context.Background(), &botmanagerpb.TransferLeadershipRequest{TargetNodeId: targetID})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable (err=%v)", status.Code(err), err)
	}
}

// TestMaintenance_PingTelegram_Direct_Success covers the bot_id == "" branch
// ("прямой канал по умолчанию") against a fake Telegram host.
func TestMaintenance_PingTelegram_Direct_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound) // real api.telegram.org answers this way to a bare GET too
	}))
	t.Cleanup(srv.Close)

	node := newTestNode(t)
	m := NewMaintenanceServer(node, raftcluster.ProxyConfig{}, srv.URL, 2*time.Second, nil)

	result, err := m.PingTelegram(context.Background(), &botmanagerpb.PingTelegramRequest{})
	if err != nil {
		t.Fatalf("PingTelegram: %v", err)
	}
	if !result.GetSuccess() {
		t.Fatalf("Success = false, want true (error=%q)", result.GetError())
	}
	if result.GetHttpStatus() != http.StatusNotFound {
		t.Fatalf("HttpStatus = %d, want %d", result.GetHttpStatus(), http.StatusNotFound)
	}
	if result.GetProxyUsed() != "" {
		t.Fatalf("ProxyUsed = %q, want empty (no proxy configured)", result.GetProxyUsed())
	}
}

// TestMaintenance_PingTelegram_Direct_NetworkFailure covers the "не висит,
// сообщает об отказе" case: nothing listens at the target address.
func TestMaintenance_PingTelegram_Direct_NetworkFailure(t *testing.T) {
	node := newTestNode(t)
	// Port 0 on loopback: connection refused, no hang.
	m := NewMaintenanceServer(node, raftcluster.ProxyConfig{}, "http://127.0.0.1:1", time.Second, nil)

	result, err := m.PingTelegram(context.Background(), &botmanagerpb.PingTelegramRequest{})
	if err != nil {
		t.Fatalf("PingTelegram: %v", err)
	}
	if result.GetSuccess() {
		t.Fatal("Success = true, want false (nothing listens on that port)")
	}
	if result.GetError() == "" {
		t.Fatal("Error is empty, want a network error message")
	}
}

// TestMaintenance_PingTelegram_UnknownBot covers the NOT_FOUND case for a
// bot_id that does not exist.
func TestMaintenance_PingTelegram_UnknownBot(t *testing.T) {
	node := newTestNode(t)
	m := NewMaintenanceServer(node, raftcluster.ProxyConfig{}, "", time.Second, nil)

	_, err := m.PingTelegram(context.Background(), &botmanagerpb.PingTelegramRequest{BotId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

// TestMaintenance_PingTelegram_ByBot_Success covers the bot_id branch: a
// live getMe call through the bot's own token against a fake Bot API
// server (same fake-server pattern as messaging_test.go's
// newFakeTelegramServer — kept local here since this test does not need the
// rest of that helper's method dispatch).
func TestMaintenance_PingTelegram_ByBot_Success(t *testing.T) {
	srv := newFakeTelegramServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		if method != "getMe" {
			t.Errorf("unexpected method %q", method)
		}
		return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": 1, "is_bot": true}}
	})

	node := newTestNode(t)
	botAdmin, _, maintenance := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	result, err := maintenance.PingTelegram(ctx, &botmanagerpb.PingTelegramRequest{BotId: bot.GetId()})
	if err != nil {
		t.Fatalf("PingTelegram: %v", err)
	}
	if !result.GetSuccess() {
		t.Fatalf("Success = false, want true (error=%q)", result.GetError())
	}
	if result.GetHttpStatus() != http.StatusOK {
		t.Fatalf("HttpStatus = %d, want %d", result.GetHttpStatus(), http.StatusOK)
	}
}

// TestMaintenance_PingTelegram_ByBot_TelegramRejects covers a Telegram error
// response surfacing as success=false with the HTTP status Telegram sent —
// distinguishing "Telegram answered but rejected us" from "no network".
func TestMaintenance_PingTelegram_ByBot_TelegramRejects(t *testing.T) {
	srv := newFakeTelegramServer(t, func(_ string, _ map[string]any) (int, map[string]any) {
		return http.StatusUnauthorized, map[string]any{"ok": false, "error_code": 401, "description": "Unauthorized"}
	})

	node := newTestNode(t)
	botAdmin, _, maintenance := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "bad:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	result, err := maintenance.PingTelegram(ctx, &botmanagerpb.PingTelegramRequest{BotId: bot.GetId()})
	if err != nil {
		t.Fatalf("PingTelegram: %v", err)
	}
	if result.GetSuccess() {
		t.Fatal("Success = true, want false (Telegram rejected the token)")
	}
	if result.GetHttpStatus() != http.StatusUnauthorized {
		t.Fatalf("HttpStatus = %d, want %d", result.GetHttpStatus(), http.StatusUnauthorized)
	}
}

// TestProxyAddressFor_StripsCredentials: a proxy
// address's embedded credentials must never leave this package.
func TestProxyAddressFor_StripsCredentials(t *testing.T) {
	got := proxyAddressFor(raftcluster.ProxyConfig{Enabled: true, Address: "socks5h://user:secret@proxy.example:1080"})
	if got != "socks5h://proxy.example:1080" {
		t.Fatalf("got %q, want no credentials", got)
	}

	if got := proxyAddressFor(raftcluster.ProxyConfig{Enabled: false, Address: "socks5h://user:secret@proxy.example:1080"}); got != "" {
		t.Fatalf("disabled proxy: got %q, want empty", got)
	}
}
