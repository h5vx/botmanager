package rpcserver

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/hashicorp/raft"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

func ptrU64(v uint64) *uint64 { return &v }

// TestSubscribe_ResumeAfterSequence: a subscriber that remembers the last
// sequence it saw gets exactly the following events on reconnect, including
// Telegram updates recorded through Raft.
func TestSubscribe_ResumeAfterSequence(t *testing.T) {
	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClients(t, node)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "A", Token: "123:a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := botAdmin.SetBotState(ctx, &botmanagerpb.SetBotStateRequest{Id: bot.GetId(), State: botmanagerpb.BotState_BOT_STATE_ENABLED}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Apply(raftcluster.Command{Type: raftcluster.CommandRecordUpdates, RecordUpdates: &raftcluster.RecordUpdatesCommand{
		BotID: bot.GetId(), NextOffset: 2,
		Updates: []raftcluster.IncomingUpdate{{Kind: raftcluster.JournalIncomingMessage, UpdateID: 1, ChatID: 5, Text: "hello", ReceivedAt: time.Now()}},
	}}, time.Second); err != nil {
		t.Fatal(err)
	}

	stream, err := messaging.Subscribe(ctx, &botmanagerpb.SubscribeRequest{AfterSequence: ptrU64(0)})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.GetBotStateChanged() == nil || first.GetSequence() != 1 {
		t.Fatalf("first = %+v", first)
	}
	second, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if second.GetIncomingMessage().GetText() != "hello" || second.GetSequence() != 2 {
		t.Fatalf("second = %+v", second)
	}

	resumed, err := messaging.Subscribe(ctx, &botmanagerpb.SubscribeRequest{AfterSequence: ptrU64(first.GetSequence())})
	if err != nil {
		t.Fatal(err)
	}
	again, err := resumed.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if again.GetSequence() != 2 || again.GetIncomingMessage() == nil {
		t.Fatalf("resumed first = %+v", again)
	}
}

// TestSubscribe_OutOfRangeWhenHistoryEvicted: resuming from a sequence the
// journal no longer retains fails loudly instead of silently skipping.
func TestSubscribe_OutOfRangeWhenHistoryEvicted(t *testing.T) {
	node := newTestNodeWith(t, func(c *raftcluster.Config) { c.JournalRetention = 2 })
	botAdmin, messaging, _ := newTestClients(t, node)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "A", Token: "123:a"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{IdempotencyKey: fmt.Sprintf("k%d", i), BotId: bot.GetId(), ChatId: 1, Text: "x"}); err != nil {
			t.Fatal(err)
		}
	}

	stream, err := messaging.Subscribe(ctx, &botmanagerpb.SubscribeRequest{AfterSequence: ptrU64(0)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.OutOfRange {
		t.Fatalf("Recv err = %v, want OutOfRange", err)
	}

	// Без after_sequence — только новые события, без ошибки.
	live, err := messaging.Subscribe(ctx, &botmanagerpb.SubscribeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if hdr, err := live.Header(); err != nil || hdr.Get(botmanagerpb.SubscribePositionHeader)[0] != "4" {
		t.Fatalf("position header = %v, %v", hdr, err)
	}
	if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{IdempotencyKey: "k-live", BotId: bot.GetId(), ChatId: 1, Text: "x"}); err != nil {
		t.Fatal(err)
	}
	upd, err := live.Recv()
	if err != nil || upd.GetMessageStatusChanged().GetIdempotencyKey() != "k-live" || upd.GetSequence() != 5 {
		t.Fatalf("live = %+v, %v", upd, err)
	}
}

// testClusterNode is one node of an in-process cluster: a Raft node plus a
// full gRPC server (with the forwarding interceptor and health service) on
// an in-memory listener.
type testClusterNode struct {
	node     *raftcluster.Node
	addr     string // gRPC "address", resolved through the shared dialer
	botAdmin botmanagerpb.BotAdminClient
	msg      botmanagerpb.MessagingClient
	maint    botmanagerpb.MaintenanceClient
}

type testNet struct {
	listeners map[string]*bufconn.Listener
}

func (n *testNet) dial(_ context.Context, addr string) (net.Conn, error) {
	l, ok := n.listeners[addr]
	if !ok {
		return nil, fmt.Errorf("no such node %q", addr)
	}
	return l.Dial()
}

func (n *testNet) dialOpts() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithContextDialer(n.dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func startClusterNode(t *testing.T, tn *testNet, node *raftcluster.Node, addr string) *testClusterNode {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	tn.listeners[addr] = lis

	fw, err := NewForwarder(node, tn.dialOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fw.Close)

	server := grpc.NewServer(grpc.ChainUnaryInterceptor(fw.UnaryInterceptor))
	botmanagerpb.RegisterBotAdminServer(server, NewBotAdminServer(node, raftcluster.ProxyConfig{}, "", 0, nil))
	botmanagerpb.RegisterMessagingServer(server, NewMessagingServer(node, raftcluster.ProxyConfig{}, "", 0, nil))
	maint := NewMaintenanceServer(node, raftcluster.ProxyConfig{}, "", 0, nil)
	maint.SetPeerDialer(fw)
	botmanagerpb.RegisterMaintenanceServer(server, maint)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(server, hs)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///"+addr, tn.dialOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &testClusterNode{
		node:     node,
		addr:     addr,
		botAdmin: botmanagerpb.NewBotAdminClient(conn),
		msg:      botmanagerpb.NewMessagingClient(conn),
		maint:    botmanagerpb.NewMaintenanceClient(conn),
	}
}

func openInmemNode(t *testing.T, id string, trans *raft.InmemTransport, bootstrap bool, servers []raft.Server, grpcAddr string, peers []raftcluster.NodeInfo) *raftcluster.Node {
	t.Helper()
	store := raft.NewInmemStore()
	node, err := raftcluster.Open(raftcluster.Config{
		NodeID:         id,
		Bootstrap:      bootstrap,
		BootstrapPeers: servers,
		Self:           raftcluster.NodeInfo{GRPCAddr: grpcAddr},
		KnownPeers:     peers,
		RaftConfig:     testRaftConfig(),
		Deps: raftcluster.Dependencies{
			Transport:     trans,
			LogStore:      store,
			StableStore:   store,
			SnapshotStore: raft.NewInmemSnapshotStore(),
		},
	})
	if err != nil {
		t.Fatalf("Open %s: %v", id, err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	return node
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", msg)
}

// TestCluster_FollowerForwardsWritesAndServesSubscribe runs three nodes,
// each with its own gRPC server, and talks only to followers: writes are
// forwarded to the leader, reads and Subscribe are served locally from the
// replicated state, and GetClusterStatus probes real peers. Then a fourth
// node joins through AddNode and is removed through RemoveNode.
func TestCluster_FollowerForwardsWritesAndServesSubscribe(t *testing.T) {
	const n = 4 // три стартовых узла + один для AddNode
	tn := &testNet{listeners: map[string]*bufconn.Listener{}}
	addrs := make([]raft.ServerAddress, n)
	trans := make([]*raft.InmemTransport, n)
	for i := 0; i < n; i++ {
		addrs[i], trans[i] = raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("raft-%d", i)))
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				trans[i].Connect(addrs[j], trans[j])
			}
		}
	}
	var servers []raft.Server
	var peers []raftcluster.NodeInfo
	for i := 0; i < 3; i++ {
		servers = append(servers, raft.Server{ID: raft.ServerID(fmt.Sprintf("n%d", i)), Address: addrs[i]})
		peers = append(peers, raftcluster.NodeInfo{ID: fmt.Sprintf("n%d", i), RaftAddr: string(addrs[i]), GRPCAddr: fmt.Sprintf("grpc-%d", i)})
	}

	nodes := make([]*testClusterNode, 3)
	for i := 0; i < 3; i++ {
		rn := openInmemNode(t, fmt.Sprintf("n%d", i), trans[i], true, servers, fmt.Sprintf("grpc-%d", i), peers)
		nodes[i] = startClusterNode(t, tn, rn, fmt.Sprintf("grpc-%d", i))
	}

	var leader *testClusterNode
	var followers []*testClusterNode
	waitUntil(t, 5*time.Second, func() bool {
		leader, followers = nil, nil
		for _, cn := range nodes {
			if cn.node.IsLeader() {
				leader = cn
			} else {
				followers = append(followers, cn)
			}
		}
		return leader != nil && len(followers) == 2
	}, "leader elected")
	waitUntil(t, 5*time.Second, func() bool {
		for _, cn := range nodes {
			if len(cn.node.ListNodes()) != 3 {
				return false
			}
		}
		return true
	}, "node registry replicated")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := followers[0]

	stream, err := followers[1].msg.Subscribe(ctx, &botmanagerpb.SubscribeRequest{AfterSequence: ptrU64(0)})
	if err != nil {
		t.Fatal(err)
	}

	bot, err := f.botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "via follower", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot on follower: %v", err)
	}
	if _, err := f.msg.Send(ctx, &botmanagerpb.SendRequest{IdempotencyKey: "fw-1", BotId: bot.GetId(), ChatId: 1, Text: "hi"}); err != nil {
		t.Fatalf("Send on follower: %v", err)
	}
	if _, ok := leader.node.GetMessage("fw-1"); !ok {
		t.Fatalf("message forwarded by follower is missing on the leader")
	}

	upd, err := stream.Recv()
	if err != nil {
		t.Fatalf("Subscribe on follower: %v", err)
	}
	if upd.GetMessageStatusChanged().GetIdempotencyKey() != "fw-1" || upd.GetSequence() == 0 {
		t.Fatalf("follower stream update = %+v", upd)
	}

	st, err := f.maint.GetClusterStatus(ctx, &botmanagerpb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if len(st.GetNodes()) != 3 {
		t.Fatalf("nodes = %+v", st.GetNodes())
	}
	for _, ns := range st.GetNodes() {
		if !ns.GetReachable() || ns.GetGrpcAddress() == "" || ns.GetRaftAddress() == "" {
			t.Fatalf("node status = %+v", ns)
		}
	}

	// AddNode через фолловера (пересылается лидеру).
	joiner := openInmemNode(t, "n3", trans[3], false, nil, "grpc-3", nil)
	startClusterNode(t, tn, joiner, "grpc-3")
	st, err = f.maint.AddNode(ctx, &botmanagerpb.AddNodeRequest{NodeId: "n3", RaftAddress: string(addrs[3]), GrpcAddress: "grpc-3"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if len(st.GetNodes()) != 4 {
		t.Fatalf("nodes after AddNode = %+v", st.GetNodes())
	}
	waitUntil(t, 5*time.Second, func() bool { _, ok := joiner.GetMessage("fw-1"); return ok }, "state replicated to joined node")

	st, err = f.maint.RemoveNode(ctx, &botmanagerpb.RemoveNodeRequest{NodeId: "n3"})
	if err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if len(st.GetNodes()) != 3 {
		t.Fatalf("nodes after RemoveNode = %+v", st.GetNodes())
	}
	if _, err := f.maint.RemoveNode(ctx, &botmanagerpb.RemoveNodeRequest{NodeId: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("RemoveNode(unknown) = %v, want NotFound", err)
	}
}
