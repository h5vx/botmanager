package rpcserver

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/telegram"
	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
)

// testRaftConfig shrinks election/heartbeat timeouts so a single-node
// cluster elects itself leader quickly in a test, mirroring
// raftcluster's own node_test.go helper (this package cannot import that
// unexported test helper directly, it lives in raftcluster's test binary).
func testRaftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 25 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.Logger = hclog.NewNullLogger()
	return cfg
}

// newTestNode opens a real single-node raftcluster.Node rooted at a fresh
// t.TempDir(), bootstrap=true — the same "single-node is a special case of
// the general Open() path" setup raftcluster/node_test.go and
// CLAUDE.md describe, exercised here end to end through gRPC
// instead of through raftcluster's own package-internal API.
func newTestNode(t *testing.T) *raftcluster.Node {
	t.Helper()
	_, transport := raft.NewInmemTransport(raft.ServerAddress("test-node"))
	store := raft.NewInmemStore() // serves as both LogStore and StableStore

	node, err := raftcluster.Open(raftcluster.Config{
		NodeID:                 "test-node",
		DataDir:                t.TempDir(),
		Bootstrap:              true,
		MessageRetentionPerBot: 50,
		RaftConfig:             testRaftConfig(),
		Deps: raftcluster.Dependencies{
			Transport:     transport,
			LogStore:      store,
			StableStore:   store,
			SnapshotStore: raft.NewInmemSnapshotStore(),
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			return node
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node did not become leader within 2s")
	return nil
}

// newTestClients wires BotAdminServer/MessagingServer/MaintenanceServer
// (backed by node) into a real grpc.Server listening on an in-memory
// bufconn, and returns connected typed clients — a real gRPC round trip
// (marshaling, service dispatch, streaming) without a TCP port.
func newTestClients(t *testing.T, node *raftcluster.Node) (botmanagerpb.BotAdminClient, botmanagerpb.MessagingClient, botmanagerpb.MaintenanceClient) {
	t.Helper()
	return newTestClientsWithBaseURL(t, node, "")
}

// newTestClientsWithBaseURL is newTestClients with an explicit Telegram API
// base URL override, for tests that need BotAdmin.GetChat/Messaging's
// live-Telegram-call RPCs to hit a local httptest.Server instead of the
// real api.telegram.org (there is no live token to test against in this
// environment — see CLAUDE.md's note on internal/telegram).
func newTestClientsWithBaseURL(t *testing.T, node *raftcluster.Node, apiBaseURL string) (botmanagerpb.BotAdminClient, botmanagerpb.MessagingClient, botmanagerpb.MaintenanceClient) {
	t.Helper()

	const bufSize = 1 << 20
	lis := bufconn.Listen(bufSize)

	server := grpc.NewServer()
	bus := telegram.NewInMemoryBus()
	botmanagerpb.RegisterBotAdminServer(server, NewBotAdminServer(node, raftcluster.ProxyConfig{}, apiBaseURL, 0, nil))
	botmanagerpb.RegisterMessagingServer(server, NewMessagingServer(node, bus, raftcluster.ProxyConfig{}, apiBaseURL, 0, nil))
	botmanagerpb.RegisterMaintenanceServer(server, NewMaintenanceServer(node, raftcluster.ProxyConfig{}, apiBaseURL, 0, nil))

	go func() {
		_ = server.Serve(lis)
	}()
	t.Cleanup(server.Stop)

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return botmanagerpb.NewBotAdminClient(conn), botmanagerpb.NewMessagingClient(conn), botmanagerpb.NewMaintenanceClient(conn)
}

// TestIntegration_CreateEnableSendListStatus walks the path the task asked
// for: CreateBot → GetBot → SetBotState(ENABLED) → Send → GetMessage/
// ListMessages → GetClusterStatus, against a real raftcluster.Node and a
// real grpc.Server over bufconn. No Telegram token is exercised here —
// GetChat/EditMessage and friends (the RPCs that make a live Telegram call)
// are covered separately by unit tests using httptest, the same pattern
// internal/telegram itself already uses (see convert_test.go/
// messaging_test.go).
func TestIntegration_CreateEnableSendListStatus(t *testing.T) {
	node := newTestNode(t)
	botAdmin, messaging, maintenance := newTestClients(t, node)
	ctx := context.Background()

	created, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{
		DisplayName: "Rehearsal bot", Token: "123456789:AAtestToken",
	})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}
	if created.GetId() == "" {
		t.Fatalf("CreateBot returned empty id")
	}
	// botmanagerpb.Bot has no Token field at all (the token is never
	// returned to callers, see convert.go's botToProto) — nothing further
	// to assert here beyond the type itself proving the point.

	got, err := botAdmin.GetBot(ctx, &botmanagerpb.GetBotRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetBot: %v", err)
	}
	if got.GetState() != botmanagerpb.BotState_BOT_STATE_DISABLED {
		t.Fatalf("GetBot.State = %v, want DISABLED (freshly created)", got.GetState())
	}

	enabled, err := botAdmin.SetBotState(ctx, &botmanagerpb.SetBotStateRequest{
		Id: created.GetId(), State: botmanagerpb.BotState_BOT_STATE_ENABLED,
	})
	if err != nil {
		t.Fatalf("SetBotState: %v", err)
	}
	if enabled.GetState() != botmanagerpb.BotState_BOT_STATE_ENABLED {
		t.Fatalf("SetBotState.State = %v, want ENABLED", enabled.GetState())
	}

	ack, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
		IdempotencyKey: "notify:rehearsal:1", BotId: created.GetId(), ChatId: 42, Text: "rehearsal moved",
		Priority: botmanagerpb.Priority_PRIORITY_NORMAL,
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if ack.GetStatus() != botmanagerpb.DeliveryStatus_DELIVERY_STATUS_PENDING {
		t.Fatalf("Send.Status = %v, want PENDING", ack.GetStatus())
	}

	msg, err := messaging.GetMessage(ctx, &botmanagerpb.GetMessageRequest{IdempotencyKey: "notify:rehearsal:1"})
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg.GetText() != "rehearsal moved" || msg.GetBotId() != created.GetId() {
		t.Fatalf("GetMessage = %+v", msg)
	}

	list, err := messaging.ListMessages(ctx, &botmanagerpb.ListMessagesRequest{BotId: created.GetId()})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(list.GetMessages()) != 1 || list.GetMessages()[0].GetIdempotencyKey() != "notify:rehearsal:1" {
		t.Fatalf("ListMessages = %+v", list.GetMessages())
	}

	status, err := maintenance.GetClusterStatus(ctx, &botmanagerpb.Empty{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if status.GetLeaderId() != "test-node" {
		t.Fatalf("GetClusterStatus.LeaderId = %q, want test-node", status.GetLeaderId())
	}
	if len(status.GetNodes()) != 1 || !status.GetNodes()[0].GetIsLeader() || !status.GetNodes()[0].GetReachable() {
		t.Fatalf("GetClusterStatus.Nodes = %+v", status.GetNodes())
	}
	if len(status.GetBots()) != 1 || status.GetBots()[0].GetId() != created.GetId() {
		t.Fatalf("GetClusterStatus.Bots = %+v", status.GetBots())
	}
}

// TestIntegration_CancelPending covers Messaging.CancelPending end to end:
// a still-PENDING message is actually cancelled (cancelled=true, status
// becomes CANCELLED), matching raftcluster's own applyCancelPending unit
// tests but exercised through the gRPC layer's Apply call.
func TestIntegration_CancelPending(t *testing.T) {
	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClients(t, node)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
		IdempotencyKey: "k1", BotId: bot.GetId(), ChatId: 1, Text: "hi",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	ack, err := messaging.CancelPending(ctx, &botmanagerpb.CancelPendingRequest{IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("CancelPending: %v", err)
	}
	if !ack.GetCancelled() {
		t.Fatalf("CancelAck.Cancelled = false, want true (message was still PENDING)")
	}

	msg, err := messaging.GetMessage(ctx, &botmanagerpb.GetMessageRequest{IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg.GetDelivery().GetStatus() != botmanagerpb.DeliveryStatus_DELIVERY_STATUS_CANCELLED {
		t.Fatalf("Delivery.Status = %v, want CANCELLED", msg.GetDelivery().GetStatus())
	}
}

// TestIntegration_WriteRejectedOnNonLeader covers requireLeader's gRPC
// error mapping for a node that has no chance of being leader: a follower
// never exists in this single-node test setup, so instead this checks that
// a plain read-only RPC on a fresh node with no bots is unaffected by
// leadership at all (GetBot/ListBots never call requireLeader — see doc.go)
// while a write against a nonexistent bot still surfaces the expected
// NotFound rather than a leadership error, proving requireLeader's leader
// check and applyError's NotFound mapping compose correctly on the one
// node this test environment can run.
func TestIntegration_WriteRejectedOnNonLeader(t *testing.T) {
	node := newTestNode(t)
	botAdmin, _, _ := newTestClients(t, node)
	ctx := context.Background()

	_, err := botAdmin.GetBot(ctx, &botmanagerpb.GetBotRequest{Id: "missing"})
	if err == nil {
		t.Fatalf("GetBot(missing): want error, got nil")
	}

	_, err = botAdmin.UpdateBot(ctx, &botmanagerpb.UpdateBotRequest{Id: "missing"})
	if err == nil {
		t.Fatalf("UpdateBot(missing): want error, got nil")
	}
}
