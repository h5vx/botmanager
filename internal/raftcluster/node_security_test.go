package raftcluster

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/h5vx/botmanager/internal/devcerts"
	"github.com/h5vx/botmanager/internal/security"
)

func newInmemNode(t *testing.T, id string, trans *raft.InmemTransport, bootstrap bool, mutate func(*Config)) *Node {
	t.Helper()
	store := raft.NewInmemStore()
	cfg := Config{
		NodeID:     id,
		Bootstrap:  bootstrap,
		RaftConfig: testRaftConfig(),
		Deps: Dependencies{
			Transport:     trans,
			LogStore:      store,
			StableStore:   store,
			SnapshotStore: raft.NewInmemSnapshotStore(),
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	n, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open %s: %v", id, err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	return n
}

// TestNode_TokenEncryptedAtRest: with a TokenCipher the token never reaches
// the replicated state (and therefore the Raft log, snapshots and exports)
// in plaintext, while Node's own read methods return it decrypted.
func TestNode_TokenEncryptedAtRest(t *testing.T) {
	cipher, err := NewTokenCipher(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	_, trans := raft.NewInmemTransport("n1")
	node := newInmemNode(t, "n1", trans, true, func(c *Config) { c.TokenCipher = cipher })
	waitForLeader(t, []*Node{node}, 3*time.Second)

	res, err := node.Apply(Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", Token: "123:secret"}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.Bot.Token != "123:secret" {
		t.Fatalf("Apply result token = %q", res.Bot.Token)
	}
	raw, _ := node.fsm.GetBot("bot-1")
	if !strings.HasPrefix(raw.Token, encryptedTokenPrefix) {
		t.Fatalf("stored token is not encrypted: %q", raw.Token)
	}
	if got, _ := node.GetBot("bot-1"); got.Token != "123:secret" {
		t.Fatalf("GetBot token = %q", got.Token)
	}

	newToken := "456:other"
	if _, err := node.Apply(Command{Type: CommandUpdateBot, UpdateBot: &UpdateBotCommand{ID: "bot-1", Token: &newToken}}, time.Second); err != nil {
		t.Fatal(err)
	}
	if newToken != "456:other" {
		t.Fatalf("caller's command was mutated: %q", newToken)
	}
	if got := node.ListBots(); len(got) != 1 || got[0].Token != "456:other" {
		t.Fatalf("ListBots = %+v", got)
	}

	var buf bytes.Buffer
	if err := node.ExportSnapshot(&buf); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "secret") || strings.Contains(buf.String(), "456:other") {
		t.Fatalf("export contains a plaintext token")
	}
}

// TestNode_RegistersSelfAndPeersOnLeadership: a node that becomes leader
// records its own and its statically known peers' addresses.
func TestNode_RegistersSelfAndPeersOnLeadership(t *testing.T) {
	addr, trans := raft.NewInmemTransport("n1")
	node := newInmemNode(t, "n1", trans, true, func(c *Config) {
		c.BootstrapPeers = []raft.Server{{ID: "n1", Address: addr}}
		c.Self = NodeInfo{GRPCAddr: "10.0.0.1:9090"}
		c.KnownPeers = []NodeInfo{{ID: "n2", RaftAddr: "10.0.0.2:9092", GRPCAddr: "10.0.0.2:9090"}}
	})
	waitForLeader(t, []*Node{node}, 3*time.Second)
	waitFor(t, 3*time.Second, func() bool { return len(node.ListNodes()) == 2 }, "two registry entries")

	self, ok := node.GetNode("n1")
	if !ok || self.GRPCAddr != "10.0.0.1:9090" || self.RaftAddr != string(addr) {
		t.Fatalf("self entry = %+v", self)
	}
	if info, ok := node.LeaderInfo(); !ok || info.ID != "n1" {
		t.Fatalf("LeaderInfo = %+v %v", info, ok)
	}
}

// TestCluster_AddVoterAndRemoveServer: a fresh, non-bootstrapped node joins
// a running cluster through AddVoter, receives the replicated state, and
// leaves it again through RemoveServer.
func TestCluster_AddVoterAndRemoveServer(t *testing.T) {
	addr1, trans1 := raft.NewInmemTransport("n1")
	addr2, trans2 := raft.NewInmemTransport("n2")
	trans1.Connect(addr2, trans2)
	trans2.Connect(addr1, trans1)

	leader := newInmemNode(t, "n1", trans1, true, nil)
	joiner := newInmemNode(t, "n2", trans2, false, nil)
	waitForLeader(t, []*Node{leader}, 3*time.Second)

	if _, err := leader.Apply(Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1"}}, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := joiner.AddVoter(NodeInfo{ID: "x", RaftAddr: "y"}, time.Second); err != ErrNotLeader {
		t.Fatalf("AddVoter on follower = %v, want ErrNotLeader", err)
	}
	if err := leader.AddVoter(NodeInfo{ID: "n2", RaftAddr: string(addr2), GRPCAddr: "n2:9090"}, 2*time.Second); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { _, ok := joiner.GetBot("bot-1"); return ok }, "state replicated to the new node")
	waitFor(t, 3*time.Second, func() bool { n, ok := joiner.GetNode("n2"); return ok && n.GRPCAddr == "n2:9090" }, "registry replicated")

	servers, err := leader.Configuration()
	if err != nil || len(servers) != 2 {
		t.Fatalf("configuration after add = %v, %v", servers, err)
	}

	if err := leader.RemoveServer("n2", 2*time.Second); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	servers, _ = leader.Configuration()
	if len(servers) != 1 {
		t.Fatalf("configuration after remove = %v", servers)
	}
	if _, ok := leader.GetNode("n2"); ok {
		t.Fatalf("n2 still in registry")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// TestCluster_MutualTLSTransport runs a real two-node cluster over TCP with
// the TLS stream layer, and checks that a node whose certificate comes from
// a different CA cannot take part.
func TestCluster_MutualTLSTransport(t *testing.T) {
	ca, err := devcerts.NewCA("test-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	transportTLS := func(ca *devcerts.CA, name string) *TransportTLS {
		leaf, err := ca.Issue(name, []string{"127.0.0.1"}, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		m, err := security.NewMTLS(ca.CertPEM, leaf.CertPEM, leaf.KeyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return &TransportTLS{Server: m.Server, Client: m.Client}
	}

	addrs := []string{freeAddr(t), freeAddr(t)}
	peers := []NodeInfo{{ID: "n0", RaftAddr: addrs[0]}, {ID: "n1", RaftAddr: addrs[1]}}
	nodes := make([]*Node, 2)
	for i := range nodes {
		n, err := Open(Config{
			NodeID:     fmt.Sprintf("n%d", i),
			DataDir:    t.TempDir(),
			BindAddr:   addrs[i],
			TLS:        transportTLS(ca, fmt.Sprintf("n%d", i)),
			Bootstrap:  true,
			KnownPeers: peers,
			RaftConfig: testRaftConfig(),
		})
		if err != nil {
			t.Fatalf("Open n%d: %v", i, err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Shutdown() })
	}

	leader := waitForLeader(t, nodes, 5*time.Second)
	if _, err := leader.Apply(Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1"}}, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		waitFor(t, 3*time.Second, func() bool { _, ok := n.GetBot("bot-1"); return ok }, "replicated over TLS")
	}

	// Узел с сертификатом чужого CA не может подключиться к кластеру.
	rogueCA, err := devcerts.NewCA("rogue-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rogue := transportTLS(rogueCA, "rogue")
	layer := &tlsStreamLayer{client: rogue.Client}
	conn, err := layer.Dial(raft.ServerAddress(addrs[0]), time.Second)
	if err == nil {
		// TLS 1.3: ошибка проверки клиентского сертификата может прийти
		// только при первом чтении.
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, err = conn.Read(make([]byte, 1))
		_ = conn.Close()
	}
	if err == nil {
		t.Fatalf("connection with a foreign-CA certificate was accepted")
	}
}

// TestNode_HandOffLeadership: a leader stepping down for a graceful
// shutdown hands leadership to another voter immediately; on a single-node
// cluster it is a no-op.
func TestNode_HandOffLeadership(t *testing.T) {
	nodes := newInmemCluster(t, 3)
	leader := waitForLeader(t, nodes, 3*time.Second)
	if err := leader.HandOffLeadership(); err != nil {
		t.Fatalf("HandOffLeadership: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		for _, n := range nodes {
			if n != leader && n.IsLeader() {
				return true
			}
		}
		return false
	}, "another node became leader")
	if leader.IsLeader() {
		t.Fatal("old leader is still leader")
	}

	_, trans := raft.NewInmemTransport("solo")
	solo := newInmemNode(t, "solo", trans, true, nil)
	waitForLeader(t, []*Node{solo}, 3*time.Second)
	if err := solo.HandOffLeadership(); err != nil || !solo.IsLeader() {
		t.Fatalf("single node: err=%v leader=%v", err, solo.IsLeader())
	}
}
