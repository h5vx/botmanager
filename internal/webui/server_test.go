package webui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/rpcserver"
)

const testPassword = "correct horse battery"

// newTestUI runs a real single-node cluster with the gRPC services on an
// in-memory listener and a web UI in front of it.
func newTestUI(t *testing.T) (*Server, *httptest.Server, botmanagerpb.MessagingClient) {
	t.Helper()
	cfg := raft.DefaultConfig()
	cfg.HeartbeatTimeout, cfg.ElectionTimeout, cfg.LeaderLeaseTimeout = 50*time.Millisecond, 50*time.Millisecond, 25*time.Millisecond
	cfg.Logger = hclog.NewNullLogger()
	_, trans := raft.NewInmemTransport("test-node")
	store := raft.NewInmemStore()
	node, err := raftcluster.Open(raftcluster.Config{
		NodeID: "test-node", Bootstrap: true, RaftConfig: cfg,
		Deps: raftcluster.Dependencies{Transport: trans, LogStore: store, StableStore: store, SnapshotStore: raft.NewInmemSnapshotStore()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	for i := 0; i < 200 && !node.IsLeader(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	botmanagerpb.RegisterBotAdminServer(gs, rpcserver.NewBotAdminServer(node, raftcluster.ProxyConfig{}, "", 0, nil))
	botmanagerpb.RegisterMessagingServer(gs, rpcserver.NewMessagingServer(node, raftcluster.ProxyConfig{}, "", 0, nil))
	maint := rpcserver.NewMaintenanceServer(node, raftcluster.ProxyConfig{}, "", 0, nil)
	maint.SetNodeRuntime(rpcserver.NodeRuntime{Version: "test", StartedAt: time.Now()})
	botmanagerpb.RegisterMaintenanceServer(gs, maint)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	hash, err := HashPassword(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	ui, err := New(Config{
		Users:       map[string][]byte{"admin": []byte(hash)},
		SessionKey:  bytes.Repeat([]byte{7}, 32),
		Local:       conn,
		LocalNodeID: "test-node",
		Dial:        func(string) (grpc.ClientConnInterface, error) { return nil, errors.New("no peers in this test") },
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(ui)
	t.Cleanup(srv.Close)
	return ui, srv, botmanagerpb.NewMessagingClient(conn)
}

type client struct {
	t      *testing.T
	base   string
	cookie *http.Cookie
}

func (c *client) do(method, path string, body any, csrf bool) (*http.Response, map[string]any) {
	c.t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, c.base+path, rd)
	if csrf {
		req.Header.Set(csrfHeader, "1")
	}
	if c.cookie != nil {
		req.AddCookie(c.cookie)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	for _, ck := range res.Cookies() {
		if ck.Name == sessionCookie {
			if ck.MaxAge < 0 {
				c.cookie = nil
			} else {
				c.cookie = ck
			}
		}
	}
	return res, out
}

func (c *client) login() {
	c.t.Helper()
	res, _ := c.do("POST", "/api/login", map[string]string{"username": "admin", "password": testPassword}, true)
	if res.StatusCode != http.StatusOK || c.cookie == nil {
		c.t.Fatalf("login status %d", res.StatusCode)
	}
}

func TestStaticAndSecurityHeaders(t *testing.T) {
	_, srv, _ := newTestUI(t)
	res, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK || !strings.Contains(res.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("index: %d %s", res.StatusCode, res.Header.Get("Content-Type"))
	}
	for _, h := range []string{"Content-Security-Policy", "X-Content-Type-Options", "X-Frame-Options", "Referrer-Policy"} {
		if res.Header.Get(h) == "" {
			t.Errorf("missing %s", h)
		}
	}
	for _, p := range []string{"/app.js", "/style.css", "/logo.svg"} {
		r, err := http.Get(srv.URL + p)
		if err != nil || r.StatusCode != http.StatusOK {
			t.Fatalf("%s: %v %v", p, err, r.StatusCode)
		}
		r.Body.Close()
	}
}

func TestLogin_Session_CSRF_RateLimit(t *testing.T) {
	ui, srv, _ := newTestUI(t)
	c := &client{t: t, base: srv.URL}

	if res, _ := c.do("GET", "/api/overview", nil, false); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous overview = %d", res.StatusCode)
	}
	if res, _ := c.do("POST", "/api/login", map[string]string{"username": "admin", "password": testPassword}, false); res.StatusCode != http.StatusForbidden {
		t.Fatalf("login without CSRF header = %d", res.StatusCode)
	}
	if res, _ := c.do("POST", "/api/login", map[string]string{"username": "admin", "password": "wrong password"}, true); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password = %d", res.StatusCode)
	}

	c.login()
	if !c.cookie.HttpOnly || c.cookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("cookie flags: %+v", c.cookie)
	}
	if res, out := c.do("GET", "/api/session", nil, false); res.StatusCode != http.StatusOK || out["username"] != "admin" {
		t.Fatalf("session = %d %v", res.StatusCode, out)
	}
	if res, _ := c.do("POST", "/api/bots", map[string]string{"token": "123:x"}, false); res.StatusCode != http.StatusForbidden {
		t.Fatalf("mutation without CSRF header = %d", res.StatusCode)
	}

	// Подделанная cookie не принимается.
	forged := &client{t: t, base: srv.URL, cookie: &http.Cookie{Name: sessionCookie, Value: strings.Replace(c.cookie.Value, ".", "x.", 1)}}
	if res, _ := forged.do("GET", "/api/session", nil, false); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("forged cookie = %d", res.StatusCode)
	}
	// Просроченная — тоже.
	ui.now = func() time.Time { return time.Now().Add(13 * time.Hour) }
	if res, _ := c.do("GET", "/api/session", nil, false); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired session = %d", res.StatusCode)
	}
	ui.now = time.Now

	// Выход сбрасывает cookie.
	c.do("POST", "/api/logout", nil, true)
	if c.cookie != nil {
		t.Fatalf("logout did not clear the cookie")
	}

	// Перебор: после 5 неудач — 429 даже с верным паролем.
	attacker := &client{t: t, base: srv.URL}
	for i := 0; i < 5; i++ {
		attacker.do("POST", "/api/login", map[string]string{"username": "admin", "password": "guess"}, true)
	}
	if res, _ := attacker.do("POST", "/api/login", map[string]string{"username": "admin", "password": testPassword}, true); res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("after 5 failures = %d, want 429", res.StatusCode)
	}
}

func TestOverviewAndBots(t *testing.T) {
	_, srv, messaging := newTestUI(t)
	c := &client{t: t, base: srv.URL}
	c.login()

	res, out := c.do("POST", "/api/bots", map[string]string{
		"display_name": "Support", "token": "123456:SECRETSECRET", "proxy_mode": "custom", "proxy_address": "socks5h://user:hunter2@proxy:1080",
	}, true)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("create bot = %d %v", res.StatusCode, out)
	}
	botID, _ := out["id"].(string)
	if botID == "" {
		t.Fatalf("create bot response = %v", out)
	}
	if res, _ := c.do("POST", "/api/bots", map[string]string{"token": "1:x", "proxy_mode": "custom", "proxy_address": "http://x"}, true); res.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad proxy accepted: %d", res.StatusCode)
	}
	if res, out := c.do("POST", "/api/bots/"+botID+"/state", map[string]string{"state": "ENABLED"}, true); res.StatusCode != http.StatusOK || out["state"] != "BOT_STATE_ENABLED" {
		t.Fatalf("enable = %d %v", res.StatusCode, out)
	}
	if _, err := messaging.Send(context.Background(), &botmanagerpb.SendRequest{IdempotencyKey: "m1", BotId: botID, ChatId: 5, Text: "hello"}); err != nil {
		t.Fatal(err)
	}

	res, out = c.do("GET", "/api/overview", nil, false)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("overview = %d %v", res.StatusCode, out)
	}
	raw, _ := json.Marshal(out)
	body := string(raw)
	if strings.Contains(body, "hunter2") || strings.Contains(body, "SECRETSECRET") {
		t.Fatalf("overview leaks a secret: %s", body)
	}
	cluster := out["cluster"].(map[string]any)
	if cluster["leader_id"] != "test-node" {
		t.Fatalf("leader = %v", cluster["leader_id"])
	}
	stats := out["node_stats"].(map[string]any)["test-node"].(map[string]any)["stats"].(map[string]any)
	if stats["raft_state"] != "Leader" || stats["messages_total"] != "1" {
		t.Fatalf("stats = %v", stats)
	}

	res, out = c.do("GET", "/api/bots/"+botID+"/messages", nil, false)
	msgs, _ := out["messages"].([]any)
	if res.StatusCode != http.StatusOK || len(msgs) != 1 || msgs[0].(map[string]any)["text"] != "hello" {
		t.Fatalf("messages = %d %v", res.StatusCode, out)
	}

	if res, _ := c.do("POST", "/api/ping", map[string]string{"node_id": "nope"}, true); res.StatusCode != http.StatusNotFound {
		t.Fatalf("ping unknown node = %d", res.StatusCode)
	}
	if res, _ := c.do("POST", "/api/leader", map[string]string{"node_id": "nope"}, true); res.StatusCode != http.StatusNotFound {
		t.Fatalf("transfer to unknown node = %d", res.StatusCode)
	}

	if res, _ := c.do("DELETE", "/api/bots/"+botID, nil, true); res.StatusCode != http.StatusNoContent {
		t.Fatalf("delete = %d", res.StatusCode)
	}
}

func TestHashPassword(t *testing.T) {
	if _, err := HashPassword("short"); err == nil {
		t.Fatal("short password accepted")
	}
	h, err := HashPassword(testPassword)
	if err != nil || !strings.HasPrefix(h, "$2") {
		t.Fatalf("hash = %q, %v", h, err)
	}
}
