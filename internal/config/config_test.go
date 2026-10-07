package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

func TestLoadMissingFileUsesDefaults(t *testing.T) {
	t.Setenv("BOTMANAGER_SECURITY__INSECURE", "true")
	cfg, err := Load("does/not/exist.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := Default()
	want.Security.Insecure = true
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("cfg = %+v, want defaults %+v", cfg, want)
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/config.yaml"
	yaml := "security:\n  insecure: true\ngrpc:\n  listen_addr: \":19090\"\nobservability:\n  log_level: DEBUG\n" +
		"raft:\n  message_retention_per_bot: 42\n  bootstrap: false\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.GRPC.ListenAddr != ":19090" {
		t.Errorf("GRPC.ListenAddr = %q, want :19090", cfg.GRPC.ListenAddr)
	}
	if cfg.Observability.LogLevel != "DEBUG" {
		t.Errorf("Observability.LogLevel = %q, want DEBUG", cfg.Observability.LogLevel)
	}
	if cfg.Raft.MessageRetentionPerBot != 42 {
		t.Errorf("Raft.MessageRetentionPerBot = %d, want 42", cfg.Raft.MessageRetentionPerBot)
	}
	if cfg.Raft.Bootstrap {
		t.Errorf("Raft.Bootstrap = true, want false")
	}
	// поле, не заданное в файле, должно остаться значением по умолчанию.
	if cfg.HTTP.ListenAddr != Default().HTTP.ListenAddr {
		t.Errorf("HTTP.ListenAddr = %q, want default %q", cfg.HTTP.ListenAddr, Default().HTTP.ListenAddr)
	}
}

func TestDefaultRaftConfig(t *testing.T) {
	cfg := Default()
	if cfg.Raft.MessageRetentionPerBot != raftcluster.DefaultMessageRetentionPerBot {
		t.Errorf("Raft.MessageRetentionPerBot = %d, want %d (raftcluster.DefaultMessageRetentionPerBot)",
			cfg.Raft.MessageRetentionPerBot, raftcluster.DefaultMessageRetentionPerBot)
	}
	if !cfg.Raft.Bootstrap {
		t.Errorf("Raft.Bootstrap = false, want true (single-node bootstrap by default)")
	}
}

func TestEnvOverride(t *testing.T) {
	env := map[string]string{
		"BOTMANAGER_GRPC__LISTEN_ADDR":               ":29090",
		"BOTMANAGER_PROXY__ENABLED":                  "true",
		"BOTMANAGER_PROXY__ADDRESS":                  "socks5h://proxy.local:1080",
		"BOTMANAGER_RAFT__MESSAGE_RETENTION_PER_BOT": "777",
		"BOTMANAGER_RAFT__BOOTSTRAP":                 "false",
	}
	lookup := func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}

	cfg := Default()
	if err := applyEnvOverrides(&cfg, EnvPrefix, lookup); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}

	if cfg.GRPC.ListenAddr != ":29090" {
		t.Errorf("GRPC.ListenAddr = %q, want :29090", cfg.GRPC.ListenAddr)
	}
	if !cfg.Proxy.Enabled {
		t.Errorf("Proxy.Enabled = false, want true")
	}
	if cfg.Proxy.Address != "socks5h://proxy.local:1080" {
		t.Errorf("Proxy.Address = %q, want socks5h://proxy.local:1080", cfg.Proxy.Address)
	}
	if cfg.Raft.MessageRetentionPerBot != 777 {
		t.Errorf("Raft.MessageRetentionPerBot = %d, want 777", cfg.Raft.MessageRetentionPerBot)
	}
	if cfg.Raft.Bootstrap {
		t.Errorf("Raft.Bootstrap = true, want false")
	}
}

func TestLoad_RequiresSecurityUnlessInsecure(t *testing.T) {
	_, err := Load("does/not/exist.yaml")
	if err == nil || !strings.Contains(err.Error(), "security.ca_file") || !strings.Contains(err.Error(), "security.token_key_file") {
		t.Fatalf("err = %v, want missing security settings", err)
	}

	for k, v := range map[string]string{
		"BOTMANAGER_SECURITY__CA_FILE": "ca.pem", "BOTMANAGER_SECURITY__CERT_FILE": "node.pem",
		"BOTMANAGER_SECURITY__KEY_FILE": "node-key.pem", "BOTMANAGER_SECURITY__TOKEN_KEY_FILE": "token.key",
	} {
		t.Setenv(k, v)
	}
	if _, err := Load("does/not/exist.yaml"); err != nil {
		t.Fatalf("Load with security settings: %v", err)
	}
}

func TestPeers_FromYAMLAndEnv(t *testing.T) {
	t.Setenv("BOTMANAGER_SECURITY__INSECURE", "true")
	path := t.TempDir() + "/c.yaml"
	yaml := "raft:\n  peers:\n    - id: n2\n      raft_addr: 10.0.0.2:9092\n      grpc_addr: 10.0.0.2:9090\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Raft.Peers) != 1 || cfg.Raft.Peers[0] != (PeerConfig{ID: "n2", RaftAddr: "10.0.0.2:9092", GRPCAddr: "10.0.0.2:9090"}) {
		t.Fatalf("yaml peers = %+v", cfg.Raft.Peers)
	}

	t.Setenv("BOTMANAGER_RAFT__PEERS", "n2/10.0.0.2:9092/10.0.0.2:9090, n3/10.0.0.3:9092/10.0.0.3:9090")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Raft.Peers) != 2 || cfg.Raft.Peers[1].ID != "n3" || cfg.Raft.Peers[1].GRPCAddr != "10.0.0.3:9090" {
		t.Fatalf("env peers = %+v", cfg.Raft.Peers)
	}

	t.Setenv("BOTMANAGER_RAFT__PEERS", "broken")
	if _, err := Load(path); err == nil {
		t.Fatalf("malformed peers accepted")
	}
}

func TestGRPCAdvertiseAddr(t *testing.T) {
	cfg := Default()
	cfg.Node.RaftAdvertise = "10.0.0.5:9092"
	if got, err := cfg.GRPCAdvertiseAddr(); err != nil || got != "10.0.0.5:9090" {
		t.Fatalf("derived = %q, %v", got, err)
	}
	cfg.Node.GRPCAdvertise = "bm-1.internal:9090"
	if got, _ := cfg.GRPCAdvertiseAddr(); got != "bm-1.internal:9090" {
		t.Fatalf("explicit = %q", got)
	}
}

func TestRaftTimeouts(t *testing.T) {
	t.Setenv("BOTMANAGER_SECURITY__INSECURE", "true")
	cfg := Default()
	hb, el, lease := cfg.Raft.RaftTimeouts()
	if hb != time.Second || el != time.Second || lease != 500*time.Millisecond {
		t.Fatalf("defaults = %s %s %s", hb, el, lease)
	}

	t.Setenv("BOTMANAGER_RAFT__HEARTBEAT_TIMEOUT_MS", "300")
	t.Setenv("BOTMANAGER_RAFT__ELECTION_TIMEOUT_MS", "300")
	t.Setenv("BOTMANAGER_RAFT__LEADER_LEASE_TIMEOUT_MS", "150")
	got, err := Load("does/not/exist.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if hb, _, lease := got.Raft.RaftTimeouts(); hb != 300*time.Millisecond || lease != 150*time.Millisecond {
		t.Fatalf("overridden = %s %s", hb, lease)
	}

	t.Setenv("BOTMANAGER_RAFT__LEADER_LEASE_TIMEOUT_MS", "400")
	if _, err := Load("does/not/exist.yaml"); err == nil {
		t.Fatal("lease > heartbeat accepted")
	}
}
