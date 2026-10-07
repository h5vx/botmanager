package config

import (
	"os"
	"testing"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

func TestLoadMissingFileUsesDefaults(t *testing.T) {
	cfg, err := Load("does/not/exist.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := Default()
	if cfg != want {
		t.Fatalf("cfg = %+v, want defaults %+v", cfg, want)
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/config.yaml"
	yaml := "grpc:\n  listen_addr: \":19090\"\nobservability:\n  log_level: DEBUG\n" +
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
