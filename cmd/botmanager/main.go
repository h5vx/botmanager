// Command botmanager — точка входа сервиса управления Telegram-ботами.
//
// Поднимаются:
//   - Raft-узел (internal/raftcluster): один узел или кластер из
//     raft.peers, транспорт поверх mTLS, токены ботов зашифрованы;
//   - internal/botlifecycle.Manager, запускающий/останавливающий раннеров
//     ботов (internal/telegram) в зависимости от лидерства и состояния
//     ботов;
//   - internal/failover.Monitor: передаёт лидерство соседу, если этот узел
//     перестал видеть Telegram, а сосед видит;
//   - HTTP-сервер наблюдаемости на :9091 (/healthz, /readyz, /metrics);
//   - gRPC-сервер на :9090 с BotAdmin/Messaging/Maintenance
//     (internal/rpcserver) поверх mTLS, с пересылкой записей лидеру, плюс
//     grpc.health.v1 и reflection.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/hashicorp/raft"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/botlifecycle"
	"github.com/h5vx/botmanager/internal/config"
	"github.com/h5vx/botmanager/internal/failover"
	"github.com/h5vx/botmanager/internal/observability"
	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/rpcserver"
	"github.com/h5vx/botmanager/internal/security"
	"github.com/h5vx/botmanager/internal/telegram"
)

const serviceName = "botmanager"

func main() {
	configPath := flag.String("config", "config/config.yaml", "путь к YAML-файлу конфигурации")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("config load failed", "error", err.Error())
		os.Exit(1)
	}

	logger := observability.NewLogger(
		observability.LogFields{Service: serviceName, Version: cfg.Observability.Version},
		cfg.Observability.LogLevel,
	)
	slog.SetDefault(logger)

	logger.Info("starting", "event", "botmanager.starting",
		"node_id", cfg.Node.ID,
		"grpc_addr", cfg.GRPC.ListenAddr,
		"http_addr", cfg.HTTP.ListenAddr,
	)

	sec, err := loadSecurity(cfg.Security, logger)
	if err != nil {
		logger.Error("security setup failed", "event", "botmanager.security_failed", "error", err.Error())
		os.Exit(1)
	}

	grpcAdvertise, err := cfg.GRPCAdvertiseAddr()
	if err != nil {
		logger.Error("grpc advertise address", "event", "botmanager.config_invalid", "error", err.Error())
		os.Exit(1)
	}
	peers := make([]raftcluster.NodeInfo, 0, len(cfg.Raft.Peers))
	for _, p := range cfg.Raft.Peers {
		peers = append(peers, raftcluster.NodeInfo{ID: p.ID, RaftAddr: p.RaftAddr, GRPCAddr: p.GRPCAddr})
	}

	raftBase := raft.DefaultConfig()
	raftBase.HeartbeatTimeout, raftBase.ElectionTimeout, raftBase.LeaderLeaseTimeout = cfg.Raft.RaftTimeouts()

	raftCfg := raftcluster.Config{
		RaftConfig:             raftBase,
		NodeID:                 cfg.Node.ID,
		DataDir:                cfg.Node.DataDir,
		BindAddr:               cfg.Node.RaftBind,
		AdvertiseAddr:          cfg.Node.RaftAdvertise,
		Bootstrap:              cfg.Raft.Bootstrap,
		MessageRetentionPerBot: cfg.Raft.MessageRetentionPerBot,
		JournalRetention:       cfg.Raft.JournalRetention,
		TokenCipher:            sec.cipher,
		Self:                   raftcluster.NodeInfo{GRPCAddr: grpcAdvertise},
		KnownPeers:             peers,
		Logger:                 logger,
	}
	if sec.mtls != nil {
		raftCfg.TLS = &raftcluster.TransportTLS{Server: sec.mtls.Server, Client: sec.mtls.Client}
	}
	node, err := raftcluster.Open(raftCfg)
	if err != nil {
		logger.Error("raft open failed", "event", "botmanager.raft_open_failed", "error", err.Error())
		os.Exit(1)
	}

	nodeProxy := raftcluster.ProxyConfig{Enabled: cfg.Proxy.Enabled, Address: cfg.Proxy.Address}
	telegramCfg := telegram.Config{
		RequestTimeout:   time.Duration(cfg.Telegram.RequestTimeoutSeconds) * time.Second,
		LongPollTimeout:  time.Duration(cfg.Telegram.LongPollTimeoutSeconds) * time.Second,
		APIBaseURL:       cfg.Telegram.APIBaseURLOverride,
		MaxSendRetries:   cfg.Telegram.MaxSendRetries,
		SendPollInterval: time.Duration(cfg.Telegram.SendPollIntervalSeconds) * time.Second,
	}

	peerCreds := insecure.NewCredentials()
	if sec.mtls != nil {
		peerCreds = credentials.NewTLS(sec.mtls.Client)
	}
	forwarder, err := rpcserver.NewForwarder(node, grpc.WithTransportCredentials(peerCreds))
	if err != nil {
		logger.Error("forwarder setup failed", "event", "botmanager.forwarder_failed", "error", err.Error())
		os.Exit(1)
	}

	managerCtx, stopManager := context.WithCancel(context.Background())
	defer stopManager()

	if cfg.Failover.Enabled {
		monitor := failover.New(failover.Config{
			FailureWindow:  time.Duration(cfg.Failover.FailureWindowSeconds) * time.Second,
			MinFailingBots: cfg.Failover.MinFailingBots,
			Cooldown:       time.Duration(cfg.Failover.CooldownSeconds) * time.Second,
			CheckInterval:  time.Duration(cfg.Failover.CheckIntervalSeconds) * time.Second,
		}, node, forwarder, logger)
		telegramCfg.Health = monitor
		go monitor.Run(managerCtx)
	}

	runnerFactory := telegram.NewRunnerFactory(node, telegramCfg, nodeProxy, logger)
	manager := botlifecycle.NewManager(node, runnerFactory, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go manager.Run(managerCtx)

	httpServer := newHTTPServer(cfg.HTTP.ListenAddr, node)
	grpcServer, grpcListener, err := newGRPCServer(cfg.GRPC.ListenAddr, node, sec, forwarder, nodeProxy, telegramCfg, logger)
	if err != nil {
		logger.Error("grpc listen failed", "event", "botmanager.grpc_listen_failed", "error", err.Error())
		os.Exit(1)
	}

	errCh := make(chan error, 2)

	go func() {
		logger.Info("http server listening", "event", "botmanager.http_listening", "addr", cfg.HTTP.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	go func() {
		logger.Info("grpc server listening", "event", "botmanager.grpc_listening", "addr", cfg.GRPC.ListenAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received", "event", "botmanager.shutdown_start")
	case err := <-errCh:
		logger.Error("server failed", "event", "botmanager.server_failed", "error", err.Error())
	}

	// Сначала отдаём лидерство другому узлу: кластер получает нового лидера
	// сразу, а не после истечения heartbeat-таймаута. Раннеры ботов на этом
	// узле Manager остановит сам — по сигналу потери лидерства.
	if err := node.HandOffLeadership(); err != nil {
		logger.Warn("leadership handoff failed", "event", "botmanager.handoff_failed", "error", err.Error())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "event", "botmanager.http_shutdown_error", "error", err.Error())
	}
	grpcServer.GracefulStop()
	forwarder.Close()

	// Останавливаем botlifecycle.Manager (и тем самым все раннеры ботов на
	// этом узле) прежде чем выключать сам Raft-узел — Manager.Run завершает
	// свой цикл по отмене managerCtx и синхронно останавливает раннеров
	// (stopAll), после чего Node.Shutdown можно вызывать, не оставляя
	// раннеров, которые попытались бы Apply на уже выключенном узле.
	stopManager()

	if err := node.Shutdown(); err != nil {
		logger.Error("raft shutdown error", "event", "botmanager.raft_shutdown_error", "error", err.Error())
	}

	logger.Info("stopped", "event", "botmanager.stopped")
}

func newHTTPServer(addr string, node *raftcluster.Node) *http.Server {
	ready := func() (bool, string) {
		// readyz проверяет реальные зависимости, а не только "процесс
		// жив" (это работа healthz). Единственная зависимость —
		// собственный Raft-узел: пока у него нет известного лидера (в т.ч.
		// себя самого), узел не может ни принять запись, ни переслать её
		// лидеру. Сразу после старта процесса, до завершения первых
		// выборов, это ожидаемо не так — ровно для такого окна readyz и
		// существует.
		if node.LeaderID() == "" {
			return false, "raft leader not yet known"
		}
		return true, ""
	}
	return &http.Server{
		Addr:              addr,
		Handler:           observability.NewHTTPMuxWithReadyz(serviceName, ready),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// securitySetup is what loadSecurity derives from config: mTLS
// configurations and the token cipher, both nil in insecure mode.
type securitySetup struct {
	mtls   *security.MTLS
	cipher *raftcluster.TokenCipher
}

func loadSecurity(cfg config.SecurityConfig, logger *slog.Logger) (securitySetup, error) {
	if cfg.Insecure {
		logger.Warn("INSECURE MODE: gRPC and Raft run without TLS or authentication, bot tokens are stored in plaintext — local development only",
			"event", "botmanager.insecure_mode")
		return securitySetup{}, nil
	}
	m, err := security.LoadMTLS(cfg.CAFile, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return securitySetup{}, err
	}
	key, err := security.LoadTokenKey(cfg.TokenKeyFile)
	if err != nil {
		return securitySetup{}, err
	}
	cipher, err := raftcluster.NewTokenCipher(key)
	if err != nil {
		return securitySetup{}, err
	}
	return securitySetup{mtls: m, cipher: cipher}, nil
}

// newGRPCServer собирает gRPC-сервер (mTLS, если не insecure-режим),
// слушатель порта и регистрирует BotAdmin/Messaging/Maintenance
// (internal/rpcserver) поверх node, плюс стандартную grpc.health.v1 службу
// и reflection. Записи, пришедшие на не-лидера, пересылаются лидеру
// (forwarder — тот же пул соединений к соседям использует failover).
func newGRPCServer(addr string, node *raftcluster.Node, sec securitySetup, forwarder *rpcserver.Forwarder, nodeProxy raftcluster.ProxyConfig, telegramCfg telegram.Config, logger *slog.Logger) (*grpc.Server, net.Listener, error) {
	serverCreds := insecure.NewCredentials()
	if sec.mtls != nil {
		serverCreds = credentials.NewTLS(sec.mtls.Server)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	server := grpc.NewServer(
		grpc.Creds(serverCreds),
		grpc.ChainUnaryInterceptor(forwarder.UnaryInterceptor),
	)

	apiBaseURL := telegramCfg.APIBaseURL
	httpTimeout := telegramCfg.RequestTimeout

	botmanagerpb.RegisterBotAdminServer(server, rpcserver.NewBotAdminServer(node, nodeProxy, apiBaseURL, httpTimeout, logger))
	botmanagerpb.RegisterMessagingServer(server, rpcserver.NewMessagingServer(node, nodeProxy, apiBaseURL, httpTimeout, logger))
	maintenance := rpcserver.NewMaintenanceServer(node, nodeProxy, apiBaseURL, httpTimeout, logger)
	maintenance.SetPeerDialer(forwarder)
	botmanagerpb.RegisterMaintenanceServer(server, maintenance)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	reflection.Register(server)

	return server, lis, nil
}
