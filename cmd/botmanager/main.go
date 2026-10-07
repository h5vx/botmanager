// Command botmanager — точка входа сервиса управления Telegram-ботами.
//
// Поднимаются:
//   - Raft-узел (internal/raftcluster), одноузловой bootstrap
//     (см. internal/raftcluster/doc.go и CLAUDE.md);
//   - internal/botlifecycle.Manager, запускающий/останавливающий раннеров
//     ботов (internal/telegram) в зависимости от лидерства и состояния
//     ботов;
//   - HTTP-сервер наблюдаемости на :9091 (/healthz, /readyz, /metrics);
//   - gRPC-сервер на :9090 с BotAdmin/Messaging/Maintenance
//     (internal/rpcserver), плюс grpc.health.v1 и reflection.
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
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/h5vx/botmanager/internal/botlifecycle"
	"github.com/h5vx/botmanager/internal/config"
	"github.com/h5vx/botmanager/internal/observability"
	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/rpcserver"
	"github.com/h5vx/botmanager/internal/telegram"
	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
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

	node, err := raftcluster.Open(raftcluster.Config{
		NodeID:                 cfg.Node.ID,
		DataDir:                cfg.Node.DataDir,
		BindAddr:               cfg.Node.RaftBind,
		Bootstrap:              cfg.Raft.Bootstrap,
		MessageRetentionPerBot: cfg.Raft.MessageRetentionPerBot,
	})
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

	bus := telegram.NewInMemoryBus()
	runnerFactory := telegram.NewRunnerFactory(node, bus, telegramCfg, nodeProxy, logger)
	manager := botlifecycle.NewManager(node, runnerFactory, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	managerCtx, stopManager := context.WithCancel(context.Background())
	defer stopManager()
	go manager.Run(managerCtx)

	httpServer := newHTTPServer(cfg.HTTP.ListenAddr, node)
	grpcServer, grpcListener, err := newGRPCServer(cfg.GRPC.ListenAddr, node, bus, nodeProxy, telegramCfg, logger)
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "event", "botmanager.http_shutdown_error", "error", err.Error())
	}
	grpcServer.GracefulStop()

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
		// себя самого — одноузловой bootstrap, см. CLAUDE.md), узел не может ни принять запись
		// (Node.Apply), ни быть уверенным в собственном состоянии. Сразу
		// после старта процесса, до завершения самых первых выборов, это
		// ожидаемо не так — ровно для такого окна readyz и существует.
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

// newGRPCServer собирает gRPC-сервер, слушатель порта и регистрирует
// BotAdmin/Messaging/Maintenance (internal/rpcserver) поверх node/bus, плюс
// стандартную grpc.health.v1 службу и reflection.
func newGRPCServer(addr string, node *raftcluster.Node, bus *telegram.InMemoryBus, nodeProxy raftcluster.ProxyConfig, telegramCfg telegram.Config, logger *slog.Logger) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	server := grpc.NewServer()

	apiBaseURL := telegramCfg.APIBaseURL
	httpTimeout := telegramCfg.RequestTimeout

	botmanagerpb.RegisterBotAdminServer(server, rpcserver.NewBotAdminServer(node, nodeProxy, apiBaseURL, httpTimeout, logger))
	botmanagerpb.RegisterMessagingServer(server, rpcserver.NewMessagingServer(node, bus, nodeProxy, apiBaseURL, httpTimeout, logger))
	botmanagerpb.RegisterMaintenanceServer(server, rpcserver.NewMaintenanceServer(node, nodeProxy, apiBaseURL, httpTimeout, logger))

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	reflection.Register(server)

	return server, lis, nil
}
