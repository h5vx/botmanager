package rpcserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/telegram"
	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
)

// exportChunkSize bounds how much of Node.ExportSnapshot's JSON output
// ExportState buffers in memory at once: the export is streamed, never
// buffered in memory whole (see doc.go).
const exportChunkSize = 64 * 1024

// MaintenanceServer implements botmanagerpb.MaintenanceServer.
// GetClusterStatus reads Raft's own configuration/leader rather than
// hardcoding a single node (see doc.go); ExportState streams
// Node.ExportSnapshot's output in bounded chunks. TransferLeadership и
// PingTelegram — для интерфейса администратора: nodeProxy/
// apiBaseURL/httpTimeout нужны PingTelegram тем же способом, что
// BotAdminServer.GetChat использует их для своего одноразового клиента
// (см. telegramClientFor в leader.go).
type MaintenanceServer struct {
	botmanagerpb.UnimplementedMaintenanceServer

	node        *raftcluster.Node
	nodeProxy   raftcluster.ProxyConfig
	apiBaseURL  string
	httpTimeout time.Duration
	logger      *slog.Logger
}

// NewMaintenanceServer constructs a MaintenanceServer. nodeProxy/apiBaseURL/
// httpTimeout/logger — тот же смысл и то же умолчание (logger == nil →
// slog.Default()), что у NewBotAdminServer/NewMessagingServer.
func NewMaintenanceServer(node *raftcluster.Node, nodeProxy raftcluster.ProxyConfig, apiBaseURL string, httpTimeout time.Duration, logger *slog.Logger) *MaintenanceServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &MaintenanceServer{
		node:        node,
		nodeProxy:   nodeProxy,
		apiBaseURL:  apiBaseURL,
		httpTimeout: httpTimeoutOrDefault(httpTimeout),
		logger:      logger,
	}
}

// GetClusterStatus reports every node in the current Raft configuration,
// which node is leader, and every bot's replicated state.
//
// reachable/proxy_healthy simplification (single-node — see
// CLAUDE.md): this node reports itself reachable (it is
// answering the RPC) and, since it has no peers to probe, every other
// listed node (there are none on a single-node cluster) would need a real
// network check this deployment cannot perform yet — there is no peer gRPC
// address list, and this environment has no way to run more than one node
// to test it against (the same constraint documented for
// internal/raftcluster's single-node bootstrap and
// internal/telegram's "node problem ⇒ step down" gap). proxy_healthy is
// left false (BotAdmin/Messaging's own live Telegram calls already surface
// proxy failures per-call via telegramError; a standalone proxy health
// probe is not implemented). This is a deliberate simplification, not a
// guess dressed up as a check.
func (s *MaintenanceServer) GetClusterStatus(_ context.Context, _ *botmanagerpb.Empty) (*botmanagerpb.ClusterStatus, error) {
	servers, err := s.node.Configuration()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read raft configuration: %v", err)
	}
	leaderID := s.node.LeaderID()
	thisID := s.node.ID()

	nodes := make([]*botmanagerpb.NodeStatus, 0, len(servers))
	for _, srv := range servers {
		id := string(srv.ID)
		nodes = append(nodes, &botmanagerpb.NodeStatus{
			NodeId:       id,
			IsLeader:     id == leaderID,
			Reachable:    id == thisID, // см. комментарий выше — соседей проверить нечем
			ProxyHealthy: false,
		})
	}

	bots := s.node.ListBots()
	botsOut := make([]*botmanagerpb.Bot, 0, len(bots))
	for _, b := range bots {
		botsOut = append(botsOut, botToProto(b))
	}

	return &botmanagerpb.ClusterStatus{
		LeaderId: leaderID,
		Nodes:    nodes,
		Bots:     botsOut,
	}, nil
}

// ExportState streams Node.ExportSnapshot's JSON-encoded state in
// exportChunkSize-bounded pieces via an io.Pipe: ExportSnapshot writes into
// the pipe from a goroutine while this method reads and forwards each chunk
// to the client, so the full export is never held in memory at once.
func (s *MaintenanceServer) ExportState(_ *botmanagerpb.ExportStateRequest, stream botmanagerpb.Maintenance_ExportStateServer) error {
	pr, pw := io.Pipe()

	go func() {
		pw.CloseWithError(s.node.ExportSnapshot(pw))
	}()

	buf := make([]byte, exportChunkSize)
	for {
		n, err := pr.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if sendErr := stream.Send(&botmanagerpb.Chunk{Data: chunk}); sendErr != nil {
				pr.CloseWithError(sendErr)
				return sendErr
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return status.Errorf(codes.Internal, "export state: %v", err)
		}
	}
}

// TransferLeadership hands mastership to a specific node — вывести узел на обслуживание, не дожидаясь штатных выборов. Реализовано
// поверх raftcluster.Node.TransferLeadershipTo, которая, в свою очередь,
// вызывает штатный механизм Raft (raft.Raft.LeadershipTransferToServer), а
// не самодельные выборы: Raft сам блокирует новые Apply на время передачи и
// уведомляет целевой узел стать кандидатом немедленно.
//
// Коды ошибок: узел неизвестен кластеру —
// NOT_FOUND; вызов пришёл не на текущего лидера — FAILED_PRECONDITION с
// именем лидера (requireLeader, тот же помощник, что и у команд на запись);
// сама передача не удалась (например, целевой узел недостижим) —
// UNAVAILABLE.
func (s *MaintenanceServer) TransferLeadership(ctx context.Context, req *botmanagerpb.TransferLeadershipRequest) (*botmanagerpb.ClusterStatus, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}

	targetID := req.GetTargetNodeId()
	if targetID == "" {
		return nil, status.Error(codes.InvalidArgument, "target_node_id is required")
	}

	servers, err := s.node.Configuration()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read raft configuration: %v", err)
	}
	var (
		targetAddr string
		found      bool
	)
	for _, srv := range servers {
		if string(srv.ID) == targetID {
			targetAddr = string(srv.Address)
			found = true
			break
		}
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "node %q is not part of the cluster", targetID)
	}

	if targetID != s.node.ID() {
		if err := s.node.TransferLeadershipTo(targetID, targetAddr); err != nil {
			return nil, status.Errorf(codes.Unavailable, "transfer leadership to %q failed: %v", targetID, err)
		}
	}
	// targetID == s.node.ID() — уже лидер там, куда просят передать: не
	// ошибка, просто нечего передавать (нет-оп), отдаём актуальный статус.

	return s.GetClusterStatus(ctx, &botmanagerpb.Empty{})
}

// PingTelegram проверяет доступность Telegram API со стороны этого узла
// botmanager, в том числе через настроенный прокси — отвечает на вопрос «это у нас сеть или у Telegram» без чтения логов.
//
// bot_id пуст → «прямой канал по умолчанию»: без токена конкретного бота,
// обычный GET на корень API-хоста через умолчание прокси узла — здесь нет
// осмысленного метода Bot API без токена, поэтому проверяется сам факт
// сетевого ответа (значение имеет соединение и прокси, а не код ответа).
// bot_id непуст → канал этого бота: getMe его собственным токеном и
// эффективным прокси (nodeProxy с учётом переопределения бота),
// поэтому заодно проверяет валидность токена. Оба пути ограничены
// s.httpTimeout — метод не может зависнуть.
func (s *MaintenanceServer) PingTelegram(ctx context.Context, req *botmanagerpb.PingTelegramRequest) (*botmanagerpb.PingResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()

	botID := req.GetBotId()
	if botID == "" {
		return s.pingDirect(ctx), nil
	}

	bot, ok := s.node.GetBot(botID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "bot %s not found", botID)
	}
	if bot.State == raftcluster.BotStateDeleted {
		return nil, status.Errorf(codes.FailedPrecondition, "bot %s is deleted", bot.ID)
	}
	return s.pingBot(ctx, bot), nil
}

// pingDirect probes the API host itself, without any bot token — the
// "прямой канал по умолчанию" branch of PingTelegram, using this node's
// default proxy config.
func (s *MaintenanceServer) pingDirect(ctx context.Context) *botmanagerpb.PingResult {
	proxyUsed := proxyAddressFor(s.nodeProxy)

	httpClient, err := telegram.NewHTTPClient(s.nodeProxy)
	if err != nil {
		return &botmanagerpb.PingResult{ProxyUsed: proxyUsed, Error: err.Error()}
	}

	base := s.apiBaseURL
	if base == "" {
		base = telegram.DefaultAPIBaseURL
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/", nil)
	if err != nil {
		return &botmanagerpb.PingResult{ProxyUsed: proxyUsed, Error: err.Error()}
	}

	started := time.Now()
	resp, err := httpClient.Do(httpReq)
	latencyMs := time.Since(started).Milliseconds()
	if err != nil {
		return &botmanagerpb.PingResult{LatencyMs: latencyMs, ProxyUsed: proxyUsed, Error: err.Error()}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return &botmanagerpb.PingResult{
		Success:    true,
		LatencyMs:  latencyMs,
		HttpStatus: int32(resp.StatusCode),
		ProxyUsed:  proxyUsed,
	}
}

// pingBot probes Telegram through one specific bot's own token/effective
// proxy (getMe) — the bot_id branch of PingTelegram.
func (s *MaintenanceServer) pingBot(ctx context.Context, bot raftcluster.Bot) *botmanagerpb.PingResult {
	effective := telegram.EffectiveProxy(s.nodeProxy, bot.Proxy)
	proxyUsed := proxyAddressFor(effective)

	client, err := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if err != nil {
		return &botmanagerpb.PingResult{ProxyUsed: proxyUsed, Error: err.Error()}
	}

	started := time.Now()
	_, callErr := client.GetMe(ctx)
	latencyMs := time.Since(started).Milliseconds()

	result := &botmanagerpb.PingResult{LatencyMs: latencyMs, ProxyUsed: proxyUsed}
	if callErr != nil {
		result.Error = callErr.Error()
		var apiErr *telegram.APIError
		if errors.As(callErr, &apiErr) {
			result.HttpStatus = int32(apiErr.HTTPStatus)
		}
		return result
	}
	result.Success = true
	result.HttpStatus = http.StatusOK
	return result
}

// proxyAddressFor reports the proxy address PingTelegram used, with any
// embedded credentials stripped (секрет прокси наружу не отдаётся ни при
// каком исходе), or "" when no proxy was in effect.
func proxyAddressFor(effective raftcluster.ProxyConfig) string {
	if !effective.Enabled {
		return ""
	}
	u, err := url.Parse(effective.Address)
	if err != nil || u.User == nil {
		return effective.Address
	}
	u.User = nil
	return u.String()
}
