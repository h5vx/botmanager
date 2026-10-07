package rpcserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/telegram"
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
	peers       PeerDialer
	runtime     NodeRuntime
}

// PeerDialer gives MaintenanceServer connections to other nodes' gRPC
// endpoints for health probes; *Forwarder implements it.
type PeerDialer interface {
	Conn(addr string) (*grpc.ClientConn, error)
}

// peerProbeTimeout bounds one health probe of a peer in GetClusterStatus.
const peerProbeTimeout = time.Second

// SetPeerDialer enables real reachability probes of other nodes in
// GetClusterStatus. Without it, only this node is reported reachable.
func (s *MaintenanceServer) SetPeerDialer(d PeerDialer) { s.peers = d }

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

// GetClusterStatus reports every node in the current Raft configuration
// with its addresses (from the node registry), which node is leader, and
// every bot's replicated state.
//
// reachable: this node is reachable by definition (it is answering); every
// other node is probed with a grpc.health.v1 Check on its registered gRPC
// address, in parallel, each bounded by peerProbeTimeout. A node without a
// registered gRPC address, or with no PeerDialer configured, is reported
// unreachable. proxy_healthy is not probed here — use PingTelegram on the
// node in question.
func (s *MaintenanceServer) GetClusterStatus(ctx context.Context, _ *botmanagerpb.Empty) (*botmanagerpb.ClusterStatus, error) {
	return s.clusterStatus(ctx)
}

func (s *MaintenanceServer) clusterStatus(ctx context.Context) (*botmanagerpb.ClusterStatus, error) {
	servers, err := s.node.Configuration()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read raft configuration: %v", err)
	}
	leaderID := s.node.LeaderID()
	thisID := s.node.ID()

	nodes := make([]*botmanagerpb.NodeStatus, len(servers))
	var wg sync.WaitGroup
	for i, srv := range servers {
		id := string(srv.ID)
		ns := &botmanagerpb.NodeStatus{
			NodeId:      id,
			IsLeader:    id == leaderID,
			Reachable:   id == thisID,
			RaftAddress: string(srv.Address),
		}
		if info, ok := s.node.GetNode(id); ok {
			ns.GrpcAddress = info.GRPCAddr
		}
		nodes[i] = ns
		if id != thisID && ns.GrpcAddress != "" && s.peers != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ns.Reachable = s.probe(ctx, ns.GrpcAddress)
			}()
		}
	}
	wg.Wait()

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

func (s *MaintenanceServer) probe(ctx context.Context, addr string) bool {
	conn, err := s.peers.Conn(addr)
	if err != nil {
		return false
	}
	pctx, cancel := context.WithTimeout(ctx, peerProbeTimeout)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(pctx, &healthpb.HealthCheckRequest{})
	return err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
}

// AddNode adds a running, not-yet-bootstrapped node to the cluster as a
// voter and records its addresses in the node registry. Leader only (a
// follower forwards the call, see Forwarder).
func (s *MaintenanceServer) AddNode(ctx context.Context, req *botmanagerpb.AddNodeRequest) (*botmanagerpb.ClusterStatus, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" || req.GetRaftAddress() == "" || req.GetGrpcAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id, raft_address and grpc_address are required")
	}
	info := raftcluster.NodeInfo{ID: req.GetNodeId(), RaftAddr: req.GetRaftAddress(), GRPCAddr: req.GetGrpcAddress()}
	if err := s.node.AddVoter(info, membershipTimeout); err != nil {
		if errors.Is(err, raftcluster.ErrNotLeader) || errors.Is(err, raftcluster.ErrInvalidCommand) {
			return nil, applyError(err)
		}
		return nil, status.Errorf(codes.Unavailable, "add node %s: %v", info.ID, err)
	}
	s.logger.Info("node added", "event", "maintenance.node_added", "node_id", info.ID, "raft_addr", info.RaftAddr, "grpc_addr", info.GRPCAddr)
	return s.clusterStatus(ctx)
}

// RemoveNode removes a node from the cluster and from the node registry.
// Removing the current leader is allowed: Raft steps it down and the
// remaining nodes elect a new one.
func (s *MaintenanceServer) RemoveNode(ctx context.Context, req *botmanagerpb.RemoveNodeRequest) (*botmanagerpb.ClusterStatus, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}
	id := req.GetNodeId()
	servers, err := s.node.Configuration()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read raft configuration: %v", err)
	}
	found := false
	for _, srv := range servers {
		if string(srv.ID) == id {
			found = true
			break
		}
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "node %q is not part of the cluster", id)
	}
	if err := s.node.RemoveServer(id, membershipTimeout); err != nil {
		if errors.Is(err, raftcluster.ErrNotLeader) {
			return nil, applyError(err)
		}
		return nil, status.Errorf(codes.Unavailable, "remove node %s: %v", id, err)
	}
	s.logger.Info("node removed", "event", "maintenance.node_removed", "node_id", id)
	return s.clusterStatus(ctx)
}

// membershipTimeout bounds one membership change (AddNode/RemoveNode).
const membershipTimeout = 10 * time.Second

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
