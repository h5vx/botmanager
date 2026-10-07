package webui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
)

// nodeStatsTimeout bounds the statistics call to each node in an overview,
// so one unreachable node does not stall the page.
const nodeStatsTimeout = 3 * time.Second

func (s *Server) maintenance(conn grpc.ClientConnInterface) botmanagerpb.MaintenanceClient {
	return botmanagerpb.NewMaintenanceClient(conn)
}

// nodeConn returns the connection to the node with the given registry
// entry: this node's own connection for itself, a dialed one otherwise.
func (s *Server) nodeConn(nodeID, grpcAddr string) (grpc.ClientConnInterface, error) {
	if nodeID == s.cfg.LocalNodeID {
		return s.cfg.Local, nil
	}
	return s.cfg.Dial(grpcAddr)
}

// handleOverview returns the cluster status (nodes, leader, bots, with bot
// proxy credentials removed) and every node's statistics, fetched in
// parallel; a node that cannot be reached gets an error instead.
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request, _ string) {
	ctx, cancel := s.ctx(r)
	defer cancel()
	st, err := s.maintenance(s.cfg.Local).GetClusterStatus(ctx, &botmanagerpb.Empty{})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	for _, b := range st.GetBots() {
		if p := b.GetProxy(); p != nil {
			p.Address = redactProxy(p.GetAddress())
		}
	}

	stats := make(map[string]nodeStats, len(st.GetNodes()))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range st.GetNodes() {
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			res := s.fetchNodeStats(ctx, id, addr)
			mu.Lock()
			stats[id] = res
			mu.Unlock()
		}(n.GetNodeId(), n.GetGrpcAddress())
	}
	wg.Wait()

	writeJSON(w, http.StatusOK, map[string]any{
		"cluster":    protoJSON(st),
		"node_stats": stats,
		"local_node": s.cfg.LocalNodeID,
	})
}

type nodeStats struct {
	Stats json.RawMessage `json:"stats,omitempty"`
	Error string          `json:"error,omitempty"`
}

func (s *Server) fetchNodeStats(ctx context.Context, id, addr string) nodeStats {
	if id != s.cfg.LocalNodeID && addr == "" {
		return nodeStats{Error: "node has no registered gRPC address"}
	}
	conn, err := s.nodeConn(id, addr)
	if err != nil {
		return nodeStats{Error: err.Error()}
	}
	nctx, cancel := context.WithTimeout(ctx, nodeStatsTimeout)
	defer cancel()
	ns, err := s.maintenance(conn).GetNodeStats(nctx, &botmanagerpb.Empty{})
	if err != nil {
		return nodeStats{Error: status.Convert(err).Message()}
	}
	return nodeStats{Stats: protoJSON(ns)}
}

// catchUp gives the browser read-your-writes when this node is a follower:
// a write was forwarded to the leader, so before answering, wait until this
// node has applied the log up to the leader's commit index — otherwise the
// next overview, read from this node's copy, could miss the change. Best
// effort, bounded by catchUpTimeout.
func (s *Server) catchUp(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, catchUpTimeout)
	defer cancel()
	local := s.maintenance(s.cfg.Local)
	st, err := local.GetClusterStatus(ctx, &botmanagerpb.Empty{})
	if err != nil || st.GetLeaderId() == "" || st.GetLeaderId() == s.cfg.LocalNodeID {
		return
	}
	var leaderAddr string
	for _, n := range st.GetNodes() {
		if n.GetNodeId() == st.GetLeaderId() {
			leaderAddr = n.GetGrpcAddress()
		}
	}
	if leaderAddr == "" {
		return
	}
	conn, err := s.cfg.Dial(leaderAddr)
	if err != nil {
		return
	}
	ls, err := s.maintenance(conn).GetNodeStats(ctx, &botmanagerpb.Empty{})
	if err != nil {
		return
	}
	target := ls.GetCommitIndex()
	for {
		ns, err := local.GetNodeStats(ctx, &botmanagerpb.Empty{})
		if err != nil || ns.GetAppliedIndex() >= target {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

const catchUpTimeout = 2 * time.Second

func (s *Server) handleTransferLeadership(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := decodeJSON(r, &req); err != nil || req.NodeID == "" {
		writeError(w, http.StatusBadRequest, "node_id is required")
		return
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	st, err := s.maintenance(s.cfg.Local).TransferLeadership(ctx, &botmanagerpb.TransferLeadershipRequest{TargetNodeId: req.NodeID})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	s.catchUp(r.Context())
	s.audit(user, "transfer_leadership", "node_id", req.NodeID)
	writeJSON(w, http.StatusOK, protoJSON(st))
}

func (s *Server) handleAddNode(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		NodeID      string `json:"node_id"`
		RaftAddress string `json:"raft_address"`
		GRPCAddress string `json:"grpc_address"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	st, err := s.maintenance(s.cfg.Local).AddNode(ctx, &botmanagerpb.AddNodeRequest{
		NodeId: strings.TrimSpace(req.NodeID), RaftAddress: strings.TrimSpace(req.RaftAddress), GrpcAddress: strings.TrimSpace(req.GRPCAddress),
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	s.catchUp(r.Context())
	s.audit(user, "add_node", "node_id", req.NodeID, "raft_address", req.RaftAddress, "grpc_address", req.GRPCAddress)
	writeJSON(w, http.StatusOK, protoJSON(st))
}

func (s *Server) handleRemoveNode(w http.ResponseWriter, r *http.Request, user string) {
	id := r.PathValue("id")
	ctx, cancel := s.ctx(r)
	defer cancel()
	st, err := s.maintenance(s.cfg.Local).RemoveNode(ctx, &botmanagerpb.RemoveNodeRequest{NodeId: id})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	s.catchUp(r.Context())
	s.audit(user, "remove_node", "node_id", id)
	writeJSON(w, http.StatusOK, protoJSON(st))
}

// handlePing runs PingTelegram on the chosen node: without bot_id it checks
// the node's default route to Telegram, with bot_id the bot's own token and
// proxy.
func (s *Server) handlePing(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		NodeID string `json:"node_id"`
		BotID  string `json:"bot_id"`
	}
	if err := decodeJSON(r, &req); err != nil || req.NodeID == "" {
		writeError(w, http.StatusBadRequest, "node_id is required")
		return
	}
	ctx, cancel := s.ctx(r)
	defer cancel()

	addr := ""
	if req.NodeID != s.cfg.LocalNodeID {
		st, err := s.maintenance(s.cfg.Local).GetClusterStatus(ctx, &botmanagerpb.Empty{})
		if err != nil {
			writeGRPCError(w, err)
			return
		}
		for _, n := range st.GetNodes() {
			if n.GetNodeId() == req.NodeID {
				addr = n.GetGrpcAddress()
			}
		}
		if addr == "" {
			writeError(w, http.StatusNotFound, "node "+req.NodeID+" is unknown or has no gRPC address")
			return
		}
	}
	conn, err := s.nodeConn(req.NodeID, addr)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	res, err := s.maintenance(conn).PingTelegram(ctx, &botmanagerpb.PingTelegramRequest{BotId: req.BotID})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	s.audit(user, "ping_telegram", "node_id", req.NodeID, "bot_id", req.BotID, "success", res.GetSuccess())
	writeJSON(w, http.StatusOK, protoJSON(res))
}

func (s *Server) handleCreateBot(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		DisplayName  string `json:"display_name"`
		Token        string `json:"token"`
		ProxyAddress string `json:"proxy_address"`
		ProxyMode    string `json:"proxy_mode"` // "default" | "custom" | "none"
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Token) == "" {
		writeError(w, http.StatusBadRequest, "token is required")
		return
	}
	create := &botmanagerpb.CreateBotRequest{DisplayName: strings.TrimSpace(req.DisplayName), Token: strings.TrimSpace(req.Token)}
	switch req.ProxyMode {
	case "", "default":
	case "none":
		create.Proxy = &botmanagerpb.ProxyConfig{Enabled: false}
	case "custom":
		if !strings.HasPrefix(req.ProxyAddress, "socks5://") && !strings.HasPrefix(req.ProxyAddress, "socks5h://") {
			writeError(w, http.StatusBadRequest, "proxy_address must start with socks5h:// or socks5://")
			return
		}
		create.Proxy = &botmanagerpb.ProxyConfig{Enabled: true, Address: req.ProxyAddress}
	default:
		writeError(w, http.StatusBadRequest, "proxy_mode must be default, custom or none")
		return
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	bot, err := botmanagerpb.NewBotAdminClient(s.cfg.Local).CreateBot(ctx, create)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	s.catchUp(r.Context())
	s.audit(user, "create_bot", "bot_id", bot.GetId())
	writeJSON(w, http.StatusOK, protoJSON(bot))
}

func (s *Server) handleSetBotState(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		State string `json:"state"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var state botmanagerpb.BotState
	switch req.State {
	case "ENABLED":
		state = botmanagerpb.BotState_BOT_STATE_ENABLED
	case "DISABLED":
		state = botmanagerpb.BotState_BOT_STATE_DISABLED
	default:
		writeError(w, http.StatusBadRequest, "state must be ENABLED or DISABLED")
		return
	}
	id := r.PathValue("id")
	ctx, cancel := s.ctx(r)
	defer cancel()
	bot, err := botmanagerpb.NewBotAdminClient(s.cfg.Local).SetBotState(ctx, &botmanagerpb.SetBotStateRequest{Id: id, State: state})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	s.catchUp(r.Context())
	s.audit(user, "set_bot_state", "bot_id", id, "state", req.State)
	writeJSON(w, http.StatusOK, protoJSON(bot))
}

func (s *Server) handleDeleteBot(w http.ResponseWriter, r *http.Request, user string) {
	id := r.PathValue("id")
	ctx, cancel := s.ctx(r)
	defer cancel()
	if _, err := botmanagerpb.NewBotAdminClient(s.cfg.Local).DeleteBot(ctx, &botmanagerpb.DeleteBotRequest{Id: id}); err != nil {
		writeGRPCError(w, err)
		return
	}
	s.catchUp(r.Context())
	s.audit(user, "delete_bot", "bot_id", id)
	w.WriteHeader(http.StatusNoContent)
}

// handleBotMessages returns the bot's most recent messages, newest first.
// Message texts are shown to logged-in administrators only.
func (s *Server) handleBotMessages(w http.ResponseWriter, r *http.Request, _ string) {
	limit := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 500 {
		limit = v
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	// История бота ограничена retention и отдаётся от старых к новым —
	// забираем её целиком и берём хвост.
	list, err := botmanagerpb.NewMessagingClient(s.cfg.Local).ListMessages(ctx, &botmanagerpb.ListMessagesRequest{BotId: r.PathValue("id"), PageSize: 100000})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	msgs := list.GetMessages()
	out := make([]json.RawMessage, 0, limit)
	for i := len(msgs) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, protoJSON(msgs[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

// redactProxy removes credentials from a proxy URL before it reaches the
// browser.
func redactProxy(addr string) string {
	u, err := url.Parse(addr)
	if err != nil || u.User == nil {
		return addr
	}
	u.User = nil
	return u.String()
}
