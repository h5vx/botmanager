package rpcserver

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

// forwardedMetadataKey marks a request a node has already forwarded to the
// leader. A node receiving a marked request while not being leader itself
// (leadership moved in between) fails it with UNAVAILABLE instead of
// forwarding again, so a request never bounces between nodes.
const forwardedMetadataKey = "x-botmanager-forwarded-by"

// writeMethods are the RPCs that change replicated state and therefore
// must run on the Raft leader.
var writeMethods = []string{
	botmanagerpb.BotAdmin_CreateBot_FullMethodName,
	botmanagerpb.BotAdmin_UpdateBot_FullMethodName,
	botmanagerpb.BotAdmin_SetBotState_FullMethodName,
	botmanagerpb.BotAdmin_DeleteBot_FullMethodName,
	botmanagerpb.Messaging_Send_FullMethodName,
	botmanagerpb.Messaging_SendBatch_FullMethodName,
	botmanagerpb.Messaging_CancelPending_FullMethodName,
	botmanagerpb.Maintenance_TransferLeadership_FullMethodName,
	botmanagerpb.Maintenance_AddNode_FullMethodName,
	botmanagerpb.Maintenance_RemoveNode_FullMethodName,
}

// liveTelegramMethods are the RPCs that call Telegram synchronously. They
// change no replicated state, but are forwarded to the leader too: the
// leader is the node that talks to Telegram (internal/failover moves
// leadership to a node that reaches it), while the node that received the
// call may not reach Telegram at all. Unlike writes they do not need
// leadership, so when no leader is known, or a forwarded call arrives
// after leadership moved, they run on the receiving node.
// Maintenance.PingTelegram is deliberately absent: it checks Telegram from
// this particular node. Reads, VerifyInitData and Subscribe are served by
// whichever node receives them.
var liveTelegramMethods = []string{
	botmanagerpb.BotAdmin_GetChat_FullMethodName,
	botmanagerpb.BotAdmin_ListChats_FullMethodName,
	botmanagerpb.BotAdmin_GetUserProfilePhoto_FullMethodName,
	botmanagerpb.Messaging_EditMessage_FullMethodName,
	botmanagerpb.Messaging_DeleteMessage_FullMethodName,
	botmanagerpb.Messaging_PinMessage_FullMethodName,
	botmanagerpb.Messaging_UnpinMessage_FullMethodName,
	botmanagerpb.Messaging_AnswerCallback_FullMethodName,
}

// Forwarder transparently forwards write and live Telegram RPCs received by
// a follower to the current leader, so clients may talk to any node. It also keeps the pool
// of connections to peers that MaintenanceServer uses for health probes.
type Forwarder struct {
	node     *raftcluster.Node
	dialOpts []grpc.DialOption
	replies  map[string]protoreflect.MessageType // full method -> response type, for every forwarded method
	live     map[string]bool                     // liveTelegramMethods

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewForwarder builds a Forwarder. dialOpts must carry the transport
// credentials for node-to-node calls (the node's own mTLS certificate).
func NewForwarder(node *raftcluster.Node, dialOpts ...grpc.DialOption) (*Forwarder, error) {
	replies := make(map[string]protoreflect.MessageType, len(writeMethods)+len(liveTelegramMethods))
	live := make(map[string]bool, len(liveTelegramMethods))
	for _, m := range slices.Concat(writeMethods, liveTelegramMethods) {
		mt, err := responseType(m)
		if err != nil {
			return nil, err
		}
		replies[m] = mt
	}
	for _, m := range liveTelegramMethods {
		live[m] = true
	}
	return &Forwarder{
		node:     node,
		dialOpts: dialOpts,
		replies:  replies,
		live:     live,
		conns:    make(map[string]*grpc.ClientConn),
	}, nil
}

// responseType resolves the response message type of a gRPC method from the
// generated descriptors, e.g. "/botmanager.v1.Messaging/Send" -> SendAck.
func responseType(fullMethod string) (protoreflect.MessageType, error) {
	parts := strings.Split(strings.TrimPrefix(fullMethod, "/"), "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpcserver: bad method name %q", fullMethod)
	}
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[0]))
	if err != nil {
		return nil, fmt.Errorf("rpcserver: service %s: %w", parts[0], err)
	}
	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("rpcserver: %s is not a service", parts[0])
	}
	md := sd.Methods().ByName(protoreflect.Name(parts[1]))
	if md == nil {
		return nil, fmt.Errorf("rpcserver: method %s not found", fullMethod)
	}
	return protoregistry.GlobalTypes.FindMessageByName(md.Output().FullName())
}

// UnaryInterceptor forwards write and live Telegram RPCs to the leader
// when this node is not the leader; all other calls go to the local
// handler.
func (f *Forwarder) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	mt, forwarded := f.replies[info.FullMethod]
	if !forwarded || f.node.IsLeader() {
		return handler(ctx, req)
	}
	live := f.live[info.FullMethod]
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(forwardedMetadataKey)) > 0 {
		if live {
			return handler(ctx, req)
		}
		return nil, status.Error(codes.Unavailable, "raft leadership moved while the request was being forwarded; retry")
	}
	leader, ok := f.node.LeaderInfo()
	if !ok || leader.GRPCAddr == "" {
		if live {
			return handler(ctx, req)
		}
		return nil, status.Error(codes.Unavailable, "raft leader (or its gRPC address) is not currently known on this node; retry")
	}
	conn, err := f.Conn(leader.GRPCAddr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connect to leader %s: %v", leader.ID, err)
	}

	reply := mt.New().Interface()
	outCtx := metadata.AppendToOutgoingContext(ctx, forwardedMetadataKey, f.node.ID())
	if err := conn.Invoke(outCtx, info.FullMethod, req.(proto.Message), reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// Conn returns a (cached) client connection to a peer's gRPC address.
func (f *Forwarder) Conn(addr string) (*grpc.ClientConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.conns[addr]; ok {
		return c, nil
	}
	// passthrough: адрес отдаётся дайлеру как есть (host:port), без
	// DNS-резолвера grpc и балансировки — нам нужен ровно этот узел.
	c, err := grpc.NewClient("passthrough:///"+addr, f.dialOpts...)
	if err != nil {
		return nil, err
	}
	f.conns[addr] = c
	return c, nil
}

// ProbeTelegram asks the node at grpcAddr whether it reaches Telegram for
// botID right now (its Maintenance.PingTelegram: getMe with the bot's
// token through the bot's effective proxy). Used by internal/failover to
// pick a leadership transfer target that can actually do better.
func (f *Forwarder) ProbeTelegram(ctx context.Context, grpcAddr, botID string) (bool, error) {
	conn, err := f.Conn(grpcAddr)
	if err != nil {
		return false, err
	}
	res, err := botmanagerpb.NewMaintenanceClient(conn).PingTelegram(ctx, &botmanagerpb.PingTelegramRequest{BotId: botID})
	if err != nil {
		return false, err
	}
	return res.GetSuccess(), nil
}

// Close closes every pooled connection.
func (f *Forwarder) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for addr, c := range f.conns {
		_ = c.Close()
		delete(f.conns, addr)
	}
}
