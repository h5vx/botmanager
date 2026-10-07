package rpcserver

import (
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/telegram"
)

// applyTimeout bounds every Node.Apply call this package makes — long
// enough for a normal Raft commit round-trip on a healthy cluster, short
// enough that a caller waiting on a stuck/partitioned cluster gets an
// error back instead of hanging indefinitely.
const applyTimeout = 5 * time.Second

// defaultHTTPTimeout bounds a single on-demand Telegram Bot API call made
// by this package (GetChat, EditMessage, ...) when no explicit timeout was
// configured.
const defaultHTTPTimeout = 10 * time.Second

func httpTimeoutOrDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultHTTPTimeout
	}
	return d
}

// requireLeader rejects a write RPC with a precise gRPC error when this
// node is not the current Raft leader, instead of letting Node.Apply fail
// with the less specific raftcluster.ErrNotLeader. With the Forwarder
// interceptor installed (cmd/botmanager) such a request never reaches the
// handler on a follower; this is the safety net for servers without it and
// for the narrow window in which leadership moves (see doc.go).
func requireLeader(node *raftcluster.Node) error {
	if node.IsLeader() {
		return nil
	}
	if addr := node.LeaderAddr(); addr != "" {
		return status.Errorf(codes.FailedPrecondition,
			"this node is not the raft leader; current leader is node %q at %s",
			node.LeaderID(), addr)
	}
	return status.Error(codes.Unavailable, "raft leader is not currently known on this node")
}

// applyError maps a raftcluster.Node.Apply error to a gRPC status.
func applyError(err error) error {
	switch {
	case errors.Is(err, raftcluster.ErrNotLeader):
		// Leadership moved (or is being transferred) between
		// requireLeader's check and this Apply call — retryable: by the
		// time the client retries, another node leads.
		return status.Error(codes.Unavailable, "raft leadership is changing; retry")
	case errors.Is(err, raftcluster.ErrBotNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, raftcluster.ErrBotAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, raftcluster.ErrMessageNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, raftcluster.ErrInvalidCommand):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// telegramClientFor builds a one-off telegram.Client for bot, honoring
// the per-bot/node-default proxy resolution (telegram.EffectiveProxy) —
// for RPCs that make a single live Telegram call instead of going through
// the bot's persistent Runner (see doc.go).
func telegramClientFor(bot raftcluster.Bot, nodeProxy raftcluster.ProxyConfig, apiBaseURL string) (*telegram.Client, error) {
	eff := telegram.EffectiveProxy(nodeProxy, bot.Proxy)
	httpClient, err := telegram.NewHTTPClient(eff)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build telegram client for bot %s: %v", bot.ID, err)
	}
	return telegram.NewClient(bot.Token, httpClient, apiBaseURL), nil
}

// telegramError maps a telegram.Client call failure to a gRPC status using
// the same failure classification (telegram.ClassifyFailure) — the same
// classification the async delivery pipeline (internal/telegram.Runner)
// acts on, so a synchronous RPC caller gets an equivalent class of
// explanation.
func telegramError(op string, err error) error {
	if err == nil {
		return nil
	}
	switch telegram.ClassifyFailure(err) {
	case raftcluster.FailureClassNode:
		return status.Errorf(codes.Unavailable, "%s: telegram unreachable: %v", op, err)
	case raftcluster.FailureClassRateLimit:
		return status.Errorf(codes.ResourceExhausted, "%s: telegram rate limit: %v", op, err)
	case raftcluster.FailureClassBot:
		return status.Errorf(codes.FailedPrecondition, "%s: bot cannot perform this action: %v", op, err)
	case raftcluster.FailureClassRecipient:
		return status.Errorf(codes.FailedPrecondition, "%s: recipient unreachable: %v", op, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", op, err)
	}
}
