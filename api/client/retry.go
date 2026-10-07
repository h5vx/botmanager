package client

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
)

// retryableMethods are the calls that are safe to repeat: reads, calls
// made idempotent by an idempotency key (Send/SendBatch) or by their own
// semantics (SetBotState, CancelPending, ...). BotAdmin.CreateBot is
// deliberately absent: if the first attempt was applied but its answer was
// lost, a retry would create a second bot. EditMessage/DeleteMessage and
// the other live Telegram calls are absent for the same reason (a repeated
// delete fails, a repeated answer may be rejected).
var retryableMethods = map[string]bool{
	botmanagerpb.Messaging_Send_FullMethodName:          true,
	botmanagerpb.Messaging_SendBatch_FullMethodName:     true,
	botmanagerpb.Messaging_CancelPending_FullMethodName: true,
	botmanagerpb.Messaging_GetMessage_FullMethodName:    true,
	botmanagerpb.Messaging_GetMessages_FullMethodName:   true,
	botmanagerpb.Messaging_ListMessages_FullMethodName:  true,

	botmanagerpb.BotAdmin_UpdateBot_FullMethodName:           true,
	botmanagerpb.BotAdmin_SetBotState_FullMethodName:         true,
	botmanagerpb.BotAdmin_DeleteBot_FullMethodName:           true,
	botmanagerpb.BotAdmin_ListBots_FullMethodName:            true,
	botmanagerpb.BotAdmin_GetBot_FullMethodName:              true,
	botmanagerpb.BotAdmin_GetChat_FullMethodName:             true,
	botmanagerpb.BotAdmin_ListChats_FullMethodName:           true,
	botmanagerpb.BotAdmin_VerifyInitData_FullMethodName:      true,
	botmanagerpb.BotAdmin_GetUserProfilePhoto_FullMethodName: true,

	botmanagerpb.Maintenance_GetClusterStatus_FullMethodName: true,
	botmanagerpb.Maintenance_PingTelegram_FullMethodName:     true,
}

const (
	retryMinBackoff = 50 * time.Millisecond
	retryMaxBackoff = 1 * time.Second
)

// retryInterceptor retries retryable calls that fail with UNAVAILABLE —
// the code botmanager uses for "no leader right now" and the code gRPC
// uses for an unreachable node — with exponential backoff, for at most
// limit or until the call's context ends. A leader election takes a
// couple of seconds; gRPC's built-in retry policy (at most 5 attempts with
// randomized sub-second backoff) gives up before it finishes.
func retryInterceptor(limit time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if !retryableMethods[method] {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		deadline := time.Now().Add(limit)
		backoff := retryMinBackoff
		for {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if status.Code(err) != codes.Unavailable || time.Now().Add(backoff).After(deadline) {
				return err
			}
			select {
			case <-ctx.Done():
				return err
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > retryMaxBackoff {
				backoff = retryMaxBackoff
			}
		}
	}
}
