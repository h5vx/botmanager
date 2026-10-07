package rpcserver

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/telegram"
	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
)

// VerifyInitData checks a Telegram Mini App initData string's signature
// using the target bot's token from raftcluster — see CLAUDE.md,
// "VerifyInitData" for why this whole check lives in botmanager rather
// than in the caller (callers never see Bot.Token). This is
// the only place in BotAdminServer that reads Bot.Token for anything other
// than passing it straight through to internal/telegram's HTTP client, but
// exactly like GetBot/ListBots (see botToProto), the token itself never
// reaches the response — initDataResultToProto below never touches it.
//
// No live Telegram call: HMAC verification is entirely local, so this
// method does not call requireLeader (it never calls Node.Apply) and works
// on any node, replica included — same reasoning as GetChat/GetBot (see
// doc.go's "Leadership" section).
//
// The bot need not be Enabled: an operator sets up a bot (Disabled while
// configuring, or already Broken from a bad token) and must still be able
// to verify a Mini App test launch against it before flipping it on — the
// lifecycle does not gate "can this bot's Mini App be used" on runtime state, only
// message delivery is. Only Deleted is rejected, matching GetChat, since a
// deleted bot's token is no longer meaningful to verify anything against.
func (s *BotAdminServer) VerifyInitData(_ context.Context, req *botmanagerpb.VerifyInitDataRequest) (*botmanagerpb.VerifyInitDataResult, error) {
	bot, ok := s.node.GetBot(req.GetBotId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "bot %s not found", req.GetBotId())
	}
	if bot.State == raftcluster.BotStateDeleted {
		return nil, status.Errorf(codes.FailedPrecondition, "bot %s is deleted", bot.ID)
	}

	maxAge := time.Duration(req.GetMaxAuthAgeSeconds()) * time.Second
	parsed, err := telegram.VerifyInitData(req.GetInitData(), bot.Token, maxAge, time.Now().UTC())
	return initDataResultToProto(parsed, err), nil
}

// initDataResultToProto never has a token to leak in the first place —
// telegram.VerifyInitData's own result type (telegram.InitData) does not
// carry one. A verification failure is not a gRPC error: valid == false
// with invalid_reason is the answer, not an exceptional condition (the
// caller always wants an answer to render to the user, e.g. "session expired, reopen
// the app", not a generic RPC failure).
func initDataResultToProto(parsed telegram.InitData, err error) *botmanagerpb.VerifyInitDataResult {
	if err != nil {
		reason := "bad_signature"
		if errors.Is(err, telegram.ErrInitDataExpired) {
			reason = "expired"
		}
		return &botmanagerpb.VerifyInitDataResult{Valid: false, InvalidReason: reason}
	}
	return &botmanagerpb.VerifyInitDataResult{
		Valid:           true,
		TelegramUserId:  parsed.User.ID,
		Username:        parsed.User.Username,
		AuthDate:        toProtoTime(parsed.AuthDate),
		StartParam:      parsed.StartParam,
		AllowsWriteToPm: parsed.User.AllowsWriteToPm,
	}
}
