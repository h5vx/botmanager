package rpcserver

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

// BotAdminServer implements botmanagerpb.BotAdminServer. Every write goes
// through raftcluster.Node.Apply; every read goes through
// Node.ListBots/GetBot. GetChat is the exception — it makes a live
// Telegram call instead (see doc.go).
type BotAdminServer struct {
	botmanagerpb.UnimplementedBotAdminServer

	node        *raftcluster.Node
	nodeProxy   raftcluster.ProxyConfig
	apiBaseURL  string
	httpTimeout time.Duration
	logger      *slog.Logger
	newID       func() (string, error)
}

// NewBotAdminServer constructs a BotAdminServer. nodeProxy is this node's
// proxy.* default (config.yaml, the node-default level); apiBaseURL
// overrides Telegram's host for GetChat's on-demand client ("" =
// api.telegram.org — tests point it at an httptest.Server, the same
// pattern internal/telegram itself uses). logger defaults to
// slog.Default() when nil.
func NewBotAdminServer(node *raftcluster.Node, nodeProxy raftcluster.ProxyConfig, apiBaseURL string, httpTimeout time.Duration, logger *slog.Logger) *BotAdminServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &BotAdminServer{
		node:        node,
		nodeProxy:   nodeProxy,
		apiBaseURL:  apiBaseURL,
		httpTimeout: httpTimeoutOrDefault(httpTimeout),
		logger:      logger,
		newID:       newBotID,
	}
}

func (s *BotAdminServer) CreateBot(_ context.Context, req *botmanagerpb.CreateBotRequest) (*botmanagerpb.Bot, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}
	if req.GetToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	id, err := s.newID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	cmd := raftcluster.Command{
		Type: raftcluster.CommandCreateBot,
		CreateBot: &raftcluster.CreateBotCommand{
			ID:          id,
			DisplayName: req.GetDisplayName(),
			Token:       req.GetToken(),
			Proxy:       proxyFromProto(req.GetProxy()),
			CreatedAt:   time.Now().UTC(),
		},
	}
	res, err := s.node.Apply(cmd, applyTimeout)
	if err != nil {
		return nil, applyError(err)
	}
	return botToProto(*res.Bot), nil
}

func (s *BotAdminServer) UpdateBot(_ context.Context, req *botmanagerpb.UpdateBotRequest) (*botmanagerpb.Bot, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	cmd := raftcluster.Command{
		Type: raftcluster.CommandUpdateBot,
		UpdateBot: &raftcluster.UpdateBotCommand{
			ID:          req.GetId(),
			Token:       req.Token,
			DisplayName: req.DisplayName,
			// req.Proxy is already nil exactly when the client omitted the
			// field (proto3 message fields are nullable regardless of the
			// explicit "optional" keyword in the .proto) — its presence is
			// ProxySet, its value converts straight through.
			ProxySet:   req.Proxy != nil,
			ProxyValue: proxyFromProto(req.GetProxy()),
			UpdatedAt:  time.Now().UTC(),
		},
	}
	res, err := s.node.Apply(cmd, applyTimeout)
	if err != nil {
		return nil, applyError(err)
	}
	return botToProto(*res.Bot), nil
}

// SetBotState only accepts ENABLED/DISABLED, per the .proto's own comment
// on SetBotStateRequest.state: BROKEN is set automatically by
// internal/telegram, DELETED only through DeleteBot.
func (s *BotAdminServer) SetBotState(_ context.Context, req *botmanagerpb.SetBotStateRequest) (*botmanagerpb.Bot, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}

	var target raftcluster.BotState
	switch req.GetState() {
	case botmanagerpb.BotState_BOT_STATE_ENABLED:
		target = raftcluster.BotStateEnabled
	case botmanagerpb.BotState_BOT_STATE_DISABLED:
		target = raftcluster.BotStateDisabled
	default:
		return nil, status.Errorf(codes.InvalidArgument,
			"state must be ENABLED or DISABLED (got %s); BROKEN is set automatically, DELETED only via DeleteBot", req.GetState())
	}

	cmd := raftcluster.Command{
		Type: raftcluster.CommandSetBotState,
		SetBotState: &raftcluster.SetBotStateCommand{
			ID:        req.GetId(),
			State:     target,
			UpdatedAt: time.Now().UTC(),
		},
	}
	res, err := s.node.Apply(cmd, applyTimeout)
	if err != nil {
		return nil, applyError(err)
	}
	return botToProto(*res.Bot), nil
}

func (s *BotAdminServer) DeleteBot(_ context.Context, req *botmanagerpb.DeleteBotRequest) (*botmanagerpb.Empty, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}

	cmd := raftcluster.Command{
		Type: raftcluster.CommandDeleteBot,
		DeleteBot: &raftcluster.DeleteBotCommand{
			ID:        req.GetId(),
			UpdatedAt: time.Now().UTC(),
		},
	}
	if _, err := s.node.Apply(cmd, applyTimeout); err != nil {
		return nil, applyError(err)
	}
	return &botmanagerpb.Empty{}, nil
}

// ListBots slices Node.ListBots' full (ID-sorted) result — the node itself
// has no pagination, see CLAUDE.md.
func (s *BotAdminServer) ListBots(_ context.Context, req *botmanagerpb.ListBotsRequest) (*botmanagerpb.BotList, error) {
	all := s.node.ListBots()
	page, next, err := paginateBots(all, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	out := &botmanagerpb.BotList{NextPageToken: next, Bots: make([]*botmanagerpb.Bot, 0, len(page))}
	for _, b := range page {
		out.Bots = append(out.Bots, botToProto(b))
	}
	return out, nil
}

func (s *BotAdminServer) GetBot(_ context.Context, req *botmanagerpb.GetBotRequest) (*botmanagerpb.Bot, error) {
	bot, ok := s.node.GetBot(req.GetId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "bot %s not found", req.GetId())
	}
	return botToProto(bot), nil
}

// GetChat answers "is the bot a member, what rights" with three live Telegram
// calls against a one-off client for the bot: getMe (learn the bot's own
// numeric user id — raftcluster.Bot does not persist it, see
// CLAUDE.md), getChat (chat title), getChatMember with the
// bot's own id (membership + rights, via Client.GetBotMembership — see its
// doc comment for why getChat alone cannot answer this).
func (s *BotAdminServer) GetChat(ctx context.Context, req *botmanagerpb.GetChatRequest) (*botmanagerpb.Chat, error) {
	bot, ok := s.node.GetBot(req.GetBotId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "bot %s not found", req.GetBotId())
	}
	if bot.State == raftcluster.BotStateDeleted {
		return nil, status.Errorf(codes.FailedPrecondition, "bot %s is deleted", bot.ID)
	}

	client, err := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()

	me, err := client.GetMe(ctx)
	if err != nil {
		return nil, telegramError("get_chat: getMe", err)
	}
	chat, err := client.GetChat(ctx, req.GetChatId())
	if err != nil {
		return nil, telegramError("get_chat: getChat", err)
	}
	member, err := client.GetBotMembership(ctx, req.GetChatId(), me.ID)
	if err != nil {
		return nil, telegramError("get_chat: getChatMember", err)
	}

	isMember, canPin, canSend := chatMemberRights(member.Status, member.CanPinMessages)
	return &botmanagerpb.Chat{
		ChatId:      chat.ID,
		Title:       chat.Title,
		BotIsMember: isMember,
		BotCanPin:   canPin,
		BotCanSend:  canSend,
	}, nil
}

// ListChats answers the "chat picker" need: chats the bot can currently be
// used in, each with a best-effort human-readable title, most-recent
// first.
//
// Two sources, merged:
//
//  1. The chat registry (raftcluster.Node.ListChatRegistry) — built from
//     my_chat_member events (see raftcluster.ChatMembership,
//     internal/telegram's recordChatMembership). This is the ONLY source
//     that already has a chat the bot was just added to and hasn't
//     exchanged a single message in yet — exactly the case that matters
//     for posting into a freshly-added group. Entries currently
//     marked not-a-member (bot removed/left) are excluded here and also
//     used to suppress a stale message-history entry for the same chat
//     below — a chat the bot was kicked from must not be offered as a
//     publish target even if old messages to it still exist.
//  2. Message history (raftcluster.Node.ListChatIDs), for chats predating
//     the registry (deployed before this feature) or any chat that for
//     whatever reason never produced a my_chat_member event. Titles for
//     these are fetched live, one bounded getChat call per chat, same as
//     before this change.
//
// # ponytail: sequential getChat calls for the history fallback under one
// shared timeout; if a bot ever accumulates many distinct history-only
// chats this should switch to bounded concurrency or cache titles instead
// of calling live on every request.
//
// A chat whose title can't be determined still appears with an empty
// title; the caller falls back to showing the bare chat_id (see .proto
// comment on ChatSummary.title).
func (s *BotAdminServer) ListChats(ctx context.Context, req *botmanagerpb.ListChatsRequest) (*botmanagerpb.ChatList, error) {
	bot, ok := s.node.GetBot(req.GetBotId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "bot %s not found", req.GetBotId())
	}

	registry := s.node.ListChatRegistry(bot.ID) // most-recently-changed first
	out := &botmanagerpb.ChatList{Chats: make([]*botmanagerpb.ChatSummary, 0, len(registry))}

	seen := make(map[int64]bool, len(registry))
	notMember := make(map[int64]bool, len(registry))
	for _, entry := range registry {
		if !entry.IsMember {
			notMember[entry.ChatID] = true
			continue
		}
		out.Chats = append(out.Chats, &botmanagerpb.ChatSummary{ChatId: entry.ChatID, Title: entry.Title})
		seen[entry.ChatID] = true
	}

	chatIDs := s.node.ListChatIDs(bot.ID)
	var remaining []int64
	for _, chatID := range chatIDs {
		if seen[chatID] || notMember[chatID] {
			continue
		}
		remaining = append(remaining, chatID)
	}
	if len(remaining) == 0 {
		return out, nil
	}

	client, clientErr := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if clientErr != nil {
		for _, chatID := range remaining {
			out.Chats = append(out.Chats, &botmanagerpb.ChatSummary{ChatId: chatID})
		}
		return out, nil
	}

	tctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()
	for _, chatID := range remaining {
		title := ""
		if chat, err := client.GetChat(tctx, chatID); err == nil {
			title = chat.Title
		}
		out.Chats = append(out.Chats, &botmanagerpb.ChatSummary{ChatId: chatID, Title: title})
	}
	return out, nil
}

// GetUserProfilePhoto — аватары пользователей: текущая фотография
// профиля пользователя Telegram, через одноразовый клиент, тем же
// паттерном, что GetChat. found=false — штатный, ожидаемый исход (у
// пользователя нет ни одной фотографии профиля), НЕ ошибка: вызывающая
// сторона в этом случае просто показывает свою заглушку (см. комментарий на GetUserProfilePhotoResult в .proto).
func (s *BotAdminServer) GetUserProfilePhoto(ctx context.Context, req *botmanagerpb.GetUserProfilePhotoRequest) (*botmanagerpb.GetUserProfilePhotoResult, error) {
	bot, ok := s.node.GetBot(req.GetBotId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "bot %s not found", req.GetBotId())
	}
	if bot.State == raftcluster.BotStateDeleted {
		return nil, status.Errorf(codes.FailedPrecondition, "bot %s is deleted", bot.ID)
	}

	client, err := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()

	photo, err := client.GetUserProfilePhoto(ctx, req.GetUserId())
	if err != nil {
		return nil, telegramError("get_user_profile_photo", err)
	}
	if photo == nil {
		return &botmanagerpb.GetUserProfilePhotoResult{Found: false}, nil
	}
	return &botmanagerpb.GetUserProfilePhotoResult{
		Found:       true,
		Data:        photo.Data,
		ContentType: photo.ContentType,
	}, nil
}
