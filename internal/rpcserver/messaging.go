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

// MessagingServer implements botmanagerpb.MessagingServer. Send/SendBatch/
// CancelPending go through raftcluster.Node.Apply; EditMessage/
// DeleteMessage/PinMessage/UnpinMessage/AnswerCallback make a live Telegram
// call the same way BotAdminServer.GetChat does (see doc.go); GetMessage/
// GetMessages/ListMessages are plain reads; Subscribe streams the
// replicated journal (messaging_subscribe.go).
type MessagingServer struct {
	botmanagerpb.UnimplementedMessagingServer

	node        *raftcluster.Node
	nodeProxy   raftcluster.ProxyConfig
	apiBaseURL  string
	httpTimeout time.Duration
	logger      *slog.Logger
}

// NewMessagingServer constructs a MessagingServer. See NewBotAdminServer for
// the meaning of nodeProxy/apiBaseURL/httpTimeout — the on-demand Telegram
// client this server builds for EditMessage/DeleteMessage/PinMessage/
// UnpinMessage/AnswerCallback follows the exact same construction
// (telegramClientFor in leader.go).
func NewMessagingServer(node *raftcluster.Node, nodeProxy raftcluster.ProxyConfig, apiBaseURL string, httpTimeout time.Duration, logger *slog.Logger) *MessagingServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &MessagingServer{
		node:        node,
		nodeProxy:   nodeProxy,
		apiBaseURL:  apiBaseURL,
		httpTimeout: httpTimeoutOrDefault(httpTimeout),
		logger:      logger,
	}
}

func (s *MessagingServer) Send(_ context.Context, req *botmanagerpb.SendRequest) (*botmanagerpb.SendAck, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}
	ack, err := s.send(req)
	if err != nil {
		return nil, err
	}
	return ack, nil
}

// send is Send's body factored out so SendBatch can reuse it per-message
// without repeating requireLeader on every element of the batch.
func (s *MessagingServer) send(req *botmanagerpb.SendRequest) (*botmanagerpb.SendAck, error) {
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	buttons, err := buttonsFromProto(req.GetButtons())
	if err != nil {
		return nil, err
	}

	cmd := raftcluster.Command{
		Type: raftcluster.CommandPutMessage,
		PutMessage: &raftcluster.PutMessageCommand{
			IdempotencyKey: req.GetIdempotencyKey(),
			BotID:          req.GetBotId(),
			ChatID:         req.GetChatId(),
			Text:           req.GetText(),
			Buttons:        buttons,
			Priority:       priorityFromProto(req.GetPriority()),
			CreatedAt:      time.Now().UTC(),
		},
	}
	res, err := s.node.Apply(cmd, applyTimeout)
	if err != nil {
		return nil, applyError(err)
	}
	return &botmanagerpb.SendAck{
		IdempotencyKey: res.Message.IdempotencyKey,
		Status:         deliveryStatusToProto(res.Message.Delivery.Status),
	}, nil
}

// SendBatchAck's acks are returned in the same order as the incoming
// SendBatchRequest.messages (there is no reason to reorder, and a
// caller matching acks back to requests by position needs this).
// req.pin/req.reply_to_message_id are accepted by the .proto but
// have no equivalent in raftcluster.PutMessageCommand yet — pin/reply are
// send-time Telegram parameters, not part of the replicated message record;
// wiring them through to the eventual Telegram call is future work for
// internal/telegram.sendLoop, not this RPC layer (Apply only records the
// message and its delivery status).
func (s *MessagingServer) SendBatch(_ context.Context, req *botmanagerpb.SendBatchRequest) (*botmanagerpb.SendBatchAck, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}

	out := &botmanagerpb.SendBatchAck{Acks: make([]*botmanagerpb.SendAck, 0, len(req.GetMessages()))}
	for _, m := range req.GetMessages() {
		ack, err := s.send(m)
		if err != nil {
			return nil, err
		}
		out.Acks = append(out.Acks, ack)
	}
	return out, nil
}

func (s *MessagingServer) EditMessage(ctx context.Context, req *botmanagerpb.EditMessageRequest) (*botmanagerpb.SendAck, error) {
	msg, bot, err := s.messageAndBot(req.GetIdempotencyKey())
	if err != nil {
		return nil, err
	}

	client, err := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()

	if _, err := client.EditMessageText(ctx, msg.ChatID, msg.MessageID, req.GetText()); err != nil {
		return nil, telegramError("edit_message", err)
	}
	return &botmanagerpb.SendAck{IdempotencyKey: msg.IdempotencyKey, Status: deliveryStatusToProto(msg.Delivery.Status)}, nil
}

func (s *MessagingServer) DeleteMessage(ctx context.Context, req *botmanagerpb.DeleteMessageRequest) (*botmanagerpb.Empty, error) {
	msg, bot, err := s.messageAndBot(req.GetIdempotencyKey())
	if err != nil {
		return nil, err
	}

	client, err := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()

	if err := client.DeleteMessage(ctx, msg.ChatID, msg.MessageID); err != nil {
		return nil, telegramError("delete_message", err)
	}
	return &botmanagerpb.Empty{}, nil
}

func (s *MessagingServer) PinMessage(ctx context.Context, req *botmanagerpb.PinMessageRequest) (*botmanagerpb.Empty, error) {
	msg, bot, err := s.messageAndBot(req.GetIdempotencyKey())
	if err != nil {
		return nil, err
	}

	client, err := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()

	if err := client.PinChatMessage(ctx, msg.ChatID, msg.MessageID); err != nil {
		return nil, telegramError("pin_message", err)
	}
	return &botmanagerpb.Empty{}, nil
}

func (s *MessagingServer) UnpinMessage(ctx context.Context, req *botmanagerpb.UnpinMessageRequest) (*botmanagerpb.Empty, error) {
	msg, bot, err := s.messageAndBot(req.GetIdempotencyKey())
	if err != nil {
		return nil, err
	}

	client, err := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()

	if err := client.UnpinChatMessage(ctx, msg.ChatID, msg.MessageID); err != nil {
		return nil, telegramError("unpin_message", err)
	}
	return &botmanagerpb.Empty{}, nil
}

// messageAndBot looks up the message by idempotency key and then its owning
// bot — the shared first two steps of EditMessage/DeleteMessage/PinMessage/
// UnpinMessage (see doc.go).
func (s *MessagingServer) messageAndBot(idempotencyKey string) (raftcluster.Message, raftcluster.Bot, error) {
	if idempotencyKey == "" {
		return raftcluster.Message{}, raftcluster.Bot{}, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	msg, ok := s.node.GetMessage(idempotencyKey)
	if !ok {
		return raftcluster.Message{}, raftcluster.Bot{}, status.Errorf(codes.NotFound, "message %s not found", idempotencyKey)
	}
	bot, ok := s.node.GetBot(msg.BotID)
	if !ok {
		return raftcluster.Message{}, raftcluster.Bot{}, status.Errorf(codes.NotFound, "bot %s not found", msg.BotID)
	}
	return msg, bot, nil
}

// CancelPending applies CommandCancelPending and reports whether the
// message was actually still cancellable (CancelAck.cancelled is
// false, not an error, when the message already reached SENT/FAILED) —
// see raftcluster.FSM.applyCancelPending.
func (s *MessagingServer) CancelPending(_ context.Context, req *botmanagerpb.CancelPendingRequest) (*botmanagerpb.CancelAck, error) {
	if err := requireLeader(s.node); err != nil {
		return nil, err
	}
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	cmd := raftcluster.Command{
		Type: raftcluster.CommandCancelPending,
		CancelPending: &raftcluster.CancelPendingCommand{
			IdempotencyKey: req.GetIdempotencyKey(),
			CancelledAt:    time.Now().UTC(),
		},
	}
	res, err := s.node.Apply(cmd, applyTimeout)
	if err != nil {
		return nil, applyError(err)
	}
	return &botmanagerpb.CancelAck{
		IdempotencyKey: res.Message.IdempotencyKey,
		Cancelled:      res.Message.Delivery.Status == raftcluster.DeliveryStatusCancelled,
	}, nil
}

// AnswerCallback makes a live answerCallbackQuery call — req.bot_id (see
// doc.go/CLAUDE.md) identifies which bot's
// token to use, since callback_query_id alone does not.
func (s *MessagingServer) AnswerCallback(ctx context.Context, req *botmanagerpb.AnswerCallbackRequest) (*botmanagerpb.Empty, error) {
	bot, ok := s.node.GetBot(req.GetBotId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "bot %s not found", req.GetBotId())
	}

	client, err := telegramClientFor(bot, s.nodeProxy, s.apiBaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.httpTimeout)
	defer cancel()

	if err := client.AnswerCallbackQuery(ctx, req.GetCallbackQueryId(), req.GetText(), req.GetShowAlert()); err != nil {
		return nil, telegramError("answer_callback", err)
	}
	return &botmanagerpb.Empty{}, nil
}

func (s *MessagingServer) GetMessage(_ context.Context, req *botmanagerpb.GetMessageRequest) (*botmanagerpb.Message, error) {
	msg, ok := s.node.GetMessage(req.GetIdempotencyKey())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "message %s not found", req.GetIdempotencyKey())
	}
	return messageToProto(msg), nil
}

// GetMessages looks up each requested key independently — a key with no
// matching message is simply absent from the result rather than failing
// the whole batch, matching GetMessage's own single-key NotFound semantics
// applied per element instead of to the call as a whole.
func (s *MessagingServer) GetMessages(_ context.Context, req *botmanagerpb.GetMessagesRequest) (*botmanagerpb.MessageList, error) {
	out := &botmanagerpb.MessageList{Messages: make([]*botmanagerpb.Message, 0, len(req.GetIdempotencyKeys()))}
	for _, key := range req.GetIdempotencyKeys() {
		if msg, ok := s.node.GetMessage(key); ok {
			out.Messages = append(out.Messages, messageToProto(msg))
		}
	}
	return out, nil
}

func (s *MessagingServer) ListMessages(_ context.Context, req *botmanagerpb.ListMessagesRequest) (*botmanagerpb.MessageList, error) {
	filter := raftcluster.ListMessagesFilter{
		BotID:       req.GetBotId(),
		ChatID:      req.GetChatId(),
		Status:      deliveryStatusFromProto(req.GetStatus()),
		CreatedFrom: timeFromProtoOrZero(req.GetCreatedFrom()),
		CreatedTo:   timeFromProtoOrZero(req.GetCreatedTo()),
	}
	msgs, next, err := s.node.ListMessages(filter, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	out := &botmanagerpb.MessageList{NextPageToken: next, Messages: make([]*botmanagerpb.Message, 0, len(msgs))}
	for _, m := range msgs {
		out.Messages = append(out.Messages, messageToProto(m))
	}
	return out, nil
}

// Subscribe (server-streaming) lives in messaging_subscribe.go — kept
// separate to stay under the project's ~400-line-per-file guideline: it
// streams the replicated journal plus its own small conversion/filtering
// helpers, and reads naturally as one self-contained unit apart from the request/response RPCs above.
