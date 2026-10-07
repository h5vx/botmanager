package rpcserver

import (
	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/telegram"
	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
)

// Subscribe merges two independent event sources into one stream (see
// doc.go): telegram.UpdateBus (incoming_message/callback_query/
// chat_member_changed, produced directly by Telegram long polling) and
// raftcluster.Node.SubscribeEvents (message_status_changed/
// bot_state_changed, produced by the replicated command stream). Both
// subscriptions are cancelled via defer when the stream ends for any reason
// (client disconnect, error, ctx cancellation) — stream.Context() is the
// only lifetime signal a server-streaming RPC gets.
//
// CommandCreateBot/CommandUpdateBot are deliberately NOT translated into
// bot_state_changed here: neither changes Bot.State (enabled/
// disabled/broken/deleted), and BotStateChanged's own fields (state,
// failure_class, reason) have nothing meaningful to report for a plain
// metadata edit — see FSM.Event's doc comment distinguishing "a real state
// transition" from "a plain metadata edit" for the same reasoning.
func (s *MessagingServer) Subscribe(req *botmanagerpb.SubscribeRequest, stream botmanagerpb.Messaging_SubscribeServer) error {
	wanted := botIDSet(req.GetBotIds())

	updCh, cancelUpd := s.bus.Subscribe()
	defer cancelUpd()
	evCh, cancelEv := s.node.SubscribeEvents()
	defer cancelEv()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case u, ok := <-updCh:
			if !ok {
				return nil
			}
			if !wanted.matches(u.BotID) {
				continue
			}
			out := telegramUpdateToProto(u)
			if out == nil {
				continue
			}
			if err := stream.Send(out); err != nil {
				return err
			}

		case ev, ok := <-evCh:
			if !ok {
				return nil
			}
			botID, out := fsmEventToProto(ev)
			if out == nil || !wanted.matches(botID) {
				continue
			}
			if err := stream.Send(out); err != nil {
				return err
			}
		}
	}
}

// botIDFilterSet implements SubscribeRequest.bot_ids' "empty = all bots on
// this node" rule.
type botIDFilterSet map[string]struct{}

func botIDSet(ids []string) botIDFilterSet {
	if len(ids) == 0 {
		return nil // nil set = match everything, see matches below
	}
	m := make(botIDFilterSet, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

func (s botIDFilterSet) matches(botID string) bool {
	if s == nil {
		return true
	}
	_, ok := s[botID]
	return ok
}

// telegramUpdateToProto converts one telegram.Update to a botmanagerpb.Update,
// or nil for an UpdateKind this server does not forward (there is currently
// none — every telegram.UpdateKind has a corresponding Update.payload case
// — but Subscribe treats nil defensively rather than assuming exhaustiveness
// holds forever).
func telegramUpdateToProto(u telegram.Update) *botmanagerpb.Update {
	occurredAt := toProtoTime(u.ReceivedAt)
	switch u.Kind {
	case telegram.UpdateKindIncomingMessage:
		return &botmanagerpb.Update{
			OccurredAt: occurredAt,
			Payload: &botmanagerpb.Update_IncomingMessage{IncomingMessage: &botmanagerpb.IncomingMessage{
				BotId:      u.BotID,
				ChatId:     u.ChatID,
				MessageId:  u.MessageID,
				FromUserId: u.FromUserID,
				Text:       u.Text,
				ReceivedAt: occurredAt,
			}},
		}
	case telegram.UpdateKindCallbackQuery:
		return &botmanagerpb.Update{
			OccurredAt: occurredAt,
			Payload: &botmanagerpb.Update_CallbackQuery{CallbackQuery: &botmanagerpb.CallbackQuery{
				BotId:           u.BotID,
				CallbackQueryId: u.CallbackQueryID,
				ChatId:          u.ChatID,
				MessageId:       u.MessageID,
				FromUserId:      u.FromUserID,
				Data:            u.CallbackData,
			}},
		}
	case telegram.UpdateKindChatMemberChanged:
		return &botmanagerpb.Update{
			OccurredAt: occurredAt,
			Payload: &botmanagerpb.Update_ChatMemberChanged{ChatMemberChanged: &botmanagerpb.ChatMemberChanged{
				BotId:       u.BotID,
				ChatId:      u.ChatID,
				BotIsMember: chatMemberStatusIsMember(u.NewChatMemberStatus),
			}},
		}
	default:
		return nil
	}
}

// chatMemberStatusIsMember mirrors chatMemberRights' "left"/"kicked" =
// not-a-member reading of Telegram's my_chat_member.new_chat_member.status
// (convert.go) — reused here for ChatMemberChanged.bot_is_member, which
// needs only the membership half, not the rights booleans.
func chatMemberStatusIsMember(status string) bool {
	switch status {
	case "left", "kicked":
		return false
	default:
		return status != ""
	}
}

// fsmEventToProto converts one raftcluster.Event to a botmanagerpb.Update
// plus the bot_id to filter Subscribe by, or ("", nil) for an event kind
// Subscribe does not forward (see the doc comment above Subscribe).
func fsmEventToProto(ev raftcluster.Event) (botID string, out *botmanagerpb.Update) {
	switch ev.Command {
	case raftcluster.CommandPutMessage, raftcluster.CommandUpdateDelivery, raftcluster.CommandCancelPending:
		if ev.Message == nil {
			return "", nil
		}
		return ev.Message.BotID, &botmanagerpb.Update{
			OccurredAt: timestamppbNow(),
			Payload: &botmanagerpb.Update_MessageStatusChanged{MessageStatusChanged: &botmanagerpb.MessageStatusChanged{
				IdempotencyKey: ev.Message.IdempotencyKey,
				Delivery:       deliveryToProto(ev.Message.Delivery),
			}},
		}
	case raftcluster.CommandSetBotState, raftcluster.CommandDeleteBot:
		if ev.Bot == nil {
			return "", nil
		}
		return ev.Bot.ID, &botmanagerpb.Update{
			OccurredAt: timestamppbNow(),
			Payload: &botmanagerpb.Update_BotStateChanged{BotStateChanged: &botmanagerpb.BotStateChanged{
				BotId:        ev.Bot.ID,
				State:        botStateToProto(ev.Bot.State),
				FailureClass: failureClassToProto(ev.Bot.LastFailureClass),
				Reason:       ev.Bot.LastFailureReason,
			}},
		}
	default:
		return "", nil
	}
}
