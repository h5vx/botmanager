package rpcserver

import (
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

// subscribeBatch bounds how many journal entries Subscribe copies out of
// the FSM per read.
const subscribeBatch = 512

// Subscribe streams the replicated journal (raftcluster.JournalEntry): both
// updates received from Telegram (incoming_message/callback_query/
// chat_member_changed, recorded by the leader's poll loop) and changes
// produced by commands (message_status_changed/bot_state_changed). Because
// the journal is replicated, any node can serve the stream, and every event
// carries the same sequence number on every node.
//
// Without after_sequence the stream starts at the current end of the
// journal. With after_sequence it first replays every retained entry with a
// higher sequence; if part of the requested range has already been evicted
// by retention, the stream ends with OUT_OF_RANGE so the caller learns it
// missed events instead of silently skipping them. The same happens if a
// subscriber falls so far behind that retention overtakes it.
//
// CommandCreateBot/CommandUpdateBot are not journaled: neither changes
// Bot.State, and a metadata edit has nothing to report in BotStateChanged.
func (s *MessagingServer) Subscribe(req *botmanagerpb.SubscribeRequest, stream botmanagerpb.Messaging_SubscribeServer) error {
	wanted := botIDSet(req.GetBotIds())

	wake, cancel := s.node.SubscribeApplied()
	defer cancel()

	var cursor uint64
	if req.AfterSequence != nil {
		cursor = req.GetAfterSequence()
	} else {
		_, _, cursor = s.node.ReadJournal(^uint64(0), 1)
	}
	if err := stream.SendHeader(metadata.Pairs(botmanagerpb.SubscribePositionHeader, strconv.FormatUint(cursor, 10))); err != nil {
		return err
	}

	ctx := stream.Context()
	for {
		entries, oldest, _ := s.node.ReadJournal(cursor, subscribeBatch)
		if oldest > 0 && cursor+1 < oldest && len(entries) > 0 {
			return status.Errorf(codes.OutOfRange,
				"events after sequence %d are no longer retained (oldest retained: %d); resubscribe without after_sequence", cursor, oldest)
		}
		for _, e := range entries {
			cursor = e.Seq
			if !wanted.matches(e.BotID) {
				continue
			}
			out := journalEntryToProto(e)
			if out == nil {
				continue
			}
			if err := stream.Send(out); err != nil {
				return err
			}
		}
		if len(entries) == subscribeBatch {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

// botIDFilterSet implements SubscribeRequest.bot_ids' "empty = all bots"
// rule.
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

// journalEntryToProto converts one journal entry to a botmanagerpb.Update,
// or nil for a kind this server does not know (defensive: every kind
// written today has a case).
func journalEntryToProto(e raftcluster.JournalEntry) *botmanagerpb.Update {
	out := &botmanagerpb.Update{Sequence: e.Seq, OccurredAt: toProtoTime(e.OccurredAt)}
	switch e.Kind {
	case raftcluster.JournalIncomingMessage:
		if e.Update == nil {
			return nil
		}
		u := e.Update
		out.Payload = &botmanagerpb.Update_IncomingMessage{IncomingMessage: &botmanagerpb.IncomingMessage{
			BotId:      e.BotID,
			ChatId:     u.ChatID,
			MessageId:  u.MessageID,
			FromUserId: u.FromUserID,
			Text:       u.Text,
			ReceivedAt: toProtoTime(u.ReceivedAt),
		}}
	case raftcluster.JournalCallbackQuery:
		if e.Update == nil {
			return nil
		}
		u := e.Update
		out.Payload = &botmanagerpb.Update_CallbackQuery{CallbackQuery: &botmanagerpb.CallbackQuery{
			BotId:           e.BotID,
			CallbackQueryId: u.CallbackQueryID,
			ChatId:          u.ChatID,
			MessageId:       u.MessageID,
			FromUserId:      u.FromUserID,
			Data:            u.CallbackData,
		}}
	case raftcluster.JournalChatMemberChanged:
		if e.Update == nil {
			return nil
		}
		out.Payload = &botmanagerpb.Update_ChatMemberChanged{ChatMemberChanged: &botmanagerpb.ChatMemberChanged{
			BotId:       e.BotID,
			ChatId:      e.Update.ChatID,
			BotIsMember: raftcluster.ChatMemberStatusIsMember(e.Update.NewChatMemberStatus),
		}}
	case raftcluster.JournalMessageStatusChanged:
		if e.Delivery == nil {
			return nil
		}
		out.Payload = &botmanagerpb.Update_MessageStatusChanged{MessageStatusChanged: &botmanagerpb.MessageStatusChanged{
			IdempotencyKey: e.IdempotencyKey,
			Delivery:       deliveryToProto(*e.Delivery),
		}}
	case raftcluster.JournalBotStateChanged:
		if e.BotState == nil {
			return nil
		}
		out.Payload = &botmanagerpb.Update_BotStateChanged{BotStateChanged: &botmanagerpb.BotStateChanged{
			BotId:        e.BotID,
			State:        botStateToProto(e.BotState.State),
			FailureClass: failureClassToProto(e.BotState.FailureClass),
			Reason:       e.BotState.Reason,
		}}
	default:
		return nil
	}
	return out
}
