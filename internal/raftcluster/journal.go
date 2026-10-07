package raftcluster

import (
	"fmt"
	"time"
)

// DefaultJournalRetention is the default number of most-recent journal
// entries kept (config `raft.journal_retention`). The journal is
// replicated like the rest of the state and lives in memory on every node,
// so it is bounded by entry count, the same way message history is.
const DefaultJournalRetention = 10000

// JournalKind is the kind of one journal entry — one-to-one with the
// Update kinds of Messaging.Subscribe.
type JournalKind string

const (
	JournalIncomingMessage      JournalKind = "incoming_message"
	JournalCallbackQuery        JournalKind = "callback_query"
	JournalChatMemberChanged    JournalKind = "chat_member_changed"
	JournalMessageStatusChanged JournalKind = "message_status_changed"
	JournalBotStateChanged      JournalKind = "bot_state_changed"
)

// IncomingUpdate is one Telegram update as recorded through Raft by the
// leader's poll loop (RecordUpdatesCommand). Only the fields relevant to
// Kind are set. ChatTitle is a best-effort chat title fetched by the
// caller before Apply for chat_member_changed joins (FSM.Apply never makes
// network calls).
type IncomingUpdate struct {
	Kind       JournalKind `json:"kind"`
	UpdateID   int64       `json:"update_id"`
	ChatID     int64       `json:"chat_id,omitempty"`
	FromUserID int64       `json:"from_user_id,omitempty"`
	MessageID  int64       `json:"message_id,omitempty"`
	Text       string      `json:"text,omitempty"`

	CallbackQueryID string `json:"callback_query_id,omitempty"`
	CallbackData    string `json:"callback_data,omitempty"`

	// NewChatMemberStatus — Telegram's my_chat_member.new_chat_member.status
	// verbatim ("member"/"administrator"/"left"/"kicked"/…).
	NewChatMemberStatus string `json:"new_chat_member_status,omitempty"`
	ChatTitle           string `json:"chat_title,omitempty"`

	ReceivedAt time.Time `json:"received_at"`
}

// BotStateChange is the payload of a bot_state_changed journal entry.
type BotStateChange struct {
	State        BotState     `json:"state"`
	FailureClass FailureClass `json:"failure_class,omitempty"`
	Reason       string       `json:"reason,omitempty"`
}

// JournalEntry is one event in the replicated journal that backs
// Messaging.Subscribe. Seq is assigned inside FSM.Apply from a counter that
// is itself part of the replicated state, so the same event has the same
// Seq on every node — a subscriber can disconnect from one node and resume
// on another with SubscribeRequest.after_sequence.
//
// Entries are immutable once appended: pointer fields are never mutated
// afterwards, so copies returned by ReadJournal may share them.
type JournalEntry struct {
	Seq        uint64      `json:"seq"`
	Kind       JournalKind `json:"kind"`
	BotID      string      `json:"bot_id"`
	OccurredAt time.Time   `json:"occurred_at"`

	// incoming_message / callback_query / chat_member_changed
	Update *IncomingUpdate `json:"update,omitempty"`
	// message_status_changed
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	Delivery       *DeliveryInfo `json:"delivery,omitempty"`
	// bot_state_changed
	BotState *BotStateChange `json:"bot_state,omitempty"`
}

// RecordUpdatesCommand records a batch of Telegram updates received by one
// bot's poll loop, together with the getUpdates offset that acknowledges
// them. Storing the offset in the replicated state is what lets a new
// leader continue polling exactly where the old one stopped: updates are
// acknowledged to Telegram only after they are committed here, so nothing
// is lost on failover, and updates with UpdateID below the stored offset
// are dropped as duplicates.
type RecordUpdatesCommand struct {
	BotID      string           `json:"bot_id"`
	NextOffset int64            `json:"next_offset"`
	Updates    []IncomingUpdate `json:"updates,omitempty"`
}

func (f *FSM) applyRecordUpdates(c *RecordUpdatesCommand) *ApplyResult {
	if c == nil || c.BotID == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: record_updates: missing bot_id", ErrInvalidCommand)}
	}
	if _, ok := f.state.bots[c.BotID]; !ok {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrBotNotFound, c.BotID)}
	}

	offset := f.state.pollOffsets[c.BotID]
	for i := range c.Updates {
		u := c.Updates[i]
		if u.UpdateID < offset {
			continue // уже записано предыдущим лидером — дубль после failover
		}
		if u.Kind == JournalChatMemberChanged {
			f.applyUpdateChatMembership(&UpdateChatMembershipCommand{
				BotID:     c.BotID,
				ChatID:    u.ChatID,
				Title:     u.ChatTitle,
				IsMember:  ChatMemberStatusIsMember(u.NewChatMemberStatus),
				ChangedAt: u.ReceivedAt,
			})
		}
		f.appendJournal(JournalEntry{
			Kind:       u.Kind,
			BotID:      c.BotID,
			OccurredAt: u.ReceivedAt,
			Update:     &u,
		})
	}
	if c.NextOffset > offset {
		f.state.pollOffsets[c.BotID] = c.NextOffset
	}
	return &ApplyResult{}
}

// ChatMemberStatusIsMember reads Telegram's my_chat_member.new_chat_member
// .status: "left"/"kicked" mean the bot is no longer a member, any other
// non-empty status means it is.
func ChatMemberStatusIsMember(status string) bool {
	switch status {
	case "left", "kicked":
		return false
	default:
		return status != ""
	}
}

// appendJournal assigns the next sequence number and appends e, trimming
// the oldest entries beyond journalRetention. Must be called with f.mu
// held, from inside Apply.
func (f *FSM) appendJournal(e JournalEntry) {
	f.state.nextSeq++
	e.Seq = f.state.nextSeq
	f.state.journal = append(f.state.journal, e)

	if excess := len(f.state.journal) - f.journalRetention; excess > 0 {
		remaining := make([]JournalEntry, len(f.state.journal)-excess)
		copy(remaining, f.state.journal[excess:])
		f.state.journal = remaining
	}
}

// journalForCommand appends the journal entry (if any) describing the
// effect of a successfully applied command. created reports whether a
// PutMessage actually created a message (an idempotent repeat changes
// nothing and is not journaled). Must be called with f.mu held.
func (f *FSM) journalForCommand(cmd Command, result *ApplyResult, created bool, at time.Time) {
	switch cmd.Type {
	case CommandPutMessage:
		if !created || result.Message == nil {
			return
		}
	case CommandUpdateDelivery, CommandCancelPending:
		if result.Message == nil {
			return
		}
	case CommandSetBotState, CommandDeleteBot:
		if result.Bot == nil {
			return
		}
		b := result.Bot
		f.appendJournal(JournalEntry{
			Kind:       JournalBotStateChanged,
			BotID:      b.ID,
			OccurredAt: at,
			BotState:   &BotStateChange{State: b.State, FailureClass: b.LastFailureClass, Reason: b.LastFailureReason},
		})
		return
	default:
		return
	}

	m := result.Message
	delivery := m.Delivery
	f.appendJournal(JournalEntry{
		Kind:           JournalMessageStatusChanged,
		BotID:          m.BotID,
		OccurredAt:     at,
		IdempotencyKey: m.IdempotencyKey,
		Delivery:       &delivery,
	})
}

// ReadJournal returns up to limit entries with Seq > after, oldest first.
// oldest is the Seq of the oldest entry still retained (0 if the journal is
// empty); a caller asking to resume after a Seq older than oldest-1 has
// lost events to retention. latest is the Seq of the newest entry ever
// appended.
func (f *FSM) ReadJournal(after uint64, limit int) (entries []JournalEntry, oldest, latest uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	j := f.state.journal
	latest = f.state.nextSeq
	if len(j) == 0 {
		return nil, 0, latest
	}
	oldest = j[0].Seq
	if after >= latest {
		return nil, oldest, latest
	}

	// Seq непрерывны внутри журнала, поэтому позицию можно вычислить, а не
	// искать.
	start := 0
	if after >= oldest {
		start = int(after - oldest + 1)
	}
	if start >= len(j) {
		return nil, oldest, latest
	}
	end := len(j)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	entries = make([]JournalEntry, end-start)
	copy(entries, j[start:end])
	return entries, oldest, latest
}

// PollOffset returns the getUpdates offset recorded for botID (0 if none).
func (f *FSM) PollOffset(botID string) int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.pollOffsets[botID]
}
