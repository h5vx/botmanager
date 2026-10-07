package telegram

import (
	"sync"
	"time"
)

// UpdateKind is the subset of Messaging.Subscribe's Update kinds produced directly by this
// package's long polling: incoming_message, callback_query,
// chat_member_changed. The other two documented kinds —
// message_status_changed (from a DeliveryInfo transition) and
// bot_state_changed (from a SetBotState transition) — originate from
// raftcluster's own command stream (observable via
// ClusterView.SubscribeApplied), not from Telegram updates, so producing
// them is not this package's job; combining all five into one
// Messaging.Subscribe stream is internal/rpcserver's job.
type UpdateKind string

const (
	UpdateKindIncomingMessage   UpdateKind = "incoming_message"
	UpdateKindCallbackQuery     UpdateKind = "callback_query"
	UpdateKindChatMemberChanged UpdateKind = "chat_member_changed"
)

// Update is one event this package publishes to an UpdateBus. It carries a
// single shape for all three UpdateKinds, with only the fields relevant to
// that kind populated (see the field comments). This mirrors
// botmanagerpb.Update; converting between the two is internal/rpcserver's
// job, not this package's.
//
// Logs must never include Text/CallbackData, only identifiers
// (BotID/ChatID/FromUserID/MessageID) and, if useful, a length — see
// pollLoop, which never logs an Update's contents. Text/CallbackData
// themselves are delivered onward (to Messaging.Subscribe) unredacted — the restriction is on what this service
// writes to its own logs, not on what it hands to its caller.
type Update struct {
	Kind       UpdateKind
	BotID      string
	UpdateID   int64
	ChatID     int64
	FromUserID int64

	// MessageID — id of the incoming message (incoming_message), or of the
	// message the pressed inline button is attached to (callback_query).
	MessageID int64
	// Text — the incoming message's text (incoming_message only).
	Text string

	// CallbackQueryID/CallbackData — callback_query only.
	CallbackQueryID string
	CallbackData    string

	// NewChatMemberStatus — the bot's new status in the chat
	// (chat_member_changed only): Telegram's my_chat_member.new_chat_member
	// .status verbatim ("member"/"administrator"/"left"/"kicked"/…).
	NewChatMemberStatus string

	ReceivedAt time.Time
}

// UpdateBus fans out Update values from per-bot Runners to any number of
// subscribers. This is the extension point gRPC
// Messaging.Subscribe attaches to (see package doc "Extension point").
type UpdateBus interface {
	Publish(u Update)
	// Subscribe returns a channel receiving every Update published after
	// the call, and a cancel function to unsubscribe.
	Subscribe() (<-chan Update, func())
}

// subscriberBufSize bounds how far behind a subscriber may fall before
// InMemoryBus starts dropping updates for it instead of blocking the
// publisher — a stalled downstream consumer must never stall long polling
// for a bot. Delivery is already "at least once" with dedup by
// update_id downstream, so an occasional drop under sustained backpressure
// stays within that documented contract rather than weakening it further.
const subscriberBufSize = 256

// InMemoryBus is the only UpdateBus implementation this package needs: a
// simple, non-persistent fan-out. It holds nothing durable — the durable
// record for outgoing messages is raftcluster's own message log; for
// incoming updates, whatever the caller chooses to persist. This
// bus only serves currently-connected subscribers.
type InMemoryBus struct {
	mu   sync.Mutex
	subs map[chan Update]struct{}
}

func NewInMemoryBus() *InMemoryBus {
	return &InMemoryBus{subs: make(map[chan Update]struct{})}
}

func (b *InMemoryBus) Publish(u Update) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- u:
		default:
			// Подписчик отстал — теряем это обновление для него, но не
			// блокируем ни остальных подписчиков, ни публикующую (long
			// polling) горутину.
		}
	}
}

func (b *InMemoryBus) Subscribe() (<-chan Update, func()) {
	ch := make(chan Update, subscriberBufSize)

	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}
	return ch, cancel
}

var _ UpdateBus = (*InMemoryBus)(nil)
