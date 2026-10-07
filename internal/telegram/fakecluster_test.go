package telegram

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// fakeCluster is a minimal in-memory stand-in for *raftcluster.Node,
// implementing exactly the telegram.ClusterView methods Runner needs — the
// same pattern botlifecycle/manager_test.go uses for its fakeCluster. It is
// not a general-purpose FSM re-implementation: only CommandUpdateDelivery
// and CommandSetBotState are supported (the only two Command types a
// Runner ever applies), matching real Node.Apply's convention of returning
// (nil, err) for a rejected command.
type fakeCluster struct {
	mu       sync.Mutex
	messages map[string]*raftcluster.Message
	bots     map[string]*raftcluster.Bot
	chats    []raftcluster.UpdateChatMembershipCommand // applied commands, in order — recordChatMembership assertions

	notifyMu sync.Mutex
	subs     map[chan struct{}]struct{}
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		messages: make(map[string]*raftcluster.Message),
		bots:     make(map[string]*raftcluster.Bot),
		subs:     make(map[chan struct{}]struct{}),
	}
}

func (f *fakeCluster) appliedChatMemberships() []raftcluster.UpdateChatMembershipCommand {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]raftcluster.UpdateChatMembershipCommand, len(f.chats))
	copy(out, f.chats)
	return out
}

func (f *fakeCluster) addBot(bot raftcluster.Bot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := bot
	f.bots[cp.ID] = &cp
}

func (f *fakeCluster) putMessage(msg raftcluster.Message) {
	f.mu.Lock()
	cp := msg
	f.messages[cp.IdempotencyKey] = &cp
	f.mu.Unlock()
	f.notify()
}

func (f *fakeCluster) getBot(id string) raftcluster.Bot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.bots[id]
}

func (f *fakeCluster) getMessage(key string) raftcluster.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.messages[key]
}

func (f *fakeCluster) Apply(cmd raftcluster.Command, _ time.Duration) (*raftcluster.ApplyResult, error) {
	f.mu.Lock()
	var result *raftcluster.ApplyResult

	switch cmd.Type {
	case raftcluster.CommandUpdateDelivery:
		c := cmd.UpdateDelivery
		msg, ok := f.messages[c.IdempotencyKey]
		if !ok {
			f.mu.Unlock()
			return nil, raftcluster.ErrMessageNotFound
		}
		msg.Delivery.Status = c.Status
		msg.Delivery.Retries = c.Retries
		msg.Delivery.LastError = c.LastError
		msg.Delivery.NextRetryAt = c.NextRetryAt
		msg.Delivery.SentAt = c.SentAt
		if c.MessageID != 0 {
			msg.MessageID = c.MessageID
		}
		cp := *msg
		result = &raftcluster.ApplyResult{Message: &cp}

	case raftcluster.CommandSetBotState:
		c := cmd.SetBotState
		bot, ok := f.bots[c.ID]
		if !ok {
			f.mu.Unlock()
			return nil, raftcluster.ErrBotNotFound
		}
		bot.State = c.State
		bot.LastFailureClass = c.FailureClass
		bot.LastFailureReason = c.FailureReason
		cp := *bot
		result = &raftcluster.ApplyResult{Bot: &cp}

	case raftcluster.CommandUpdateChatMembership:
		c := *cmd.UpdateChatMembership
		f.chats = append(f.chats, c)
		result = &raftcluster.ApplyResult{Chat: &raftcluster.ChatMembership{
			BotID: c.BotID, ChatID: c.ChatID, Title: c.Title, IsMember: c.IsMember, ChangedAt: c.ChangedAt,
		}}

	default:
		f.mu.Unlock()
		return nil, fmt.Errorf("fakeCluster: unsupported command %s", cmd.Type)
	}

	f.mu.Unlock()
	f.notify()
	return result, nil
}

func (f *fakeCluster) ListMessages(filter raftcluster.ListMessagesFilter, _ int, _ string) ([]raftcluster.Message, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []raftcluster.Message
	for _, m := range f.messages {
		if m.BotID != filter.BotID {
			continue
		}
		if filter.ChatID != 0 && m.ChatID != filter.ChatID {
			continue
		}
		if filter.Status != raftcluster.DeliveryStatusUnspecified && m.Delivery.Status != filter.Status {
			continue
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, "", nil
}

func (f *fakeCluster) SubscribeApplied() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)

	f.notifyMu.Lock()
	f.subs[ch] = struct{}{}
	f.notifyMu.Unlock()

	cancel := func() {
		f.notifyMu.Lock()
		delete(f.subs, ch)
		f.notifyMu.Unlock()
	}
	return ch, cancel
}

func (f *fakeCluster) notify() {
	f.notifyMu.Lock()
	defer f.notifyMu.Unlock()
	for ch := range f.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

var _ ClusterView = (*fakeCluster)(nil)
