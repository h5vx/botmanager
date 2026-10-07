package raftcluster

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

// DefaultMessageRetentionPerBot is the default number of most-recent
// messages kept per bot (see doc.go for the reasoning). Configurable via
// config/config.yaml `raft.message_retention_per_bot`.
const DefaultMessageRetentionPerBot = 500

// ApplyResult is what FSM.Apply returns (as interface{}, per raft.FSM) and
// what raft.ApplyFuture.Response() yields back to Node.Apply's caller. At
// most one of Bot/Message is set, matching whichever command ran; Err is
// set instead when the command was rejected (bad reference, duplicate ID,
// malformed payload — see errors.go). Err is deliberately part of the
// returned value rather than a Go error from Apply itself: raft.FSM.Apply
// has no error return, and a rejected command must still be recorded as
// committed (every replica reached the same conclusion), it just didn't
// change anything.
type ApplyResult struct {
	Bot     *Bot
	Message *Message
	Chat    *ChatMembership
	Err     error
}

// fsmState is the entire replicated state machine. Bots and Messages are
// keyed by ID for O(1) lookup; Messages additionally keeps, for each bot,
// a slice ordered oldest-first (append-only, trimmed from the front by
// retention) so that Snapshot/Restore and ListMessages preserve order
// without re-deriving it. msgIndex is a derived structure (idempotency_key
// -> pointer) kept in sync by every mutation in this package; Restore
// rebuilds it from scratch.
type fsmState struct {
	bots     map[string]*Bot
	messages map[string][]*Message                // botID -> messages, oldest first, len <= retention
	msgIndex map[string]*Message                  // idempotency_key -> message (nil entry = never has one; deletion via delete())
	chats    map[string]map[int64]*ChatMembership // botID -> chatID -> membership registry entry

	journal     []JournalEntry      // oldest first, len <= journalRetention, Seq contiguous
	nextSeq     uint64              // Seq of the newest journal entry ever appended
	pollOffsets map[string]int64    // botID -> getUpdates offset acknowledged through Raft
	nodes       map[string]NodeInfo // nodeID -> addresses (node registry)
}

func newFSMState() fsmState {
	return fsmState{
		bots:        make(map[string]*Bot),
		messages:    make(map[string][]*Message),
		msgIndex:    make(map[string]*Message),
		chats:       make(map[string]map[int64]*ChatMembership),
		pollOffsets: make(map[string]int64),
		nodes:       make(map[string]NodeInfo),
	}
}

// FSM implements raft.FSM (embedded BoltDB storage as the Raft state
// machine, with snapshots). All mutation happens inside Apply,
// called by the raft library once per committed log entry, in log order,
// identically on every replica — that is what makes the state
// deterministic and replicable. Accessor methods (ListBots, GetBot,
// ListMessages) take a read lock and return copies (see Bot.Clone /
// Message.Clone) so callers can never mutate FSM state through a returned
// pointer.
type FSM struct {
	mu               sync.RWMutex
	state            fsmState
	retentionPerBot  int
	journalRetention int

	notifyMu  sync.Mutex
	notifyChs map[chan struct{}]struct{}
}

// NewFSM constructs an empty FSM. retentionPerBot must be positive; a
// non-positive value is replaced with DefaultMessageRetentionPerBot rather
// than rejected, since an FSM must always be constructible (it may be
// built before config validation runs).
func NewFSM(retentionPerBot int) *FSM {
	if retentionPerBot <= 0 {
		retentionPerBot = DefaultMessageRetentionPerBot
	}
	return &FSM{
		state:            newFSMState(),
		retentionPerBot:  retentionPerBot,
		journalRetention: DefaultJournalRetention,
		notifyChs:        make(map[chan struct{}]struct{}),
	}
}

// SetJournalRetention overrides DefaultJournalRetention; n <= 0 is ignored.
// Must be called before the FSM is handed to Raft.
func (f *FSM) SetJournalRetention(n int) {
	if n > 0 {
		f.journalRetention = n
	}
}

// SubscribeApplied returns a channel that receives a value after every
// call to Apply, committed or rejected — "something happened, re-read
// state if you care", not a queue of individual commands. The channel is
// buffered (size 1) with latest-value-wins delivery, same as Node.Subscribe.
// Call the returned cancel function to unsubscribe.
func (f *FSM) SubscribeApplied() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)

	f.notifyMu.Lock()
	f.notifyChs[ch] = struct{}{}
	f.notifyMu.Unlock()

	cancel := func() {
		f.notifyMu.Lock()
		delete(f.notifyChs, ch)
		f.notifyMu.Unlock()
	}
	return ch, cancel
}

func (f *FSM) notifyApplied() {
	f.notifyMu.Lock()
	defer f.notifyMu.Unlock()
	for ch := range f.notifyChs {
		select {
		case ch <- struct{}{}:
		default:
			// уже есть непрочитанное уведомление — этого достаточно.
		}
	}
}

// Apply decodes one Command from log.Data and applies it. See the
// determinism note on Command for why every input that could vary between
// replicas (time, IDs) must already be baked into the log entry.
func (f *FSM) Apply(log *raft.Log) interface{} {
	defer f.notifyApplied()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return &ApplyResult{Err: fmt.Errorf("%w: decode: %v", ErrInvalidCommand, err)}
	}

	f.mu.Lock()
	created := false
	if cmd.Type == CommandPutMessage && cmd.PutMessage != nil {
		_, exists := f.state.msgIndex[cmd.PutMessage.IdempotencyKey]
		created = !exists
	}

	var result *ApplyResult
	switch cmd.Type {
	case CommandCreateBot:
		result = f.applyCreateBot(cmd.CreateBot)
	case CommandUpdateBot:
		result = f.applyUpdateBot(cmd.UpdateBot)
	case CommandSetBotState:
		result = f.applySetBotState(cmd.SetBotState)
	case CommandDeleteBot:
		result = f.applyDeleteBot(cmd.DeleteBot)
	case CommandPutMessage:
		result = f.applyPutMessage(cmd.PutMessage)
	case CommandUpdateDelivery:
		result = f.applyUpdateDelivery(cmd.UpdateDelivery)
	case CommandCancelPending:
		result = f.applyCancelPending(cmd.CancelPending)
	case CommandUpdateChatMembership:
		result = f.applyUpdateChatMembership(cmd.UpdateChatMembership)
	case CommandRecordUpdates:
		result = f.applyRecordUpdates(cmd.RecordUpdates)
	case CommandRegisterNode:
		result = f.applyRegisterNode(cmd.RegisterNode)
	case CommandUnregisterNode:
		result = f.applyUnregisterNode(cmd.UnregisterNode)
	default:
		result = &ApplyResult{Err: fmt.Errorf("%w: unknown type %q", ErrInvalidCommand, cmd.Type)}
	}
	if result.Err == nil {
		// Время события — AppendedAt записи журнала Raft: его выставляет
		// лидер, и оно одинаково на всех репликах (в отличие от time.Now()).
		f.journalForCommand(cmd, result, created, log.AppendedAt.UTC())
	}
	f.mu.Unlock()

	return result
}

func (f *FSM) applyCreateBot(c *CreateBotCommand) *ApplyResult {
	if c == nil || c.ID == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: create_bot: missing id", ErrInvalidCommand)}
	}
	if _, exists := f.state.bots[c.ID]; exists {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrBotAlreadyExists, c.ID)}
	}

	bot := &Bot{
		ID:          c.ID,
		DisplayName: c.DisplayName,
		Token:       c.Token,
		State:       BotStateDisabled, // записан, процесс не запущен, пока явно не включён
		Proxy:       clonedProxy(c.Proxy),
		CreatedAt:   c.CreatedAt,
		UpdatedAt:   c.CreatedAt,
	}
	f.state.bots[c.ID] = bot

	return &ApplyResult{Bot: ptr(bot.Clone())}
}

func (f *FSM) applyUpdateBot(c *UpdateBotCommand) *ApplyResult {
	if c == nil || c.ID == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: update_bot: missing id", ErrInvalidCommand)}
	}
	bot, ok := f.state.bots[c.ID]
	if !ok {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrBotNotFound, c.ID)}
	}

	if c.Token != nil {
		bot.Token = *c.Token
	}
	if c.DisplayName != nil {
		bot.DisplayName = *c.DisplayName
	}
	if c.ProxySet {
		bot.Proxy = clonedProxy(c.ProxyValue)
	}
	bot.UpdatedAt = c.UpdatedAt

	return &ApplyResult{Bot: ptr(bot.Clone())}
}

// applySetBotState handles both explicit SetBotState commands and the
// automatic broken-transition path: the caller decides which
// State to request, this method only enforces that the bot exists and
// that a deleted bot cannot be resurrected through this command (deleted
// is terminal — use of DeleteBot is the only way in and there is no way
// out: "процесс остановлен, история сообщений сохраняется" is a one-way
// transition).
func (f *FSM) applySetBotState(c *SetBotStateCommand) *ApplyResult {
	if c == nil || c.ID == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: set_bot_state: missing id", ErrInvalidCommand)}
	}
	bot, ok := f.state.bots[c.ID]
	if !ok {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrBotNotFound, c.ID)}
	}
	if bot.State == BotStateDeleted {
		return &ApplyResult{Err: fmt.Errorf("%w: bot %s is deleted", ErrInvalidCommand, c.ID)}
	}

	bot.State = c.State
	bot.UpdatedAt = c.UpdatedAt
	if c.State == BotStateBroken {
		bot.LastFailureClass = c.FailureClass
		bot.LastFailureReason = c.FailureReason
	} else {
		bot.LastFailureClass = FailureClassUnspecified
		bot.LastFailureReason = ""
	}

	return &ApplyResult{Bot: ptr(bot.Clone())}
}

func (f *FSM) applyDeleteBot(c *DeleteBotCommand) *ApplyResult {
	if c == nil || c.ID == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: delete_bot: missing id", ErrInvalidCommand)}
	}
	bot, ok := f.state.bots[c.ID]
	if !ok {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrBotNotFound, c.ID)}
	}

	if bot.State != BotStateDeleted {
		bot.State = BotStateDeleted
		bot.UpdatedAt = c.UpdatedAt
	}
	// уже удалён — идемпотентный no-op, но всё равно возвращаем текущее
	// состояние, а не ошибку (повтор команды после потери ack — обычный
	// случай при переизбрании лидера).

	return &ApplyResult{Bot: ptr(bot.Clone())}
}

// ListBots returns a snapshot copy of every bot, ordered by ID for
// deterministic output (map iteration order is not).
func (f *FSM) ListBots() []Bot {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out := make([]Bot, 0, len(f.state.bots))
	for _, b := range f.state.bots {
		out = append(out, b.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// GetBot returns a copy of one bot and whether it exists.
func (f *FSM) GetBot(id string) (Bot, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	b, ok := f.state.bots[id]
	if !ok {
		return Bot{}, false
	}
	return b.Clone(), true
}

func clonedProxy(p *ProxyConfig) *ProxyConfig {
	if p == nil {
		return nil
	}
	cp := *p
	return &cp
}

func ptr[T any](v T) *T { return &v }
