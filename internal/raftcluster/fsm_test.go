package raftcluster

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func mustApply(t *testing.T, fsm *FSM, cmd Command) *ApplyResult {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	res, ok := fsm.Apply(&raft.Log{Data: data}).(*ApplyResult)
	if !ok {
		t.Fatalf("Apply returned %T, want *ApplyResult", res)
	}
	return res
}

func t1(offsetSeconds int) time.Time {
	return time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC).Add(time.Duration(offsetSeconds) * time.Second)
}

// TestApplyCreateBot covers CommandCreateBot: fresh creation, duplicate ID
// rejected, missing ID rejected.
func TestApplyCreateBot(t *testing.T) {
	cases := []struct {
		name    string
		seed    []Command // applied before the command under test
		cmd     Command
		wantErr error
		wantBot *Bot
	}{
		{
			name: "creates a disabled bot",
			cmd: Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{
				ID: "bot-1", DisplayName: "Rehearsal bot", Token: "t0ken",
				CreatedAt: t1(0),
			}},
			wantBot: &Bot{
				ID: "bot-1", DisplayName: "Rehearsal bot", Token: "t0ken",
				State: BotStateDisabled, CreatedAt: t1(0), UpdatedAt: t1(0),
			},
		},
		{
			name:    "duplicate id rejected",
			seed:    []Command{{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}}},
			cmd:     Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(1)}},
			wantErr: ErrBotAlreadyExists,
		},
		{
			name:    "missing id rejected",
			cmd:     Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{CreatedAt: t1(0)}},
			wantErr: ErrInvalidCommand,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewFSM(10)
			for _, seed := range tc.seed {
				mustApply(t, fsm, seed)
			}
			res := mustApply(t, fsm, tc.cmd)

			if tc.wantErr != nil {
				if !errors.Is(res.Err, tc.wantErr) {
					t.Fatalf("Err = %v, want %v", res.Err, tc.wantErr)
				}
				return
			}
			if res.Err != nil {
				t.Fatalf("unexpected Err: %v", res.Err)
			}
			if res.Bot == nil {
				t.Fatalf("Bot = nil, want %+v", tc.wantBot)
			}
			if *res.Bot != *tc.wantBot {
				t.Fatalf("Bot = %+v, want %+v", *res.Bot, *tc.wantBot)
			}
		})
	}
}

// TestApplyUpdateBot covers CommandUpdateBot: partial field updates, the
// tri-state proxy semantics (untouched / explicitly cleared / explicitly
// set), and updating a bot that does not exist.
func TestApplyUpdateBot(t *testing.T) {
	newFSMWithBot := func() *FSM {
		fsm := NewFSM(10)
		mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{
			ID: "bot-1", DisplayName: "Original", Token: "orig-token",
			Proxy:     &ProxyConfig{Enabled: true, Address: "socks5h://a:1080"},
			CreatedAt: t1(0),
		}})
		return fsm
	}

	t.Run("updates only touched fields", func(t *testing.T) {
		fsm := newFSMWithBot()
		newName := "Renamed"
		res := mustApply(t, fsm, Command{Type: CommandUpdateBot, UpdateBot: &UpdateBotCommand{
			ID: "bot-1", DisplayName: &newName, UpdatedAt: t1(5),
		}})
		if res.Err != nil {
			t.Fatalf("unexpected Err: %v", res.Err)
		}
		if res.Bot.DisplayName != "Renamed" {
			t.Errorf("DisplayName = %q, want Renamed", res.Bot.DisplayName)
		}
		if res.Bot.Token != "orig-token" {
			t.Errorf("Token = %q, want unchanged orig-token", res.Bot.Token)
		}
		if res.Bot.Proxy == nil || res.Bot.Proxy.Address != "socks5h://a:1080" {
			t.Errorf("Proxy = %+v, want unchanged", res.Bot.Proxy)
		}
	})

	t.Run("explicit proxy clear reverts to node default", func(t *testing.T) {
		fsm := newFSMWithBot()
		res := mustApply(t, fsm, Command{Type: CommandUpdateBot, UpdateBot: &UpdateBotCommand{
			ID: "bot-1", ProxySet: true, ProxyValue: nil, UpdatedAt: t1(5),
		}})
		if res.Err != nil {
			t.Fatalf("unexpected Err: %v", res.Err)
		}
		if res.Bot.Proxy != nil {
			t.Errorf("Proxy = %+v, want nil (node default)", res.Bot.Proxy)
		}
	})

	t.Run("explicit proxy disable is distinct from clearing", func(t *testing.T) {
		fsm := newFSMWithBot()
		res := mustApply(t, fsm, Command{Type: CommandUpdateBot, UpdateBot: &UpdateBotCommand{
			ID: "bot-1", ProxySet: true, ProxyValue: &ProxyConfig{Enabled: false}, UpdatedAt: t1(5),
		}})
		if res.Err != nil {
			t.Fatalf("unexpected Err: %v", res.Err)
		}
		if res.Bot.Proxy == nil || res.Bot.Proxy.Enabled {
			t.Errorf("Proxy = %+v, want explicit {Enabled:false}", res.Bot.Proxy)
		}
	})

	t.Run("unknown bot rejected", func(t *testing.T) {
		fsm := NewFSM(10)
		res := mustApply(t, fsm, Command{Type: CommandUpdateBot, UpdateBot: &UpdateBotCommand{ID: "missing", UpdatedAt: t1(0)}})
		if !errors.Is(res.Err, ErrBotNotFound) {
			t.Fatalf("Err = %v, want ErrBotNotFound", res.Err)
		}
	})
}

// TestApplySetBotState covers the lifecycle transitions and the
// terminal nature of "deleted".
func TestApplySetBotState(t *testing.T) {
	fsm := NewFSM(10)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	res := mustApply(t, fsm, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{
		ID: "bot-1", State: BotStateEnabled, UpdatedAt: t1(1),
	}})
	if res.Err != nil || res.Bot.State != BotStateEnabled {
		t.Fatalf("enable: res = %+v", res)
	}

	res = mustApply(t, fsm, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{
		ID: "bot-1", State: BotStateBroken, FailureClass: FailureClassBot, FailureReason: "401 unauthorized", UpdatedAt: t1(2),
	}})
	if res.Err != nil || res.Bot.State != BotStateBroken {
		t.Fatalf("break: res = %+v", res)
	}
	if res.Bot.LastFailureClass != FailureClassBot || res.Bot.LastFailureReason != "401 unauthorized" {
		t.Fatalf("failure info not recorded: %+v", res.Bot)
	}

	// re-enabling clears the failure info
	res = mustApply(t, fsm, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{
		ID: "bot-1", State: BotStateEnabled, UpdatedAt: t1(3),
	}})
	if res.Err != nil || res.Bot.LastFailureClass != FailureClassUnspecified || res.Bot.LastFailureReason != "" {
		t.Fatalf("failure info not cleared on re-enable: %+v", res.Bot)
	}

	mustApply(t, fsm, Command{Type: CommandDeleteBot, DeleteBot: &DeleteBotCommand{ID: "bot-1", UpdatedAt: t1(4)}})

	res = mustApply(t, fsm, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{
		ID: "bot-1", State: BotStateEnabled, UpdatedAt: t1(5),
	}})
	if !errors.Is(res.Err, ErrInvalidCommand) {
		t.Fatalf("resurrecting a deleted bot: Err = %v, want ErrInvalidCommand", res.Err)
	}
}

// TestApplyDeleteBot covers soft delete: history stays, and deleting
// twice is an idempotent no-op rather than an error.
func TestApplyDeleteBot(t *testing.T) {
	fsm := NewFSM(10)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})
	mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "k1", BotID: "bot-1", ChatID: 1, Text: "hi", CreatedAt: t1(1),
	}})

	res := mustApply(t, fsm, Command{Type: CommandDeleteBot, DeleteBot: &DeleteBotCommand{ID: "bot-1", UpdatedAt: t1(2)}})
	if res.Err != nil || res.Bot.State != BotStateDeleted {
		t.Fatalf("delete: res = %+v", res)
	}

	// idempotent repeat
	res = mustApply(t, fsm, Command{Type: CommandDeleteBot, DeleteBot: &DeleteBotCommand{ID: "bot-1", UpdatedAt: t1(3)}})
	if res.Err != nil || res.Bot.State != BotStateDeleted {
		t.Fatalf("repeat delete: res = %+v", res)
	}

	// message history is preserved
	msg, ok := fsm.GetMessage("k1")
	if !ok {
		t.Fatalf("message history lost after delete")
	}
	if msg.BotID != "bot-1" {
		t.Fatalf("msg.BotID = %q, want bot-1", msg.BotID)
	}

	res = mustApply(t, fsm, Command{Type: CommandDeleteBot, DeleteBot: &DeleteBotCommand{ID: "missing", UpdatedAt: t1(4)}})
	if !errors.Is(res.Err, ErrBotNotFound) {
		t.Fatalf("Err = %v, want ErrBotNotFound", res.Err)
	}
}

// TestApplyPutMessage_Idempotent verifies idempotency: repeating a PutMessage
// command with the same idempotency_key does not create a second message.
func TestApplyPutMessage_Idempotent(t *testing.T) {
	fsm := NewFSM(10)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	first := mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "notify:1:2:3:1", BotID: "bot-1", ChatID: 42, Text: "rehearsal moved", CreatedAt: t1(1),
	}})
	if first.Err != nil {
		t.Fatalf("first put: %v", first.Err)
	}

	second := mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "notify:1:2:3:1", BotID: "bot-1", ChatID: 42, Text: "rehearsal moved AGAIN?!", CreatedAt: t1(2),
	}})
	if second.Err != nil {
		t.Fatalf("second put: %v", second.Err)
	}

	if second.Message.Text != first.Message.Text {
		t.Fatalf("second put changed text: got %q, want unchanged %q", second.Message.Text, first.Message.Text)
	}
	if !second.Message.CreatedAt.Equal(first.Message.CreatedAt) {
		t.Fatalf("second put changed CreatedAt: got %v, want %v", second.Message.CreatedAt, first.Message.CreatedAt)
	}

	msgs, _, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1"}, 0, "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1 (no duplicate created)", len(msgs))
	}
}

func TestApplyPutMessage_UnknownBotRejected(t *testing.T) {
	fsm := NewFSM(10)
	res := mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "k1", BotID: "missing", ChatID: 1, CreatedAt: t1(0),
	}})
	if !errors.Is(res.Err, ErrBotNotFound) {
		t.Fatalf("Err = %v, want ErrBotNotFound", res.Err)
	}
}

// TestApplyUpdateDelivery covers the delivery-status fields.
func TestApplyUpdateDelivery(t *testing.T) {
	fsm := NewFSM(10)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})
	mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "k1", BotID: "bot-1", ChatID: 1, Text: "hi", CreatedAt: t1(1),
	}})

	res := mustApply(t, fsm, Command{Type: CommandUpdateDelivery, UpdateDelivery: &UpdateDeliveryCommand{
		IdempotencyKey: "k1", Status: DeliveryStatusRetrying, Retries: 1, LastError: "timeout", NextRetryAt: t1(10),
	}})
	if res.Err != nil {
		t.Fatalf("update 1: %v", res.Err)
	}
	if res.Message.Delivery.Status != DeliveryStatusRetrying || res.Message.Delivery.Retries != 1 {
		t.Fatalf("delivery = %+v", res.Message.Delivery)
	}

	res = mustApply(t, fsm, Command{Type: CommandUpdateDelivery, UpdateDelivery: &UpdateDeliveryCommand{
		IdempotencyKey: "k1", Status: DeliveryStatusSent, Retries: 1, SentAt: t1(20), MessageID: 999,
	}})
	if res.Err != nil {
		t.Fatalf("update 2: %v", res.Err)
	}
	if res.Message.Delivery.Status != DeliveryStatusSent || res.Message.MessageID != 999 {
		t.Fatalf("final message = %+v", res.Message)
	}

	res = mustApply(t, fsm, Command{Type: CommandUpdateDelivery, UpdateDelivery: &UpdateDeliveryCommand{
		IdempotencyKey: "does-not-exist", Status: DeliveryStatusFailed,
	}})
	if !errors.Is(res.Err, ErrMessageNotFound) {
		t.Fatalf("Err = %v, want ErrMessageNotFound", res.Err)
	}
}

func TestApply_UnknownCommandType(t *testing.T) {
	fsm := NewFSM(10)
	res, ok := fsm.Apply(&raft.Log{Data: []byte(`{"type":"not_a_real_command"}`)}).(*ApplyResult)
	if !ok {
		t.Fatalf("Apply returned %T", res)
	}
	if !errors.Is(res.Err, ErrInvalidCommand) {
		t.Fatalf("Err = %v, want ErrInvalidCommand", res.Err)
	}
}

func TestApply_MalformedData(t *testing.T) {
	fsm := NewFSM(10)
	res, ok := fsm.Apply(&raft.Log{Data: []byte(`not json`)}).(*ApplyResult)
	if !ok {
		t.Fatalf("Apply returned %T", res)
	}
	if !errors.Is(res.Err, ErrInvalidCommand) {
		t.Fatalf("Err = %v, want ErrInvalidCommand", res.Err)
	}
}
