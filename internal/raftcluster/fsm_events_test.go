package raftcluster

import (
	"errors"
	"testing"
	"time"
)

// recvEvent reads one Event off ch, failing the test if none arrives
// promptly — publishEvent is synchronous within Apply (buffered channel),
// so a successful Apply's event is always already queued by the time
// mustApply returns.
func recvEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(time.Second):
		t.Fatalf("no event received within 1s")
		return Event{}
	}
}

func assertNoEvent(t *testing.T, ch <-chan Event) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("unexpected event: %+v", ev)
	case <-time.After(50 * time.Millisecond):
		// ничего не пришло — ожидаемо
	}
}

// TestSubscribeEvents_OneEventPerSuccessfulCommand covers "Event describes
// what changed" (fsm.go): every command type that mutates state produces
// exactly one Event carrying the right Command/Bot/Message, for a
// subscriber registered via SubscribeEvents.
func TestSubscribeEvents_OneEventPerSuccessfulCommand(t *testing.T) {
	fsm := NewFSM(10)
	ch, cancel := fsm.SubscribeEvents()
	defer cancel()

	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", DisplayName: "B1", CreatedAt: t1(0)}})
	ev := recvEvent(t, ch)
	if ev.Command != CommandCreateBot || ev.Bot == nil || ev.Bot.ID != "bot-1" || ev.Message != nil {
		t.Fatalf("create_bot event = %+v", ev)
	}

	newName := "B1 renamed"
	mustApply(t, fsm, Command{Type: CommandUpdateBot, UpdateBot: &UpdateBotCommand{ID: "bot-1", DisplayName: &newName, UpdatedAt: t1(1)}})
	ev = recvEvent(t, ch)
	if ev.Command != CommandUpdateBot || ev.Bot == nil || ev.Bot.DisplayName != newName {
		t.Fatalf("update_bot event = %+v", ev)
	}

	mustApply(t, fsm, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{ID: "bot-1", State: BotStateEnabled, UpdatedAt: t1(2)}})
	ev = recvEvent(t, ch)
	if ev.Command != CommandSetBotState || ev.Bot == nil || ev.Bot.State != BotStateEnabled {
		t.Fatalf("set_bot_state event = %+v", ev)
	}

	mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "k1", BotID: "bot-1", ChatID: 1, Text: "hi", CreatedAt: t1(3),
	}})
	ev = recvEvent(t, ch)
	if ev.Command != CommandPutMessage || ev.Message == nil || ev.Message.IdempotencyKey != "k1" || ev.Bot != nil {
		t.Fatalf("put_message event = %+v", ev)
	}

	mustApply(t, fsm, Command{Type: CommandUpdateDelivery, UpdateDelivery: &UpdateDeliveryCommand{
		IdempotencyKey: "k1", Status: DeliveryStatusSent, SentAt: t1(4),
	}})
	ev = recvEvent(t, ch)
	if ev.Command != CommandUpdateDelivery || ev.Message == nil || ev.Message.Delivery.Status != DeliveryStatusSent {
		t.Fatalf("update_delivery event = %+v", ev)
	}

	mustApply(t, fsm, Command{Type: CommandCancelPending, CancelPending: &CancelPendingCommand{
		IdempotencyKey: "k1", CancelledAt: t1(5),
	}})
	ev = recvEvent(t, ch)
	// k1 is already SENT (terminal in the other direction) — cancel is a
	// structural no-op but still a *successful* Apply, so it still
	// publishes an event (see fsm.go: any non-Err result with Bot/Message
	// set publishes).
	if ev.Command != CommandCancelPending || ev.Message == nil || ev.Message.IdempotencyKey != "k1" {
		t.Fatalf("cancel_pending event = %+v", ev)
	}

	mustApply(t, fsm, Command{Type: CommandDeleteBot, DeleteBot: &DeleteBotCommand{ID: "bot-1", UpdatedAt: t1(6)}})
	ev = recvEvent(t, ch)
	if ev.Command != CommandDeleteBot || ev.Bot == nil || ev.Bot.State != BotStateDeleted {
		t.Fatalf("delete_bot event = %+v", ev)
	}
}

// TestSubscribeEvents_NoEventOnRejectedApply covers the other half of
// fsm.go's "Rejected commands (ApplyResult.Err != nil) never produce an
// Event": a command that fails validation/lookup must not show up on
// SubscribeEvents at all.
func TestSubscribeEvents_NoEventOnRejectedApply(t *testing.T) {
	fsm := NewFSM(10)
	ch, cancel := fsm.SubscribeEvents()
	defer cancel()

	res := mustApply(t, fsm, Command{Type: CommandUpdateBot, UpdateBot: &UpdateBotCommand{ID: "missing", UpdatedAt: t1(0)}})
	if !errors.Is(res.Err, ErrBotNotFound) {
		t.Fatalf("Err = %v, want ErrBotNotFound", res.Err)
	}
	assertNoEvent(t, ch)

	res = mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "k1", BotID: "missing-bot", CreatedAt: t1(1),
	}})
	if !errors.Is(res.Err, ErrBotNotFound) {
		t.Fatalf("Err = %v, want ErrBotNotFound", res.Err)
	}
	assertNoEvent(t, ch)

	res = mustApply(t, fsm, Command{Type: CommandCancelPending, CancelPending: &CancelPendingCommand{
		IdempotencyKey: "does-not-exist", CancelledAt: t1(2),
	}})
	if !errors.Is(res.Err, ErrMessageNotFound) {
		t.Fatalf("Err = %v, want ErrMessageNotFound", res.Err)
	}
	assertNoEvent(t, ch)
}

// TestSubscribeEvents_UnknownCommandTypeProducesNoEvent rounds out the
// rejected-Apply coverage with the "unrecognized Command.Type" path (a
// malformed/forward-incompatible log entry), distinct from a structurally
// valid command referencing something that does not exist.
func TestSubscribeEvents_UnknownCommandTypeProducesNoEvent(t *testing.T) {
	fsm := NewFSM(10)
	ch, cancel := fsm.SubscribeEvents()
	defer cancel()

	res := mustApply(t, fsm, Command{Type: "not_a_real_command"})
	if !errors.Is(res.Err, ErrInvalidCommand) {
		t.Fatalf("Err = %v, want ErrInvalidCommand", res.Err)
	}
	assertNoEvent(t, ch)
}
