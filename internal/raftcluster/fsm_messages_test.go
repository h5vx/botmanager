package raftcluster

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

// TestRetention_KeepsOnlyMostRecentN inserts more than N messages for one
// bot and checks exactly N remain — the newest ones, oldest-first order
// preserved, and that trimmed messages are no longer reachable by key.
func TestRetention_KeepsOnlyMostRecentN(t *testing.T) {
	const retention = 5
	fsm := NewFSM(retention)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	const total = 12
	for i := 0; i < total; i++ {
		mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
			IdempotencyKey: fmt.Sprintf("k%02d", i), BotID: "bot-1", ChatID: 1,
			Text: fmt.Sprintf("msg %d", i), CreatedAt: t1(i),
		}})
	}

	msgs, _, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1"}, 0, "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != retention {
		t.Fatalf("len(msgs) = %d, want %d", len(msgs), retention)
	}

	// the surviving messages must be the most recent `retention`, in
	// ascending (oldest-first) order.
	for i, m := range msgs {
		wantIdx := total - retention + i
		wantKey := fmt.Sprintf("k%02d", wantIdx)
		if m.IdempotencyKey != wantKey {
			t.Errorf("msgs[%d].IdempotencyKey = %q, want %q", i, m.IdempotencyKey, wantKey)
		}
	}

	// an evicted key is gone, not just filtered out of ListMessages.
	if _, ok := fsm.GetMessage("k00"); ok {
		t.Errorf("GetMessage(k00) found an evicted message")
	}
	if _, ok := fsm.GetMessage("k11"); !ok {
		t.Errorf("GetMessage(k11) missing the most recent message")
	}
}

func TestListMessages_FiltersAndPaginates(t *testing.T) {
	fsm := NewFSM(100)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	for i := 0; i < 5; i++ {
		status := DeliveryStatusPending
		if i%2 == 0 {
			status = DeliveryStatusSent
		}
		mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
			IdempotencyKey: fmt.Sprintf("k%d", i), BotID: "bot-1", ChatID: int64(i % 2), CreatedAt: t1(i),
		}})
		if status == DeliveryStatusSent {
			mustApply(t, fsm, Command{Type: CommandUpdateDelivery, UpdateDelivery: &UpdateDeliveryCommand{
				IdempotencyKey: fmt.Sprintf("k%d", i), Status: DeliveryStatusSent, SentAt: t1(i + 100),
			}})
		}
	}

	page1, next, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1"}, 2, "")
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 2 || next == "" {
		t.Fatalf("page1 = %+v, next = %q", page1, next)
	}
	page2, next2, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1"}, 2, next)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 2 || next2 == "" {
		t.Fatalf("page2 = %+v, next2 = %q", page2, next2)
	}
	page3, next3, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1"}, 2, next2)
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	if len(page3) != 1 || next3 != "" {
		t.Fatalf("page3 = %+v, next3 = %q", page3, next3)
	}

	sent, _, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1", Status: DeliveryStatusSent}, 0, "")
	if err != nil {
		t.Fatalf("sent filter: %v", err)
	}
	if len(sent) != 3 {
		t.Fatalf("len(sent) = %d, want 3", len(sent))
	}

	byChat, _, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1", ChatID: 1}, 0, "")
	if err != nil {
		t.Fatalf("chat filter: %v", err)
	}
	if len(byChat) != 2 {
		t.Fatalf("len(byChat) = %d, want 2", len(byChat))
	}
}

// TestListMessages_PeriodFilter checks CreatedFrom/CreatedTo narrow the
// result to the half-open interval [CreatedFrom, CreatedTo) — time
// intervals are half-open everywhere, including this filter
// (ListMessagesRequest.created_from/created_to). Five messages at t1(0)..t1(4).
func TestListMessages_PeriodFilter(t *testing.T) {
	fsm := NewFSM(100)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})
	for i := 0; i < 5; i++ {
		mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
			IdempotencyKey: fmt.Sprintf("k%d", i), BotID: "bot-1", ChatID: 1, CreatedAt: t1(i),
		}})
	}

	// [t1(1), t1(3)) — includes k1, k2; excludes k0 (before), k3 (the
	// exclusive upper bound itself), k4.
	mid, _, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1", CreatedFrom: t1(1), CreatedTo: t1(3)}, 0, "")
	if err != nil {
		t.Fatalf("mid: %v", err)
	}
	if len(mid) != 2 || mid[0].IdempotencyKey != "k1" || mid[1].IdempotencyKey != "k2" {
		t.Fatalf("mid = %+v, want [k1 k2]", mid)
	}

	// lower bound alone.
	fromOnly, _, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1", CreatedFrom: t1(3)}, 0, "")
	if err != nil {
		t.Fatalf("fromOnly: %v", err)
	}
	if len(fromOnly) != 2 || fromOnly[0].IdempotencyKey != "k3" || fromOnly[1].IdempotencyKey != "k4" {
		t.Fatalf("fromOnly = %+v, want [k3 k4]", fromOnly)
	}

	// upper bound alone, exclusive: t1(3) itself is excluded.
	toOnly, _, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1", CreatedTo: t1(3)}, 0, "")
	if err != nil {
		t.Fatalf("toOnly: %v", err)
	}
	if len(toOnly) != 3 || toOnly[2].IdempotencyKey != "k2" {
		t.Fatalf("toOnly = %+v, want [k0 k1 k2]", toOnly)
	}

	// a period matching nothing returns an empty (not nil-panicking) page
	// with no next token.
	empty, nextEmpty, err := fsm.ListMessages(
		ListMessagesFilter{BotID: "bot-1", CreatedFrom: t1(10), CreatedTo: t1(20)}, 0, "",
	)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(empty) != 0 || nextEmpty != "" {
		t.Fatalf("empty = %+v, next = %q, want empty page", empty, nextEmpty)
	}
}

// TestPutMessage_ButtonsSurviveApply checks a message carrying inline
// buttons is recorded and readable back through GetMessage/ListMessages —
// the FSM path behind Messaging.Send inline-button support.
func TestPutMessage_ButtonsSurviveApply(t *testing.T) {
	fsm := NewFSM(100)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	buttons := []Button{{Text: "Открыть", URL: "https://example.org/app"}}
	mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "k0", BotID: "bot-1", ChatID: 1, Text: "hi", Buttons: buttons, CreatedAt: t1(1),
	}})

	got, ok := fsm.GetMessage("k0")
	if !ok {
		t.Fatalf("GetMessage(k0) not found")
	}
	if len(got.Buttons) != 1 || got.Buttons[0] != buttons[0] {
		t.Errorf("GetMessage(k0).Buttons = %+v, want %+v", got.Buttons, buttons)
	}

	msgs, _, err := fsm.ListMessages(ListMessagesFilter{BotID: "bot-1"}, 0, "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(msgs) != 1 || len(msgs[0].Buttons) != 1 || msgs[0].Buttons[0] != buttons[0] {
		t.Errorf("ListMessages result buttons = %+v, want %+v", msgs, buttons)
	}
}

// TestPutMessage_CallbackButtonSurvivesApplyAndSnapshot checks a
// callback_data button is
// recorded through Apply, readable back through GetMessage, and survives a
// snapshot/restore roundtrip — the same guarantee TestPutMessage_
// ButtonsSurviveApply and TestSnapshotRestore_Roundtrip already give URL
// buttons, exercised here for the new field.
func TestPutMessage_CallbackButtonSurvivesApplyAndSnapshot(t *testing.T) {
	fsm := NewFSM(100)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	buttons := []Button{{Text: "Буду", CallbackData: "avail:yes"}}
	mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "k0", BotID: "bot-1", ChatID: 1, Text: "hi", Buttons: buttons, CreatedAt: t1(1),
	}})

	got, ok := fsm.GetMessage("k0")
	if !ok {
		t.Fatalf("GetMessage(k0) not found")
	}
	if len(got.Buttons) != 1 || got.Buttons[0] != buttons[0] {
		t.Fatalf("GetMessage(k0).Buttons = %+v, want %+v", got.Buttons, buttons)
	}

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &fakeSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release()

	restored := NewFSM(100)
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	after, ok := restored.GetMessage("k0")
	if !ok {
		t.Fatalf("GetMessage(k0) not found after restore")
	}
	if len(after.Buttons) != 1 || after.Buttons[0] != buttons[0] {
		t.Errorf("GetMessage(k0).Buttons after restore = %+v, want %+v", after.Buttons, buttons)
	}
}

// TestMessageClone_ButtonsIndependent proves Message.Clone's slice fix:
// mutating the Buttons slice of a value returned by GetMessage must not be
// visible to the FSM's own state (regression test for the pre-fix
// `return m` value-receiver Clone, which shared the Buttons backing array).
func TestMessageClone_ButtonsIndependent(t *testing.T) {
	fsm := NewFSM(100)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})
	mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "k0", BotID: "bot-1", ChatID: 1, Text: "hi",
		Buttons:   []Button{{Text: "one", URL: "https://example.org/1"}},
		CreatedAt: t1(1),
	}})

	got, ok := fsm.GetMessage("k0")
	if !ok {
		t.Fatalf("GetMessage(k0) not found")
	}
	got.Buttons[0].Text = "mutated"

	again, _ := fsm.GetMessage("k0")
	if again.Buttons[0].Text != "one" {
		t.Errorf("mutating a cloned Message's Buttons leaked into FSM state: got %q, want %q", again.Buttons[0].Text, "one")
	}
}
