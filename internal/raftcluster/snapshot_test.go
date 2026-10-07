package raftcluster

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
)

// fakeSnapshotSink is the minimal raft.SnapshotSink needed to exercise
// FSM.Snapshot()'s Persist without a real raft.SnapshotStore.
type fakeSnapshotSink struct {
	bytes.Buffer
	cancelled bool
}

func (s *fakeSnapshotSink) ID() string    { return "test-snapshot" }
func (s *fakeSnapshotSink) Cancel() error { s.cancelled = true; return nil }
func (s *fakeSnapshotSink) Close() error  { return nil }

var _ raft.SnapshotSink = (*fakeSnapshotSink)(nil)

// TestSnapshotRestore_Roundtrip builds a non-trivial state (several bots in
// different lifecycle states, several messages with different delivery
// statuses, retention already having trimmed one bot's history) and checks
// that Snapshot -> Persist -> Restore on a fresh FSM reproduces the exact
// same observable state.
func TestSnapshotRestore_Roundtrip(t *testing.T) {
	const retention = 3
	src := NewFSM(retention)

	mustApply(t, src, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{
		ID: "bot-1", DisplayName: "Alpha", Token: "tok-a",
		Proxy: &ProxyConfig{Enabled: true, Address: "socks5h://p:1080"}, CreatedAt: t1(0),
	}})
	mustApply(t, src, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{
		ID: "bot-2", DisplayName: "Beta", Token: "tok-b", CreatedAt: t1(1),
	}})
	mustApply(t, src, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{
		ID: "bot-1", State: BotStateEnabled, UpdatedAt: t1(2),
	}})
	mustApply(t, src, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{
		ID: "bot-2", State: BotStateBroken, FailureClass: FailureClassBot, FailureReason: "401", UpdatedAt: t1(3),
	}})

	// bot-1 gets more messages than retention allows, to prove the trimmed
	// state (not the raw pre-trim history) is what round-trips.
	for i := 0; i < 6; i++ {
		mustApply(t, src, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
			IdempotencyKey: fmt.Sprintf("a%d", i), BotID: "bot-1", ChatID: 10,
			Text: fmt.Sprintf("hello %d", i), Priority: PriorityNormal, CreatedAt: t1(10 + i),
		}})
	}
	mustApply(t, src, Command{Type: CommandUpdateDelivery, UpdateDelivery: &UpdateDeliveryCommand{
		IdempotencyKey: "a5", Status: DeliveryStatusSent, Retries: 0, SentAt: t1(50), MessageID: 4242,
	}})
	mustApply(t, src, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "b0", BotID: "bot-2", ChatID: 20, Text: "beta hi",
		Buttons:  []Button{{Text: "Открыть", URL: "https://example.org/app"}},
		Priority: PriorityCritical, CreatedAt: t1(20),
	}})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &fakeSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release()

	// mutate the source AFTER taking the snapshot to prove Snapshot() took
	// a deep copy rather than referencing live state.
	mustApply(t, src, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "after-snapshot", BotID: "bot-1", ChatID: 10, Text: "should not appear",
		CreatedAt: t1(999),
	}})

	dst := NewFSM(retention)
	if err := dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	wantBots := []Bot{
		{ID: "bot-1", DisplayName: "Alpha", Token: "tok-a", State: BotStateEnabled,
			Proxy: &ProxyConfig{Enabled: true, Address: "socks5h://p:1080"}, CreatedAt: t1(0), UpdatedAt: t1(2)},
		{ID: "bot-2", DisplayName: "Beta", Token: "tok-b", State: BotStateBroken,
			CreatedAt: t1(1), UpdatedAt: t1(3), LastFailureClass: FailureClassBot, LastFailureReason: "401"},
	}
	gotBots := dst.ListBots()
	if !reflect.DeepEqual(gotBots, wantBots) {
		t.Fatalf("bots after restore =\n%+v\nwant\n%+v", gotBots, wantBots)
	}

	gotMsgs, _, err := dst.ListMessages(ListMessagesFilter{BotID: "bot-1"}, 0, "")
	if err != nil {
		t.Fatalf("ListMessages bot-1: %v", err)
	}
	if len(gotMsgs) != retention {
		t.Fatalf("len(bot-1 messages) = %d, want %d (retention should have already trimmed before snapshot)", len(gotMsgs), retention)
	}
	for i, m := range gotMsgs {
		wantKey := fmt.Sprintf("a%d", 3+i) // a0..a2 trimmed, a3..a5 remain
		if m.IdempotencyKey != wantKey {
			t.Errorf("bot-1 messages[%d].IdempotencyKey = %q, want %q", i, m.IdempotencyKey, wantKey)
		}
	}
	if gotMsgs[2].Delivery.Status != DeliveryStatusSent || gotMsgs[2].MessageID != 4242 {
		t.Errorf("a5 delivery info not preserved: %+v", gotMsgs[2])
	}

	beta, _, err := dst.ListMessages(ListMessagesFilter{BotID: "bot-2"}, 0, "")
	if err != nil {
		t.Fatalf("ListMessages bot-2: %v", err)
	}
	if len(beta) != 1 || beta[0].IdempotencyKey != "b0" || beta[0].Priority != PriorityCritical {
		t.Fatalf("bot-2 messages = %+v", beta)
	}
	wantButtons := []Button{{Text: "Открыть", URL: "https://example.org/app"}}
	if !reflect.DeepEqual(beta[0].Buttons, wantButtons) {
		t.Errorf("bot-2 message buttons after snapshot/restore = %+v, want %+v", beta[0].Buttons, wantButtons)
	}

	// the trimmed and post-snapshot messages must not be reachable.
	for _, key := range []string{"a0", "a1", "a2", "after-snapshot"} {
		if _, ok := dst.GetMessage(key); ok {
			t.Errorf("GetMessage(%q) unexpectedly found after restore", key)
		}
	}

	// the restored FSM must still behave like a normal FSM afterwards
	// (idempotency index rebuilt correctly, not just the display list).
	dup := mustApply(t, dst, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
		IdempotencyKey: "a5", BotID: "bot-1", ChatID: 10, Text: "should be treated as a duplicate of a5", CreatedAt: t1(1000),
	}})
	if dup.Message.Text == "should be treated as a duplicate of a5" {
		t.Errorf("msgIndex not rebuilt by Restore: reused key a5 created a new message instead of being treated as a duplicate")
	}
}
