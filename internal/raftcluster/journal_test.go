package raftcluster

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func journalKinds(entries []JournalEntry) []JournalKind {
	out := make([]JournalKind, len(entries))
	for i, e := range entries {
		out[i] = e.Kind
	}
	return out
}

// TestJournal_CommandsProduceEntries: every state change a subscriber cares
// about lands in the journal exactly once, with contiguous sequence numbers;
// metadata edits and idempotent repeats do not.
func TestJournal_CommandsProduceEntries(t *testing.T) {
	fsm := NewFSM(10)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})
	name := "renamed"
	mustApply(t, fsm, Command{Type: CommandUpdateBot, UpdateBot: &UpdateBotCommand{ID: "bot-1", DisplayName: &name, UpdatedAt: t1(1)}})
	mustApply(t, fsm, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{ID: "bot-1", State: BotStateEnabled, UpdatedAt: t1(2)}})
	put := Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{IdempotencyKey: "k1", BotID: "bot-1", ChatID: 7, Text: "hi", CreatedAt: t1(3)}}
	mustApply(t, fsm, put)
	mustApply(t, fsm, put) // идемпотентный повтор — без записи в журнал
	mustApply(t, fsm, Command{Type: CommandUpdateDelivery, UpdateDelivery: &UpdateDeliveryCommand{IdempotencyKey: "k1", Status: DeliveryStatusSent, SentAt: t1(4)}})

	entries, oldest, latest := fsm.ReadJournal(0, 0)
	want := []JournalKind{JournalBotStateChanged, JournalMessageStatusChanged, JournalMessageStatusChanged}
	if got := journalKinds(entries); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("kinds = %v, want %v", got, want)
	}
	if oldest != 1 || latest != 3 {
		t.Fatalf("oldest/latest = %d/%d, want 1/3", oldest, latest)
	}
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			t.Fatalf("entry %d seq = %d", i, e.Seq)
		}
	}
	if entries[0].BotState == nil || entries[0].BotState.State != BotStateEnabled {
		t.Fatalf("bot state entry = %+v", entries[0])
	}
	if entries[2].Delivery == nil || entries[2].Delivery.Status != DeliveryStatusSent || entries[2].IdempotencyKey != "k1" {
		t.Fatalf("delivery entry = %+v", entries[2])
	}

	after, _, _ := fsm.ReadJournal(2, 0)
	if len(after) != 1 || after[0].Seq != 3 {
		t.Fatalf("ReadJournal(2) = %+v", after)
	}
	if none, _, _ := fsm.ReadJournal(3, 0); len(none) != 0 {
		t.Fatalf("ReadJournal(latest) = %+v, want empty", none)
	}
}

// TestJournal_RecordUpdates_DedupAndOffset: updates below the stored offset
// (re-polled by a new leader after failover) are dropped, the offset only
// moves forward, and membership updates also feed the chat registry.
func TestJournal_RecordUpdates_DedupAndOffset(t *testing.T) {
	fsm := NewFSM(10)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	mustApply(t, fsm, Command{Type: CommandRecordUpdates, RecordUpdates: &RecordUpdatesCommand{
		BotID: "bot-1", NextOffset: 12,
		Updates: []IncomingUpdate{
			{Kind: JournalIncomingMessage, UpdateID: 10, ChatID: 5, Text: "a", ReceivedAt: t1(1)},
			{Kind: JournalChatMemberChanged, UpdateID: 11, ChatID: 6, NewChatMemberStatus: "member", ChatTitle: "Group", ReceivedAt: t1(2)},
		},
	}})
	// новый лидер переспросил с более старым offset — 11 уже записан
	mustApply(t, fsm, Command{Type: CommandRecordUpdates, RecordUpdates: &RecordUpdatesCommand{
		BotID: "bot-1", NextOffset: 13,
		Updates: []IncomingUpdate{
			{Kind: JournalChatMemberChanged, UpdateID: 11, ChatID: 6, NewChatMemberStatus: "member", ReceivedAt: t1(2)},
			{Kind: JournalCallbackQuery, UpdateID: 12, CallbackData: "x", ReceivedAt: t1(3)},
		},
	}})

	entries, _, _ := fsm.ReadJournal(0, 0)
	got := journalKinds(entries)
	if len(got) != 3 || got[2] != JournalCallbackQuery {
		t.Fatalf("kinds = %v", got)
	}
	if entries[0].Update.Text != "a" || entries[0].BotID != "bot-1" {
		t.Fatalf("first entry = %+v", entries[0])
	}
	if off := fsm.PollOffset("bot-1"); off != 13 {
		t.Fatalf("offset = %d, want 13", off)
	}
	mustApply(t, fsm, Command{Type: CommandRecordUpdates, RecordUpdates: &RecordUpdatesCommand{BotID: "bot-1", NextOffset: 5}})
	if off := fsm.PollOffset("bot-1"); off != 13 {
		t.Fatalf("offset moved backwards to %d", off)
	}
	reg := fsm.ListChatRegistry("bot-1")
	if len(reg) != 1 || reg[0].ChatID != 6 || !reg[0].IsMember || reg[0].Title != "Group" {
		t.Fatalf("registry = %+v", reg)
	}
}

// TestJournal_Retention: only the newest entries are kept, sequence numbers
// keep growing, and ReadJournal reports the oldest retained one so a
// subscriber can detect a gap.
func TestJournal_Retention(t *testing.T) {
	fsm := NewFSM(10)
	fsm.SetJournalRetention(3)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})
	for i := 0; i < 5; i++ {
		state := BotStateEnabled
		if i%2 == 1 {
			state = BotStateDisabled
		}
		mustApply(t, fsm, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{ID: "bot-1", State: state, UpdatedAt: t1(i)}})
	}
	entries, oldest, latest := fsm.ReadJournal(0, 0)
	if len(entries) != 3 || oldest != 3 || latest != 5 || entries[0].Seq != 3 {
		t.Fatalf("entries=%d oldest=%d latest=%d first=%d", len(entries), oldest, latest, entries[0].Seq)
	}
	if part, _, _ := fsm.ReadJournal(3, 1); len(part) != 1 || part[0].Seq != 4 {
		t.Fatalf("ReadJournal(3,1) = %+v", part)
	}
}

// TestJournal_SnapshotRoundtrip: the journal, its counter, poll offsets and
// the node registry survive Snapshot/Restore.
func TestJournal_SnapshotRoundtrip(t *testing.T) {
	src := NewFSM(10)
	mustApply(t, src, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})
	mustApply(t, src, Command{Type: CommandRecordUpdates, RecordUpdates: &RecordUpdatesCommand{
		BotID: "bot-1", NextOffset: 2,
		Updates: []IncomingUpdate{{Kind: JournalIncomingMessage, UpdateID: 1, Text: "x", ReceivedAt: t1(1)}},
	}})
	mustApply(t, src, Command{Type: CommandRegisterNode, RegisterNode: &RegisterNodeCommand{Node: NodeInfo{ID: "n1", RaftAddr: "a:1", GRPCAddr: "a:2"}}})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &fakeSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	dst := NewFSM(10)
	if err := dst.Restore(io.NopCloser(&sink.Buffer)); err != nil {
		t.Fatal(err)
	}
	entries, _, latest := dst.ReadJournal(0, 0)
	if len(entries) != 1 || entries[0].Update.Text != "x" || latest != 1 {
		t.Fatalf("restored journal = %+v latest=%d", entries, latest)
	}
	if dst.PollOffset("bot-1") != 2 {
		t.Fatalf("restored offset = %d", dst.PollOffset("bot-1"))
	}
	if n, ok := dst.GetNode("n1"); !ok || n.GRPCAddr != "a:2" {
		t.Fatalf("restored node = %+v %v", n, ok)
	}
	// счётчик продолжается, а не начинается заново
	mustApply(t, dst, Command{Type: CommandSetBotState, SetBotState: &SetBotStateCommand{ID: "bot-1", State: BotStateEnabled}})
	if _, _, latest := dst.ReadJournal(0, 0); latest != 2 {
		t.Fatalf("latest after restore+apply = %d, want 2", latest)
	}
}

func TestTokenCipher_Roundtrip(t *testing.T) {
	c, err := NewTokenCipher(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	enc, err := c.Encrypt("123:secret")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(enc, encryptedTokenPrefix) || strings.Contains(enc, "secret") {
		t.Fatalf("encrypted = %q", enc)
	}
	if again, _ := c.Encrypt(enc); again != enc {
		t.Fatalf("double encryption changed value")
	}
	plain, err := c.Decrypt(enc)
	if err != nil || plain != "123:secret" {
		t.Fatalf("decrypt = %q, %v", plain, err)
	}
	if legacy, _ := c.Decrypt("123:plain"); legacy != "123:plain" {
		t.Fatalf("plaintext passthrough = %q", legacy)
	}

	other, _ := NewTokenCipher(bytes.Repeat([]byte{8}, 32))
	if _, err := other.Decrypt(enc); err == nil {
		t.Fatalf("decrypt with wrong key succeeded")
	}
	if _, err := NewTokenCipher([]byte("short")); err == nil {
		t.Fatalf("short key accepted")
	}
}
