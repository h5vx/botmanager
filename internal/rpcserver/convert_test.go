package rpcserver

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

func TestChatMemberRights(t *testing.T) {
	cases := []struct {
		status         string
		canPinMessages bool
		wantMember     bool
		wantPin        bool
		wantSend       bool
	}{
		{status: "creator", wantMember: true, wantPin: true, wantSend: true},
		{status: "administrator", canPinMessages: true, wantMember: true, wantPin: true, wantSend: true},
		{status: "administrator", canPinMessages: false, wantMember: true, wantPin: false, wantSend: true},
		{status: "member", wantMember: true, wantPin: false, wantSend: true},
		{status: "restricted", wantMember: true, wantPin: false, wantSend: false},
		{status: "left", wantMember: false, wantPin: false, wantSend: false},
		{status: "kicked", wantMember: false, wantPin: false, wantSend: false},
		{status: "some_future_status", wantMember: false, wantPin: false, wantSend: false},
	}
	for _, tc := range cases {
		isMember, canPin, canSend := chatMemberRights(tc.status, tc.canPinMessages)
		if isMember != tc.wantMember || canPin != tc.wantPin || canSend != tc.wantSend {
			t.Errorf("chatMemberRights(%q, %v) = (%v,%v,%v), want (%v,%v,%v)",
				tc.status, tc.canPinMessages, isMember, canPin, canSend, tc.wantMember, tc.wantPin, tc.wantSend)
		}
	}
}

func TestChatMemberStatusIsMember(t *testing.T) {
	cases := map[string]bool{
		"left": false, "kicked": false, "member": true, "administrator": true, "creator": true, "": false,
	}
	for status, want := range cases {
		if got := raftcluster.ChatMemberStatusIsMember(status); got != want {
			t.Errorf("ChatMemberStatusIsMember(%q) = %v, want %v", status, got, want)
		}
	}
}

func TestPaginateBots(t *testing.T) {
	all := []raftcluster.Bot{{ID: "a"}, {ID: "b"}, {ID: "c"}}

	page, next, err := paginateBots(all, 2, "")
	if err != nil || len(page) != 2 || next == "" {
		t.Fatalf("page1: page=%v next=%q err=%v", page, next, err)
	}
	page2, next2, err := paginateBots(all, 2, next)
	if err != nil || len(page2) != 1 || next2 != "" {
		t.Fatalf("page2: page=%v next=%q err=%v", page2, next2, err)
	}
	if _, _, err := paginateBots(all, 2, "not-a-number"); err == nil {
		t.Fatalf("invalid page_token: want error, got nil")
	}
}

func TestDeliveryStatusRoundTrip(t *testing.T) {
	cases := []raftcluster.DeliveryStatus{
		raftcluster.DeliveryStatusPending, raftcluster.DeliveryStatusRetrying,
		raftcluster.DeliveryStatusSent, raftcluster.DeliveryStatusFailed,
		raftcluster.DeliveryStatusCancelled,
	}
	for _, s := range cases {
		got := deliveryStatusFromProto(deliveryStatusToProto(s))
		if got != s {
			t.Errorf("round trip %v -> %v -> %v", s, deliveryStatusToProto(s), got)
		}
	}
}

func TestBotIDFilterSet(t *testing.T) {
	empty := botIDSet(nil)
	if !empty.matches("anything") {
		t.Errorf("empty filter should match everything")
	}

	filtered := botIDSet([]string{"bot-1", "bot-2"})
	if !filtered.matches("bot-1") || filtered.matches("bot-3") {
		t.Errorf("filtered set matched incorrectly: %+v", filtered)
	}
}

func TestJournalEntryToProto(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	msg := journalEntryToProto(raftcluster.JournalEntry{
		Seq: 7, Kind: raftcluster.JournalIncomingMessage, BotID: "b1", OccurredAt: now,
		Update: &raftcluster.IncomingUpdate{ChatID: 1, MessageID: 2, FromUserID: 3, Text: "hi", ReceivedAt: now,
			FromUsername: "anna_k", FromFirstName: "Анна", FromLastName: "К", FromLanguageCode: "ru"},
	})
	im := msg.GetIncomingMessage()
	if im == nil || im.GetBotId() != "b1" || im.GetText() != "hi" || !msg.GetOccurredAt().AsTime().Equal(now) || msg.GetSequence() != 7 {
		t.Fatalf("incoming_message = %+v", msg)
	}
	if f := im.GetFrom(); f.GetId() != 3 || f.GetUsername() != "anna_k" || f.GetFirstName() != "Анна" ||
		f.GetLastName() != "К" || f.GetLanguageCode() != "ru" || f.GetIsBot() {
		t.Fatalf("incoming_message.from = %+v", f)
	}

	cb := journalEntryToProto(raftcluster.JournalEntry{
		Seq: 8, Kind: raftcluster.JournalCallbackQuery, BotID: "b1",
		Update: &raftcluster.IncomingUpdate{CallbackQueryID: "cq1", CallbackData: "data", ChatID: 1, MessageID: 2, FromUserID: 3},
	})
	if cb.GetCallbackQuery() == nil || cb.GetCallbackQuery().GetCallbackQueryId() != "cq1" {
		t.Fatalf("callback_query = %+v", cb)
	}
	// records written before the sender profile existed: id only
	if f := cb.GetCallbackQuery().GetFrom(); f.GetId() != 3 || f.GetFirstName() != "" {
		t.Fatalf("callback_query.from = %+v", f)
	}
	// no sender at all (channel post): from is absent
	ch := journalEntryToProto(raftcluster.JournalEntry{Kind: raftcluster.JournalIncomingMessage, BotID: "b1",
		Update: &raftcluster.IncomingUpdate{ChatID: -100, Text: "post"}})
	if ch.GetIncomingMessage().GetFrom() != nil {
		t.Fatalf("channel post must have no from: %+v", ch)
	}

	cm := journalEntryToProto(raftcluster.JournalEntry{
		Seq: 9, Kind: raftcluster.JournalChatMemberChanged, BotID: "b1",
		Update: &raftcluster.IncomingUpdate{ChatID: 1, NewChatMemberStatus: "left"},
	})
	if cm.GetChatMemberChanged() == nil || cm.GetChatMemberChanged().GetBotIsMember() {
		t.Fatalf("chat_member_changed = %+v", cm)
	}

	st := journalEntryToProto(raftcluster.JournalEntry{
		Seq: 10, Kind: raftcluster.JournalBotStateChanged, BotID: "b1",
		BotState: &raftcluster.BotStateChange{State: raftcluster.BotStateBroken, FailureClass: raftcluster.FailureClassBot, Reason: "401"},
	})
	if bs := st.GetBotStateChanged(); bs == nil || bs.GetState() != botmanagerpb.BotState_BOT_STATE_BROKEN || bs.GetReason() != "401" {
		t.Fatalf("bot_state_changed = %+v", st)
	}

	if journalEntryToProto(raftcluster.JournalEntry{Kind: raftcluster.JournalIncomingMessage}) != nil {
		t.Fatalf("entry without payload must convert to nil")
	}
}

// TestButtonsFromProto_UrlOrCallbackData covers the "exactly one of url or
// callback_data" rule (convert.go): url-only and
// callback_data-only are both accepted, both-or-neither are rejected.
func TestButtonsFromProto_UrlOrCallbackData(t *testing.T) {
	cases := []struct {
		name    string
		button  *botmanagerpb.InlineButton
		wantErr bool
	}{
		{"url only", &botmanagerpb.InlineButton{Text: "Открыть", Url: "https://example.org"}, false},
		{"callback_data only", &botmanagerpb.InlineButton{Text: "Буду", CallbackData: "avail:yes"}, false},
		{"both", &botmanagerpb.InlineButton{Text: "Оба", Url: "https://example.org", CallbackData: "avail:yes"}, true},
		{"neither", &botmanagerpb.InlineButton{Text: "Ничего"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := buttonsFromProto([]*botmanagerpb.InlineButton{tc.button})
			if tc.wantErr {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("err = %v, want InvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("buttonsFromProto: %v", err)
			}
			if len(out) != 1 || out[0].URL != tc.button.GetUrl() || out[0].CallbackData != tc.button.GetCallbackData() {
				t.Fatalf("buttonsFromProto = %+v, want url=%q callback_data=%q", out, tc.button.GetUrl(), tc.button.GetCallbackData())
			}
		})
	}
}

// TestButtonsFromProto_CallbackDataTooLong checks Telegram's 64-byte limit
// on callback_data is enforced in bytes, not runes — a multi-byte Cyrillic
// string that fits in 64 runes but not 64 bytes must still be rejected.
func TestButtonsFromProto_CallbackDataTooLong(t *testing.T) {
	// 33 Cyrillic runes = 66 bytes in UTF-8 (2 bytes/rune) — over the limit
	// on bytes even though far under it on rune count.
	data := strings.Repeat("б", 33)
	if len(data) <= maxCallbackDataBytes {
		t.Fatalf("test fixture too short: %d bytes", len(data))
	}

	_, err := buttonsFromProto([]*botmanagerpb.InlineButton{{Text: "Буду", CallbackData: data}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}
