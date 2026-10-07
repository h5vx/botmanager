package rpcserver

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/internal/raftcluster"
	"github.com/h5vx/botmanager/internal/telegram"
	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
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
		if got := chatMemberStatusIsMember(status); got != want {
			t.Errorf("chatMemberStatusIsMember(%q) = %v, want %v", status, got, want)
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

func TestTelegramUpdateToProto(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	msg := telegramUpdateToProto(telegram.Update{
		Kind: telegram.UpdateKindIncomingMessage, BotID: "b1", ChatID: 1, MessageID: 2, FromUserID: 3,
		Text: "hi", ReceivedAt: now,
	})
	im := msg.GetIncomingMessage()
	if im == nil || im.GetBotId() != "b1" || im.GetText() != "hi" || !msg.GetOccurredAt().AsTime().Equal(now) {
		t.Fatalf("incoming_message = %+v", msg)
	}

	cb := telegramUpdateToProto(telegram.Update{
		Kind: telegram.UpdateKindCallbackQuery, BotID: "b1", CallbackQueryID: "cq1", CallbackData: "data",
		ChatID: 1, MessageID: 2, FromUserID: 3, ReceivedAt: now,
	})
	if cb.GetCallbackQuery() == nil || cb.GetCallbackQuery().GetCallbackQueryId() != "cq1" {
		t.Fatalf("callback_query = %+v", cb)
	}

	cm := telegramUpdateToProto(telegram.Update{
		Kind: telegram.UpdateKindChatMemberChanged, BotID: "b1", ChatID: 1, NewChatMemberStatus: "left", ReceivedAt: now,
	})
	if cm.GetChatMemberChanged() == nil || cm.GetChatMemberChanged().GetBotIsMember() {
		t.Fatalf("chat_member_changed = %+v", cm)
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

func TestFsmEventToProto(t *testing.T) {
	msgEvent := raftcluster.Event{
		Command: raftcluster.CommandPutMessage,
		Message: &raftcluster.Message{IdempotencyKey: "k1", BotID: "b1", Delivery: raftcluster.DeliveryInfo{Status: raftcluster.DeliveryStatusPending}},
	}
	botID, out := fsmEventToProto(msgEvent)
	if botID != "b1" || out.GetMessageStatusChanged() == nil || out.GetMessageStatusChanged().GetIdempotencyKey() != "k1" {
		t.Fatalf("put_message event = %q, %+v", botID, out)
	}

	botEvent := raftcluster.Event{
		Command: raftcluster.CommandSetBotState,
		Bot:     &raftcluster.Bot{ID: "b1", State: raftcluster.BotStateBroken, LastFailureClass: raftcluster.FailureClassBot, LastFailureReason: "401"},
	}
	botID, out = fsmEventToProto(botEvent)
	if botID != "b1" || out.GetBotStateChanged() == nil || out.GetBotStateChanged().GetState() != botmanagerpb.BotState_BOT_STATE_BROKEN {
		t.Fatalf("set_bot_state event = %q, %+v", botID, out)
	}

	// CommandCreateBot/CommandUpdateBot are deliberately not translated
	// (see messaging.go's doc comment on Subscribe).
	createEvent := raftcluster.Event{Command: raftcluster.CommandCreateBot, Bot: &raftcluster.Bot{ID: "b1"}}
	if _, out := fsmEventToProto(createEvent); out != nil {
		t.Fatalf("create_bot event should not translate to an Update, got %+v", out)
	}

	// A nil Message/Bot (should not happen for a real successfully-applied
	// event, but fsmEventToProto must not panic) yields no Update.
	if _, out := fsmEventToProto(raftcluster.Event{Command: raftcluster.CommandPutMessage}); out != nil {
		t.Fatalf("nil Message should yield no Update, got %+v", out)
	}
}
