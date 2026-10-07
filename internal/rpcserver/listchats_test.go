package rpcserver

import (
	"context"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/internal/raftcluster"
	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
)

// TestBotAdmin_ListChats covers distinct chats a bot has
// exchanged messages with, most-recently-active first, with a
// best-effort title from a live getChat call.
func TestBotAdmin_ListChats(t *testing.T) {
	titles := map[int64]string{100: "Первая группа", 200: "Вторая группа"}
	srv := newFakeTelegramServer(t, func(method string, body map[string]any) (int, map[string]any) {
		if method != "getChat" {
			t.Errorf("unexpected method %q", method)
		}
		chatID, _ := body["chat_id"].(float64)
		title, ok := titles[int64(chatID)]
		if !ok {
			return http.StatusOK, map[string]any{"ok": false, "error_code": 400, "description": "chat not found"}
		}
		return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": chatID, "title": title, "type": "group"}}
	})

	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	// Chat 100 first, then 200, then 100 again — most recent activity for
	// 100 is the LAST message, so ListChats must return [100, 200], not
	// insertion order [100, 200] by coincidence — cover it with a distinct
	// 300 that has no known title (getChat returns an error for it).
	for i, chatID := range []int64{100, 200, 300, 100} {
		if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
			IdempotencyKey: "k" + string(rune('a'+i)), BotId: bot.GetId(), ChatId: chatID, Text: "hi",
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	list, err := botAdmin.ListChats(ctx, &botmanagerpb.ListChatsRequest{BotId: bot.GetId()})
	if err != nil {
		t.Fatalf("ListChats: %v", err)
	}
	if len(list.GetChats()) != 3 {
		t.Fatalf("got %d chats, want 3: %+v", len(list.GetChats()), list.GetChats())
	}
	// Most recently active chat (last message was to 100) comes first.
	if list.GetChats()[0].GetChatId() != 100 || list.GetChats()[0].GetTitle() != "Первая группа" {
		t.Fatalf("chats[0] = %+v, want chat 100 with known title", list.GetChats()[0])
	}
	if list.GetChats()[1].GetChatId() != 300 || list.GetChats()[1].GetTitle() != "" {
		t.Fatalf("chats[1] = %+v, want chat 300 with empty title (unknown to getChat)", list.GetChats()[1])
	}
	if list.GetChats()[2].GetChatId() != 200 || list.GetChats()[2].GetTitle() != "Вторая группа" {
		t.Fatalf("chats[2] = %+v, want chat 200 with known title", list.GetChats()[2])
	}

	// Unknown bot -> NotFound.
	if _, err := botAdmin.ListChats(ctx, &botmanagerpb.ListChatsRequest{BotId: "no-such-bot"}); status.Code(err) != codes.NotFound {
		t.Fatalf("ListChats(unknown bot) code = %v, want NotFound", status.Code(err))
	}
}

// TestBotAdmin_ListChats_NoHistory covers the empty case: a freshly
// created bot with no messages yet returns an empty list, not an error.
func TestBotAdmin_ListChats_NoHistory(t *testing.T) {
	node := newTestNode(t)
	botAdmin, _, _ := newTestClients(t, node)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	list, err := botAdmin.ListChats(ctx, &botmanagerpb.ListChatsRequest{BotId: bot.GetId()})
	if err != nil {
		t.Fatalf("ListChats: %v", err)
	}
	if len(list.GetChats()) != 0 {
		t.Fatalf("got %d chats, want 0", len(list.GetChats()))
	}
}

// TestBotAdmin_ListChats_RegistryAndHistoryMerge covers the chat registry: a chat
// the bot was just added to (registry only, zero messages) must already
// appear — that's the whole reason the registry exists — and a chat the
// bot was kicked from must never appear, even if old messages to it still
// exist in history. A chat that predates the registry (history only) still
// falls back to a live getChat lookup, unchanged from before this feature.
func TestBotAdmin_ListChats_RegistryAndHistoryMerge(t *testing.T) {
	titles := map[int64]string{500: "История"}
	srv := newFakeTelegramServer(t, func(method string, body map[string]any) (int, map[string]any) {
		if method != "getChat" {
			t.Errorf("unexpected method %q", method)
		}
		chatID, _ := body["chat_id"].(float64)
		title, ok := titles[int64(chatID)]
		if !ok {
			return http.StatusOK, map[string]any{"ok": false, "error_code": 400, "description": "chat not found"}
		}
		return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": chatID, "title": title, "type": "group"}}
	})

	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	// Chat 100: joined, never messaged — must appear from the registry
	// alone.
	applyMembership(t, node, bot.GetId(), 100, "Только что добавили", true, t1(1))
	// Chat 200: joined then kicked — must be excluded.
	applyMembership(t, node, bot.GetId(), 200, "Выгнали", true, t1(2))
	applyMembership(t, node, bot.GetId(), 200, "", false, t1(3))
	// Chat 500: history only (predates the registry) — falls back to
	// live getChat.
	if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
		IdempotencyKey: "hist-1", BotId: bot.GetId(), ChatId: 500, Text: "hi",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	list, err := botAdmin.ListChats(ctx, &botmanagerpb.ListChatsRequest{BotId: bot.GetId()})
	if err != nil {
		t.Fatalf("ListChats: %v", err)
	}
	got := map[int64]string{}
	for _, c := range list.GetChats() {
		got[c.GetChatId()] = c.GetTitle()
	}
	if len(got) != 2 {
		t.Fatalf("got %d chats, want 2: %+v", len(got), got)
	}
	if title, ok := got[100]; !ok || title != "Только что добавили" {
		t.Fatalf("chat 100 (registry, no messages) = %q, ok=%v", title, ok)
	}
	if _, ok := got[200]; ok {
		t.Fatalf("chat 200 (kicked) must be excluded, got %+v", got)
	}
	if title, ok := got[500]; !ok || title != "История" {
		t.Fatalf("chat 500 (history fallback) = %q, ok=%v", title, ok)
	}
}

func applyMembership(t *testing.T, node *raftcluster.Node, botID string, chatID int64, title string, isMember bool, changedAt time.Time) {
	t.Helper()
	res, err := node.Apply(raftcluster.Command{
		Type: raftcluster.CommandUpdateChatMembership,
		UpdateChatMembership: &raftcluster.UpdateChatMembershipCommand{
			BotID: botID, ChatID: chatID, Title: title, IsMember: isMember, ChangedAt: changedAt,
		},
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("Apply(update_chat_membership): %v", err)
	}
	if res.Err != nil {
		t.Fatalf("update_chat_membership rejected: %v", res.Err)
	}
}

func t1(offsetSeconds int) time.Time {
	return time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC).Add(time.Duration(offsetSeconds) * time.Second)
}
