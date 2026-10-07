package telegram

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// TestLive_RealTelegram runs the client against the real Telegram Bot API.
// It is skipped unless a real bot is provided:
//
//	BOTMANAGER_LIVE_TOKEN    bot token
//	BOTMANAGER_LIVE_CHAT_ID  chat the bot can write to (a group with the bot,
//	                         or a user who has started the bot)
//	BOTMANAGER_LIVE_PROXY    optional, e.g. socks5h://127.0.0.1:1080
//
// It sends one message with inline buttons, edits it, checks the bot's
// membership in the chat and deletes the message again. getUpdates is
// exercised too, with a zero timeout, without acknowledging anything.
func TestLive_RealTelegram(t *testing.T) {
	token := os.Getenv("BOTMANAGER_LIVE_TOKEN")
	chatRaw := os.Getenv("BOTMANAGER_LIVE_CHAT_ID")
	if token == "" || chatRaw == "" {
		t.Skip("set BOTMANAGER_LIVE_TOKEN and BOTMANAGER_LIVE_CHAT_ID to run against the real Telegram Bot API")
	}
	chatID, err := strconv.ParseInt(chatRaw, 10, 64)
	if err != nil {
		t.Fatalf("BOTMANAGER_LIVE_CHAT_ID: %v", err)
	}

	proxy := raftcluster.ProxyConfig{}
	if p := os.Getenv("BOTMANAGER_LIVE_PROXY"); p != "" {
		proxy = raftcluster.ProxyConfig{Enabled: true, Address: p}
	}
	httpClient, err := NewHTTPClient(proxy)
	if err != nil {
		t.Fatal(err)
	}
	api := NewClient(token, httpClient, "")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	me, err := api.GetMe(ctx)
	if err != nil {
		t.Fatalf("getMe: %v (class %s)", err, ClassifyFailure(err))
	}
	if !me.IsBot || me.ID == 0 {
		t.Fatalf("getMe = %+v", me)
	}
	t.Logf("bot @%s (%d)", me.Username, me.ID)

	if _, err := api.GetUpdates(ctx, 0, 0, AllowedUpdateKinds); err != nil {
		// Конфликт с вебхуком или другим polling-процессом — тоже полезный
		// диагноз, но не провал клиента.
		t.Logf("getUpdates: %v (class %s)", err, ClassifyFailure(err))
	}

	if chat, err := api.GetChat(ctx, chatID); err != nil {
		t.Fatalf("getChat: %v", err)
	} else {
		t.Logf("chat %d: type=%s title=%q", chat.ID, chat.Type, chat.Title)
	}
	if member, err := api.GetBotMembership(ctx, chatID, me.ID); err != nil {
		t.Fatalf("getChatMember: %v", err)
	} else {
		t.Logf("bot membership: %+v", member)
	}

	sent, err := api.SendMessage(ctx, chatID, "botmanager live test "+time.Now().UTC().Format(time.RFC3339), 0, []raftcluster.Button{
		{Text: "botmanager", URL: "https://github.com/h5vx/botmanager"},
		{Text: "callback", CallbackData: "live-test"},
	})
	if err != nil {
		t.Fatalf("sendMessage: %v (class %s)", err, ClassifyFailure(err))
	}
	if _, err := api.EditMessageText(ctx, chatID, sent.MessageID, "botmanager live test (edited)"); err != nil {
		t.Fatalf("editMessageText: %v", err)
	}
	if err := api.DeleteMessage(ctx, chatID, sent.MessageID); err != nil {
		t.Fatalf("deleteMessage: %v", err)
	}

	// Заведомо неверный токен должен классифицироваться как проблема бота.
	bad := NewClient("1:invalid-token-for-live-test", httpClient, "")
	if _, err := bad.GetMe(ctx); err == nil || ClassifyFailure(err) != raftcluster.FailureClassBot {
		t.Fatalf("invalid token: err = %v, class = %v, want FailureClassBot", err, ClassifyFailure(err))
	}
}
