package rpcserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
)

// newFakeTelegramServer emulates just enough of the real Telegram Bot API
// for these tests — same pattern internal/telegram/client_test.go uses,
// duplicated here (unexported, package-local) rather than shared, since
// internal/telegram deliberately does not depend on this package or vice
// versa (see doc.go's boundary note).
func newFakeTelegramServer(t *testing.T, handler func(method string, body map[string]any) (status int, envelope map[string]any)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)

		method := r.URL.Path
		for i := len(method) - 1; i >= 0; i-- {
			if method[i] == '/' {
				method = method[i+1:]
				break
			}
		}

		status, env := handler(method, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(env)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestIntegration_EditMessage_LiveTelegramCall covers the
// message-lookup-then-live-Telegram-call path (EditMessage/DeleteMessage/
// PinMessage/UnpinMessage all share messageAndBot + telegramClientFor +
// telegramError — this exercises the shared machinery once through
// EditMessage) against a fake Bot API server, not api.telegram.org (see
// CLAUDE.md's note on internal/telegram having no live token to
// test against — the same constraint applies here).
func TestIntegration_EditMessage_LiveTelegramCall(t *testing.T) {
	var gotChatID, gotMessageID float64
	var gotText string
	srv := newFakeTelegramServer(t, func(method string, body map[string]any) (int, map[string]any) {
		if method != "editMessageText" {
			t.Errorf("unexpected method %q", method)
		}
		gotChatID, _ = body["chat_id"].(float64)
		gotMessageID, _ = body["message_id"].(float64)
		gotText, _ = body["text"].(string)
		return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"message_id": gotMessageID}}
	})

	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}
	if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
		IdempotencyKey: "k1", BotId: bot.GetId(), ChatId: 42, Text: "original",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// The message has no Telegram message_id yet (never SENT) — Telegram's
	// own zero value is exercised here on purpose: EditMessage does not
	// require the message to have been marked SENT first, it just forwards
	// whatever message_id raftcluster currently has for this key.
	if _, err := messaging.EditMessage(ctx, &botmanagerpb.EditMessageRequest{IdempotencyKey: "k1", Text: "edited"}); err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	if gotChatID != 42 || gotText != "edited" {
		t.Fatalf("editMessageText call: chat_id=%v text=%v", gotChatID, gotText)
	}

	// Unknown message -> NotFound, not a panic/500.
	if _, err := messaging.EditMessage(ctx, &botmanagerpb.EditMessageRequest{IdempotencyKey: "missing", Text: "x"}); err == nil {
		t.Fatalf("EditMessage(missing): want error, got nil")
	}
}

// TestIntegration_AnswerCallback_UsesBotIDFromRequest covers
// AnswerCallbackRequest.bot_id (see CLAUDE.md): the RPC
// must route the call through the bot named by bot_id, not some implicit
// bot.
func TestIntegration_AnswerCallback_UsesBotIDFromRequest(t *testing.T) {
	var calledWithID string
	srv := newFakeTelegramServer(t, func(method string, body map[string]any) (int, map[string]any) {
		if method != "answerCallbackQuery" {
			t.Errorf("unexpected method %q", method)
		}
		calledWithID, _ = body["callback_query_id"].(string)
		return http.StatusOK, map[string]any{"ok": true}
	})

	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	if _, err := messaging.AnswerCallback(ctx, &botmanagerpb.AnswerCallbackRequest{
		CallbackQueryId: "cq-1", BotId: bot.GetId(), Text: "ok",
	}); err != nil {
		t.Fatalf("AnswerCallback: %v", err)
	}
	if calledWithID != "cq-1" {
		t.Fatalf("callback_query_id = %q, want cq-1", calledWithID)
	}

	// Unknown bot_id -> NotFound.
	if _, err := messaging.AnswerCallback(ctx, &botmanagerpb.AnswerCallbackRequest{
		CallbackQueryId: "cq-2", BotId: "missing",
	}); err == nil {
		t.Fatalf("AnswerCallback(missing bot): want error, got nil")
	}
}

// TestIntegration_Subscribe_FiltersByBot: a subscriber sees
// message_status_changed events produced by a real Send through the same
// node, filtered to the requested bot_id — an event for a different bot
// must not arrive.
func TestIntegration_Subscribe_FiltersByBot(t *testing.T) {
	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClients(t, node)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	botA, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "A", Token: "123:a"})
	if err != nil {
		t.Fatalf("CreateBot A: %v", err)
	}
	botB, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:b"})
	if err != nil {
		t.Fatalf("CreateBot B: %v", err)
	}

	stream, err := messaging.Subscribe(ctx, &botmanagerpb.SubscribeRequest{BotIds: []string{botA.GetId()}})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Filtered out: a Send for bot B must not produce a visible update.
	if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
		IdempotencyKey: "b-msg", BotId: botB.GetId(), ChatId: 1, Text: "hi",
	}); err != nil {
		t.Fatalf("Send (bot B): %v", err)
	}

	// Visible: a Send for bot A produces a message_status_changed update.
	if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
		IdempotencyKey: "a-msg", BotId: botA.GetId(), ChatId: 1, Text: "hi",
	}); err != nil {
		t.Fatalf("Send (bot A): %v", err)
	}

	upd, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv: %v", err)
	}
	msc := upd.GetMessageStatusChanged()
	if msc == nil || msc.GetIdempotencyKey() != "a-msg" {
		t.Fatalf("first update = %+v, want message_status_changed for a-msg", upd)
	}
}

// TestSend_WithButtons_Succeeds covers the happy path of Messaging.Send's
// inline-button support: a well-formed URL button
// is accepted and the message ends up PENDING like a button-less Send.
func TestSend_WithButtons_Succeeds(t *testing.T) {
	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClients(t, node)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	ack, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
		IdempotencyKey: "with-button", BotId: bot.GetId(), ChatId: 1, Text: "poll is open",
		Buttons: []*botmanagerpb.InlineButton{{Text: "Открыть", Url: "https://example.org/app"}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if ack.GetStatus() != botmanagerpb.DeliveryStatus_DELIVERY_STATUS_PENDING {
		t.Fatalf("Send.Status = %v, want PENDING", ack.GetStatus())
	}
}

// TestSend_RejectsInvalidButtons covers the trust-boundary validation
// buttonsFromProto performs (convert.go): an empty button text and a
// non-http(s) URL must both be rejected with InvalidArgument, and
// SendBatch must reject the same way since it reuses Send's validation
// path (messaging.go's send helper).
func TestSend_RejectsInvalidButtons(t *testing.T) {
	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClients(t, node)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	cases := []struct {
		name    string
		buttons []*botmanagerpb.InlineButton
	}{
		{"empty text", []*botmanagerpb.InlineButton{{Text: "", Url: "https://example.org"}}},
		{"non-http url", []*botmanagerpb.InlineButton{{Text: "Открыть", Url: "tg://resolve"}}},
		{"web_app-looking url without scheme", []*botmanagerpb.InlineButton{{Text: "Открыть", Url: "example.org"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := messaging.Send(ctx, &botmanagerpb.SendRequest{
				IdempotencyKey: "bad-" + c.name, BotId: bot.GetId(), ChatId: 1, Text: "hi", Buttons: c.buttons,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Send error = %v, want InvalidArgument", err)
			}
		})
	}

	// SendBatch reuses the same validation path (see messaging.go's send
	// helper) — one bad message in the batch must fail the whole call.
	_, err = messaging.SendBatch(ctx, &botmanagerpb.SendBatchRequest{Messages: []*botmanagerpb.SendRequest{
		{IdempotencyKey: "batch-ok", BotId: bot.GetId(), ChatId: 1, Text: "hi"},
		{IdempotencyKey: "batch-bad", BotId: bot.GetId(), ChatId: 1, Text: "hi",
			Buttons: []*botmanagerpb.InlineButton{{Text: "", Url: "https://example.org"}}},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SendBatch error = %v, want InvalidArgument", err)
	}
}

// TestSend_RejectsTooManyButtons covers buttonsFromProto's maxSendButtons
// cap (convert.go) — a request over the limit is rejected rather than
// silently truncated.
func TestSend_RejectsTooManyButtons(t *testing.T) {
	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClients(t, node)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	buttons := make([]*botmanagerpb.InlineButton, 9)
	for i := range buttons {
		buttons[i] = &botmanagerpb.InlineButton{Text: "b", Url: "https://example.org"}
	}
	_, err = messaging.Send(ctx, &botmanagerpb.SendRequest{
		IdempotencyKey: "too-many", BotId: bot.GetId(), ChatId: 1, Text: "hi", Buttons: buttons,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Send error = %v, want InvalidArgument", err)
	}
}
