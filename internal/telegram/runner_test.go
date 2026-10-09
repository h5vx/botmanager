package telegram

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/botlifecycle"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

// validTestToken matches tokenPattern (runner.go) — a structurally valid,
// obviously fake token; no real Telegram credentials exist for this
// project's tests (see the task brief: there is no real bot token to test
// against, so every test here talks to an httptest.Server instead).
const validTestToken = "123456789:AAExxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func startTestRunner(t *testing.T, cluster ClusterView, apiBaseURL string, bot raftcluster.Bot) botlifecycle.BotRunner {
	t.Helper()
	factory := NewRunnerFactory(cluster, Config{
		APIBaseURL:       apiBaseURL,
		RequestTimeout:   time.Second,
		LongPollTimeout:  time.Second,
		SendPollInterval: 20 * time.Millisecond,
	}, raftcluster.ProxyConfig{}, testLogger())

	runner := factory()
	if err := runner.Start(context.Background(), bot); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(runner.Stop)
	return runner
}

// emptyGetUpdates is the getUpdates response every test in this file uses:
// none of these tests exercise incoming updates, only the outgoing queue —
// see client_test.go's TestClient_GetUpdates_ParsesMessageCallbackAndChatMember
// for update parsing coverage.
func emptyGetUpdates() map[string]any {
	return map[string]any{"ok": true, "result": []any{}}
}

func TestRunner_SendsPendingMessagesAndUpdatesDelivery(t *testing.T) {
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		switch method {
		case "getUpdates":
			return http.StatusOK, emptyGetUpdates()
		case "sendMessage":
			return http.StatusOK, map[string]any{
				"ok":     true,
				"result": map[string]any{"message_id": 999, "chat": map[string]any{"id": body["chat_id"]}, "text": body["text"]},
			}
		default:
			t.Fatalf("unexpected method %q", method)
			return 0, nil
		}
	})

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)
	cluster.putMessage(raftcluster.Message{
		IdempotencyKey: "key-1", BotID: bot.ID, ChatID: 42, Text: "hello",
		Priority: raftcluster.PriorityNormal, CreatedAt: time.Now(),
		Delivery: raftcluster.DeliveryInfo{Status: raftcluster.DeliveryStatusPending},
	})

	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, 2*time.Second, func() bool {
		return cluster.getMessage("key-1").Delivery.Status == raftcluster.DeliveryStatusSent
	})

	msg := cluster.getMessage("key-1")
	if msg.MessageID != 999 {
		t.Errorf("MessageID = %d, want 999", msg.MessageID)
	}
	if msg.Delivery.SentAt.IsZero() {
		t.Error("SentAt is zero after a successful send")
	}
}

// TestRunner_NewRunnerSendsRetryingMessagesAtOnce: a runner starts on a
// new leader, and the RETRYING message's backoff was set by the previous
// leader — the first pass sends it without waiting for NextRetryAt.
func TestRunner_NewRunnerSendsRetryingMessagesAtOnce(t *testing.T) {
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		switch method {
		case "getUpdates":
			return http.StatusOK, emptyGetUpdates()
		case "sendMessage":
			return http.StatusOK, map[string]any{
				"ok":     true,
				"result": map[string]any{"message_id": 7, "chat": map[string]any{"id": body["chat_id"]}, "text": body["text"]},
			}
		default:
			t.Fatalf("unexpected method %q", method)
			return 0, nil
		}
	})

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)
	cluster.putMessage(raftcluster.Message{
		IdempotencyKey: "key-1", BotID: bot.ID, ChatID: 42, Text: "login",
		Priority: raftcluster.PriorityNormal, CreatedAt: time.Now(),
		Delivery: raftcluster.DeliveryInfo{
			Status: raftcluster.DeliveryStatusRetrying, Retries: 5,
			NextRetryAt: time.Now().Add(time.Hour),
		},
	})

	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, 2*time.Second, func() bool {
		return cluster.getMessage("key-1").Delivery.Status == raftcluster.DeliveryStatusSent
	})
}

func TestRunner_PriorityCriticalBeforeNormal(t *testing.T) {
	var mu sync.Mutex
	var order []string

	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		switch method {
		case "getUpdates":
			return http.StatusOK, emptyGetUpdates()
		case "sendMessage":
			mu.Lock()
			order = append(order, body["text"].(string))
			n := len(order)
			mu.Unlock()
			return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"message_id": n}}
		default:
			t.Fatalf("unexpected method %q", method)
			return 0, nil
		}
	})

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)

	now := time.Now()
	pending := func(key, text string, priority raftcluster.Priority, createdAt time.Time) raftcluster.Message {
		return raftcluster.Message{
			IdempotencyKey: key, BotID: bot.ID, ChatID: 1, Text: text,
			Priority: priority, CreatedAt: createdAt,
			Delivery: raftcluster.DeliveryInfo{Status: raftcluster.DeliveryStatusPending},
		}
	}
	// Оба normal-сообщения записаны раньше critical, но приоритет должен
	// обогнать порядок создания.
	cluster.putMessage(pending("normal-1", "normal-1", raftcluster.PriorityNormal, now))
	cluster.putMessage(pending("normal-2", "normal-2", raftcluster.PriorityNormal, now.Add(time.Millisecond)))
	cluster.putMessage(pending("critical-1", "critical-1", raftcluster.PriorityCritical, now.Add(2*time.Millisecond)))

	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})

	mu.Lock()
	defer mu.Unlock()
	if order[0] != "critical-1" {
		t.Errorf("send order = %v, want critical-1 first", order)
	}
}

func TestRunner_RecipientFailureMarksMessageFailed(t *testing.T) {
	srv := newTestServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		switch method {
		case "getUpdates":
			return http.StatusOK, emptyGetUpdates()
		case "sendMessage":
			return http.StatusForbidden, map[string]any{
				"ok": false, "error_code": 403, "description": "Forbidden: bot was blocked by the user",
			}
		default:
			t.Fatalf("unexpected method %q", method)
			return 0, nil
		}
	})

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)
	cluster.putMessage(raftcluster.Message{
		IdempotencyKey: "key-1", BotID: bot.ID, ChatID: 42, Text: "hi",
		Priority: raftcluster.PriorityNormal, CreatedAt: time.Now(),
		Delivery: raftcluster.DeliveryInfo{Status: raftcluster.DeliveryStatusPending},
	})

	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, 2*time.Second, func() bool {
		return cluster.getMessage("key-1").Delivery.Status == raftcluster.DeliveryStatusFailed
	})

	msg := cluster.getMessage("key-1")
	if msg.Delivery.LastError == "" {
		t.Error("LastError is empty for a FAILED message")
	}
	// Бот НЕ должен быть тронут — это проблема конкретного адресата, не бота.
	if got := cluster.getBot(bot.ID).State; got != raftcluster.BotStateEnabled {
		t.Errorf("bot state = %v, want unchanged (enabled)", got)
	}
}

func TestRunner_BotFailureMarksBotBrokenAndStopsRunner(t *testing.T) {
	var sendAttempts int32
	srv := newTestServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		switch method {
		case "getUpdates":
			return http.StatusOK, emptyGetUpdates()
		case "sendMessage":
			atomic.AddInt32(&sendAttempts, 1)
			return http.StatusUnauthorized, map[string]any{"ok": false, "error_code": 401, "description": "Unauthorized"}
		default:
			t.Fatalf("unexpected method %q", method)
			return 0, nil
		}
	})

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)
	cluster.putMessage(raftcluster.Message{
		IdempotencyKey: "key-1", BotID: bot.ID, ChatID: 42, Text: "hi",
		Priority: raftcluster.PriorityNormal, CreatedAt: time.Now(),
		Delivery: raftcluster.DeliveryInfo{Status: raftcluster.DeliveryStatusPending},
	})

	runner := startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, 2*time.Second, func() bool {
		return cluster.getBot(bot.ID).State == raftcluster.BotStateBroken
	})

	got := cluster.getBot(bot.ID)
	if got.LastFailureClass != raftcluster.FailureClassBot {
		t.Errorf("LastFailureClass = %v, want Bot", got.LastFailureClass)
	}
	if got.LastFailureReason == "" {
		t.Error("LastFailureReason is empty")
	}

	// Раннер должен был остановить себя сам (см. Runner.reportBroken) —
	// Stop() должен вернуться быстро, без ожидания внешней отмены ctx.
	done := make(chan struct{})
	go func() {
		runner.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return promptly after the runner self-reported broken state")
	}

	if atomic.LoadInt32(&sendAttempts) == 0 {
		t.Error("sendMessage was never attempted")
	}
}

func TestRunner_MalformedTokenRejectedByStart(t *testing.T) {
	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: "not-a-real-token", State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)

	factory := NewRunnerFactory(cluster, Config{}, raftcluster.ProxyConfig{}, testLogger())
	runner := factory()

	err := runner.Start(context.Background(), bot)
	if err == nil {
		t.Fatal("expected Start to reject a malformed token")
	}
}

func TestRunner_ContextCancelStopsLoopsPromptly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok": true, "result": []}`))
	}))
	t.Cleanup(srv.Close)

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)

	factory := NewRunnerFactory(cluster, Config{
		APIBaseURL:       srv.URL,
		RequestTimeout:   time.Second,
		LongPollTimeout:  time.Second,
		SendPollInterval: 20 * time.Millisecond,
	}, raftcluster.ProxyConfig{}, testLogger())
	runner := factory()

	ctx, cancel := context.WithCancel(context.Background())
	if err := runner.Start(ctx, bot); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancel()

	done := make(chan struct{})
	go func() {
		runner.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return after ctx cancellation — goroutine leak?")
	}
}
