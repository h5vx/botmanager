package telegram

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

type recordingHealth struct {
	mu                     sync.Mutex
	reachable, unreachable int
	stopped                []string
}

func (h *recordingHealth) TelegramReachable(string) { h.mu.Lock(); h.reachable++; h.mu.Unlock() }
func (h *recordingHealth) TelegramUnreachable(string, error) {
	h.mu.Lock()
	h.unreachable++
	h.mu.Unlock()
}
func (h *recordingHealth) BotStopped(id string) {
	h.mu.Lock()
	h.stopped = append(h.stopped, id)
	h.mu.Unlock()
}
func (h *recordingHealth) counts() (int, int, []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reachable, h.unreachable, append([]string(nil), h.stopped...)
}

// TestRunner_ReportsTelegramReachability: any answer from Telegram, even an
// error answer, counts as reachable; no answer at all counts as
// unreachable; stopping the runner reports BotStopped.
func TestRunner_ReportsTelegramReachability(t *testing.T) {
	srv := newTestServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		if method == "getUpdates" {
			return http.StatusBadRequest, map[string]any{"ok": false, "error_code": 400, "description": "Bad Request"}
		}
		return http.StatusOK, emptyGetUpdates()
	})
	health := &recordingHealth{}
	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)

	runner := NewRunnerFactory(cluster, Config{APIBaseURL: srv.URL, RequestTimeout: time.Second, LongPollTimeout: time.Second, Health: health}, raftcluster.ProxyConfig{}, testLogger())()
	if err := runner.Start(t.Context(), bot); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { r, _, _ := health.counts(); return r > 0 })
	runner.Stop()
	if _, u, stopped := health.counts(); u != 0 || len(stopped) != 1 || stopped[0] != "bot-1" {
		t.Fatalf("unreachable=%d stopped=%v", u, stopped)
	}

	// Сервер недоступен вовсе — проблема узла.
	srv.Close()
	health2 := &recordingHealth{}
	runner2 := NewRunnerFactory(cluster, Config{APIBaseURL: srv.URL, RequestTimeout: time.Second, LongPollTimeout: time.Second, Health: health2}, raftcluster.ProxyConfig{}, testLogger())()
	if err := runner2.Start(t.Context(), bot); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { _, u, _ := health2.counts(); return u > 0 })
	runner2.Stop()
	if r, _, _ := health2.counts(); r != 0 {
		t.Fatalf("reachable reported for a closed server: %d", r)
	}
}
