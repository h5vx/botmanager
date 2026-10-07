package telegram

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// pollServer serves getUpdates from a fixed list of updates, honoring the
// offset parameter the way Telegram does: only updates with update_id >=
// offset are returned. It records every offset it was asked for.
type pollServer struct {
	mu      sync.Mutex
	updates []map[string]any
	offsets []int64
}

func (p *pollServer) handle(t *testing.T, method string, body map[string]any) (int, map[string]any) {
	switch method {
	case "getUpdates":
		var offset int64
		if v, ok := body["offset"].(float64); ok {
			offset = int64(v)
		}
		p.mu.Lock()
		p.offsets = append(p.offsets, offset)
		var out []map[string]any
		for _, u := range p.updates {
			if int64(u["update_id"].(int)) >= offset {
				out = append(out, u)
			}
		}
		p.mu.Unlock()
		if out == nil {
			time.Sleep(20 * time.Millisecond) // имитация long polling без обновлений
			return http.StatusOK, emptyGetUpdates()
		}
		return http.StatusOK, map[string]any{"ok": true, "result": out}
	case "getChat":
		return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": 900, "title": "Новая группа", "type": "group"}}
	default:
		return http.StatusOK, emptyGetUpdates()
	}
}

func (p *pollServer) seenOffsets() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int64(nil), p.offsets...)
}

func memberUpdate(id int, chatID int64, status string) map[string]any {
	return map[string]any{
		"update_id": id,
		"my_chat_member": map[string]any{
			"chat":            map[string]any{"id": chatID},
			"from":            map[string]any{"id": 1},
			"new_chat_member": map[string]any{"status": status},
		},
	}
}

func messageUpdate(id int, text string) map[string]any {
	return map[string]any{
		"update_id": id,
		"message": map[string]any{
			"message_id": id, "text": text,
			"chat": map[string]any{"id": 555}, "from": map[string]any{"id": 777},
		},
	}
}

// TestPollLoop_RecordsUpdatesWithOffsetAndTitle: a batch is committed as one
// record_updates command carrying the next offset; a join gets its chat
// title from getChat, a kick does not trigger getChat at all.
func TestPollLoop_RecordsUpdatesWithOffsetAndTitle(t *testing.T) {
	ps := &pollServer{updates: []map[string]any{
		messageUpdate(10, "привет"),
		memberUpdate(11, 900, "member"),
		memberUpdate(12, 901, "kicked"),
	}}
	var getChatCalls int
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		if method == "getChat" {
			getChatCalls++
		}
		return ps.handle(t, method, body)
	})

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)
	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, time.Second, func() bool { return len(cluster.appliedRecords()) > 0 })
	rec := cluster.appliedRecords()[0]
	if rec.BotID != "bot-1" || rec.NextOffset != 13 || len(rec.Updates) != 3 {
		t.Fatalf("record = %+v", rec)
	}
	if rec.Updates[0].Kind != raftcluster.JournalIncomingMessage || rec.Updates[0].Text != "привет" {
		t.Fatalf("message update = %+v", rec.Updates[0])
	}
	if rec.Updates[1].ChatTitle != "Новая группа" {
		t.Fatalf("join update title = %q", rec.Updates[1].ChatTitle)
	}
	if rec.Updates[2].ChatTitle != "" || getChatCalls != 1 {
		t.Fatalf("kick update = %+v, getChat calls = %d", rec.Updates[2], getChatCalls)
	}
	// после коммита следующий getUpdates подтверждает пачку offset'ом 13
	waitFor(t, time.Second, func() bool {
		offs := ps.seenOffsets()
		return len(offs) >= 2 && offs[len(offs)-1] == 13
	})
}

// TestPollLoop_StartsFromReplicatedOffset: a runner started on a new
// leader continues from the offset committed by the previous one.
func TestPollLoop_StartsFromReplicatedOffset(t *testing.T) {
	ps := &pollServer{updates: []map[string]any{messageUpdate(5, "old"), messageUpdate(6, "new")}}
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) { return ps.handle(t, method, body) })

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)
	cluster.setOffset("bot-1", 6)
	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, time.Second, func() bool { return len(cluster.appliedRecords()) > 0 })
	if first := ps.seenOffsets()[0]; first != 6 {
		t.Fatalf("first getUpdates offset = %d, want 6", first)
	}
	rec := cluster.appliedRecords()[0]
	if len(rec.Updates) != 1 || rec.Updates[0].Text != "new" {
		t.Fatalf("record = %+v", rec)
	}
}

// TestPollLoop_DoesNotAcknowledgeUncommittedUpdates: while record_updates
// fails, the offset must not advance — Telegram keeps the updates and
// redelivers them once the commit succeeds.
func TestPollLoop_DoesNotAcknowledgeUncommittedUpdates(t *testing.T) {
	ps := &pollServer{updates: []map[string]any{messageUpdate(20, "keep me")}}
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) { return ps.handle(t, method, body) })

	cluster := newFakeCluster()
	cluster.setFailRecords(2)
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)
	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, 5*time.Second, func() bool { return len(cluster.appliedRecords()) > 0 })
	offs := ps.seenOffsets()
	if len(offs) < 3 || offs[0] != 0 || offs[1] != 0 || offs[2] != 0 {
		t.Fatalf("offsets before successful commit = %v, want the first three to be 0", offs)
	}
	if rec := cluster.appliedRecords()[0]; len(rec.Updates) != 1 || rec.Updates[0].Text != "keep me" {
		t.Fatalf("record = %+v", rec)
	}
}
