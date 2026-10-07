package telegram

import (
	"net/http"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// TestRunner_ChatMemberJoin_RecordsMembershipWithTitle covers the chat
// registry's core requirement end to end through Runner.pollLoop: a my_chat_member
// "added to a group" update must produce a CommandUpdateChatMembership
// Apply call carrying a title fetched via getChat — before a single
// message has ever been exchanged with that chat.
func TestRunner_ChatMemberJoin_RecordsMembershipWithTitle(t *testing.T) {
	var pollCount int
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		switch method {
		case "getUpdates":
			pollCount++
			if pollCount > 1 {
				return http.StatusOK, emptyGetUpdates()
			}
			return http.StatusOK, map[string]any{
				"ok": true,
				"result": []map[string]any{
					{
						"update_id": 1,
						"my_chat_member": map[string]any{
							"chat":            map[string]any{"id": 900},
							"from":            map[string]any{"id": 1},
							"new_chat_member": map[string]any{"status": "member"},
						},
					},
				},
			}
		case "getChat":
			return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"id": 900, "title": "Новая группа", "type": "group"}}
		default:
			t.Fatalf("unexpected method %q", method)
			return 0, nil
		}
	})

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)

	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, time.Second, func() bool { return len(cluster.appliedChatMemberships()) > 0 })

	got := cluster.appliedChatMemberships()
	if len(got) != 1 {
		t.Fatalf("got %d chat membership applies, want 1: %+v", len(got), got)
	}
	if got[0].ChatID != 900 || !got[0].IsMember || got[0].Title != "Новая группа" {
		t.Fatalf("applied membership = %+v", got[0])
	}
}

// TestRunner_ChatMemberKicked_RecordsNotMemberWithoutTitleLookup covers
// removal: no getChat call is made for a "left"/"kicked" status (title is
// pointless once the bot can no longer see the chat), IsMember is false.
func TestRunner_ChatMemberKicked_RecordsNotMemberWithoutTitleLookup(t *testing.T) {
	var pollCount int
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		switch method {
		case "getUpdates":
			pollCount++
			if pollCount > 1 {
				return http.StatusOK, emptyGetUpdates()
			}
			return http.StatusOK, map[string]any{
				"ok": true,
				"result": []map[string]any{
					{
						"update_id": 1,
						"my_chat_member": map[string]any{
							"chat":            map[string]any{"id": 901},
							"from":            map[string]any{"id": 1},
							"new_chat_member": map[string]any{"status": "kicked"},
						},
					},
				},
			}
		case "getChat":
			t.Fatalf("getChat must not be called for a kicked/left event")
			return 0, nil
		default:
			t.Fatalf("unexpected method %q", method)
			return 0, nil
		}
	})

	cluster := newFakeCluster()
	bot := raftcluster.Bot{ID: "bot-1", Token: validTestToken, State: raftcluster.BotStateEnabled}
	cluster.addBot(bot)

	startTestRunner(t, cluster, srv.URL, bot)

	waitFor(t, time.Second, func() bool { return len(cluster.appliedChatMemberships()) > 0 })

	got := cluster.appliedChatMemberships()
	if len(got) != 1 || got[0].ChatID != 901 || got[0].IsMember {
		t.Fatalf("applied membership = %+v", got)
	}
}
