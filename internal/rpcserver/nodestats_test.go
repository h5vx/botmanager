package rpcserver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

func TestMaintenance_GetNodeStats(t *testing.T) {
	node := newTestNode(t)
	botAdmin, messaging, _ := newTestClients(t, node)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "A", Token: "123:a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := botAdmin.SetBotState(ctx, &botmanagerpb.SetBotStateRequest{Id: bot.GetId(), State: botmanagerpb.BotState_BOT_STATE_ENABLED}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2"} {
		if _, err := messaging.Send(ctx, &botmanagerpb.SendRequest{IdempotencyKey: k, BotId: bot.GetId(), ChatId: 1, Text: "x"}); err != nil {
			t.Fatal(err)
		}
	}

	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "raft.bolt"), make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	srv := NewMaintenanceServer(node, raftcluster.ProxyConfig{}, "", 0, nil)
	srv.SetNodeRuntime(NodeRuntime{Version: "1.2.3", StartedAt: started, DataDir: dataDir, RunningBots: func() int { return 1 }})

	st, err := srv.GetNodeStats(ctx, &botmanagerpb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if st.GetNodeId() != "test-node" || st.GetVersion() != "1.2.3" || !st.GetStartedAt().AsTime().Equal(started) {
		t.Fatalf("identity = %+v", st)
	}
	if st.GetRaftState() != "Leader" || st.GetAppliedIndex() == 0 || st.GetLastLogIndex() < st.GetAppliedIndex() {
		t.Fatalf("raft = %s applied=%d last=%d", st.GetRaftState(), st.GetAppliedIndex(), st.GetLastLogIndex())
	}
	if st.GetHeapAllocBytes() == 0 || st.GetSysBytes() == 0 || st.GetGoroutines() == 0 {
		t.Fatalf("memory = %+v", st)
	}
	if st.GetDataDirBytes() != 4096 || st.GetRunningBots() != 1 {
		t.Fatalf("data dir = %d running = %d", st.GetDataDirBytes(), st.GetRunningBots())
	}
	if st.GetBotsTotal() != 2 || st.GetBotsByState()["ENABLED"] != 1 || st.GetBotsByState()["DISABLED"] != 1 {
		t.Fatalf("bots = %d %v", st.GetBotsTotal(), st.GetBotsByState())
	}
	if st.GetMessagesTotal() != 2 || st.GetMessagesByStatus()["PENDING"] != 2 {
		t.Fatalf("messages = %d %v", st.GetMessagesTotal(), st.GetMessagesByStatus())
	}
	if st.GetJournalLatestSequence() == 0 || st.GetJournalRetained() == 0 {
		t.Fatalf("journal = %d/%d", st.GetJournalLatestSequence(), st.GetJournalRetained())
	}
}
