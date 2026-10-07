package rpcserver

import (
	"context"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/h5vx/botmanager/api/botmanagerpb"
)

// NodeRuntime is what GetNodeStats reports about the process itself, beyond
// what raftcluster.Node knows. Set it with SetNodeRuntime; zero values are
// reported as empty.
type NodeRuntime struct {
	Version   string
	StartedAt time.Time
	// DataDir is measured on every call (journal and snapshots on disk).
	DataDir string
	// RunningBots reports how many bot runners this node runs right now.
	RunningBots func() int
}

// SetNodeRuntime configures the process information GetNodeStats returns.
func (s *MaintenanceServer) SetNodeRuntime(rt NodeRuntime) { s.runtime = rt }

// GetNodeStats reports this node's memory, Raft position and a summary of
// its copy of the replicated state. It is never forwarded: to see another
// node, call that node.
func (s *MaintenanceServer) GetNodeStats(context.Context, *botmanagerpb.Empty) (*botmanagerpb.NodeStats, error) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	raftStats := s.node.RaftStats()
	counts := s.node.Counts()

	out := &botmanagerpb.NodeStats{
		NodeId:                s.node.ID(),
		Version:               s.runtime.Version,
		RaftState:             raftStats["state"],
		RaftTerm:              parseUint(raftStats["term"]),
		LastLogIndex:          parseUint(raftStats["last_log_index"]),
		CommitIndex:           parseUint(raftStats["commit_index"]),
		AppliedIndex:          parseUint(raftStats["applied_index"]),
		HeapAllocBytes:        mem.HeapAlloc,
		HeapSysBytes:          mem.HeapSys,
		SysBytes:              mem.Sys,
		Goroutines:            int32(runtime.NumGoroutine()),
		BotsByState:           make(map[string]int32),
		MessagesTotal:         counts.MessagesTotal,
		MessagesByStatus:      make(map[string]int64),
		JournalLatestSequence: counts.JournalLatest,
		JournalRetained:       int32(counts.JournalRetained),
		ChatsKnown:            int32(counts.ChatsKnown),
	}
	if !s.runtime.StartedAt.IsZero() {
		out.StartedAt = toProtoTime(s.runtime.StartedAt)
	}
	if s.runtime.RunningBots != nil {
		out.RunningBots = int32(s.runtime.RunningBots())
	}
	if s.runtime.DataDir != "" {
		out.DataDirBytes = dirSize(s.runtime.DataDir)
	}
	for state, n := range counts.BotsByState {
		out.BotsByState[strings.TrimPrefix(botStateToProto(state).String(), "BOT_STATE_")] = int32(n)
		out.BotsTotal += int32(n)
	}
	for st, n := range counts.MessagesByStatus {
		out.MessagesByStatus[strings.TrimPrefix(deliveryStatusToProto(st).String(), "DELIVERY_STATUS_")] = n
	}
	return out, nil
}

func parseUint(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

// dirSize sums regular file sizes under dir; unreadable entries are skipped.
func dirSize(dir string) uint64 {
	var total uint64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				total += uint64(info.Size())
			}
		}
		return nil
	})
	return total
}
