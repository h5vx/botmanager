package raftcluster

// StateCounts summarises the replicated state for monitoring.
type StateCounts struct {
	BotsByState      map[BotState]int
	MessagesByStatus map[DeliveryStatus]int64
	MessagesTotal    int64
	JournalRetained  int
	JournalLatest    uint64
	ChatsKnown       int
}

// Counts returns StateCounts under a read lock. It walks every retained
// message, which is bounded by retention (bots × message_retention_per_bot).
func (f *FSM) Counts() StateCounts {
	f.mu.RLock()
	defer f.mu.RUnlock()

	c := StateCounts{
		BotsByState:      make(map[BotState]int),
		MessagesByStatus: make(map[DeliveryStatus]int64),
		JournalRetained:  len(f.state.journal),
		JournalLatest:    f.state.nextSeq,
	}
	for _, b := range f.state.bots {
		c.BotsByState[b.State]++
	}
	for _, msgs := range f.state.messages {
		for _, m := range msgs {
			c.MessagesByStatus[m.Delivery.Status]++
			c.MessagesTotal++
		}
	}
	for _, byChat := range f.state.chats {
		c.ChatsKnown += len(byChat)
	}
	return c
}

// Counts returns a summary of this node's copy of the replicated state.
func (n *Node) Counts() StateCounts { return n.fsm.Counts() }

// RaftStats returns hashicorp/raft's own statistics for this node
// ("state", "term", "last_log_index", "commit_index", "applied_index", ...).
func (n *Node) RaftStats() map[string]string { return n.raft.Stats() }
