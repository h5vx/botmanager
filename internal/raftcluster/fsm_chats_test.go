package raftcluster

import "testing"

// TestChatMembership_JoinAndKick covers the whole point of the chat registry: a
// bot added to a chat is immediately visible in the registry — even with
// zero messages sent — and a later kick flips IsMember without erasing the
// record (knowing the bot used to be there is useful too).
func TestChatMembership_JoinAndKick(t *testing.T) {
	fsm := NewFSM(10)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	res := mustApply(t, fsm, Command{Type: CommandUpdateChatMembership, UpdateChatMembership: &UpdateChatMembershipCommand{
		BotID: "bot-1", ChatID: 100, Title: "Хор", IsMember: true, ChangedAt: t1(1),
	}})
	if res.Err != nil {
		t.Fatalf("join: %v", res.Err)
	}
	if res.Chat == nil || res.Chat.Title != "Хор" || !res.Chat.IsMember {
		t.Fatalf("join result = %+v", res.Chat)
	}

	reg := fsm.ListChatRegistry("bot-1")
	if len(reg) != 1 || reg[0].ChatID != 100 || !reg[0].IsMember || reg[0].Title != "Хор" {
		t.Fatalf("registry after join = %+v", reg)
	}

	// Kicked — title-less event (getChat is pointless once removed), must
	// not erase the previously known title, and must flip IsMember.
	mustApply(t, fsm, Command{Type: CommandUpdateChatMembership, UpdateChatMembership: &UpdateChatMembershipCommand{
		BotID: "bot-1", ChatID: 100, IsMember: false, ChangedAt: t1(2),
	}})

	reg = fsm.ListChatRegistry("bot-1")
	if len(reg) != 1 {
		t.Fatalf("kick must not delete the record, got %+v", reg)
	}
	if reg[0].IsMember {
		t.Fatalf("IsMember still true after kick")
	}
	if reg[0].Title != "Хор" {
		t.Fatalf("Title = %q, want preserved %q", reg[0].Title, "Хор")
	}
}

// TestChatMembership_OrderAndUnknownBot covers most-recent-first ordering
// across chats and the unknown-bot rejection (mirrors applyPutMessage's
// ErrBotNotFound check).
func TestChatMembership_OrderAndUnknownBot(t *testing.T) {
	fsm := NewFSM(10)
	mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})

	mustApply(t, fsm, Command{Type: CommandUpdateChatMembership, UpdateChatMembership: &UpdateChatMembershipCommand{
		BotID: "bot-1", ChatID: 100, Title: "A", IsMember: true, ChangedAt: t1(1),
	}})
	mustApply(t, fsm, Command{Type: CommandUpdateChatMembership, UpdateChatMembership: &UpdateChatMembershipCommand{
		BotID: "bot-1", ChatID: 200, Title: "B", IsMember: true, ChangedAt: t1(5),
	}})

	reg := fsm.ListChatRegistry("bot-1")
	if len(reg) != 2 || reg[0].ChatID != 200 || reg[1].ChatID != 100 {
		t.Fatalf("order = %+v, want [200, 100] (most recent first)", reg)
	}

	res := mustApply(t, fsm, Command{Type: CommandUpdateChatMembership, UpdateChatMembership: &UpdateChatMembershipCommand{
		BotID: "no-such-bot", ChatID: 1, IsMember: true, ChangedAt: t1(0),
	}})
	if res.Err == nil {
		t.Fatalf("expected error for unknown bot")
	}

	if got := fsm.ListChatRegistry("bot-with-no-chats"); len(got) != 0 {
		t.Fatalf("empty registry, got %+v", got)
	}
}
