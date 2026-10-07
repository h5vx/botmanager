package raftcluster

import (
	"fmt"
	"sort"
)

// applyUpdateChatMembership records one my_chat_member event into the
// replicated chat registry (ChatMembership: Telegram gives a
// bot no "list my chats" method at all — this registry, built from events
// the bot actually receives, is the only honest source). Idempotent per
// (BotID, ChatID): a later event always overwrites IsMember/ChangedAt for
// that pair — Telegram redelivers updates at-least-once, and the
// newest membership state is always what matters, never a merge of old and
// new. Title is best-effort: an event carrying an empty Title (getChat
// failed at the time) does not erase a previously recorded non-empty one,
// so a transient Telegram hiccup on removal does not blank out the name a
// join already established.
func (f *FSM) applyUpdateChatMembership(c *UpdateChatMembershipCommand) *ApplyResult {
	if c == nil || c.BotID == "" || c.ChatID == 0 {
		return &ApplyResult{Err: fmt.Errorf("%w: update_chat_membership: missing bot_id or chat_id", ErrInvalidCommand)}
	}
	if _, ok := f.state.bots[c.BotID]; !ok {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrBotNotFound, c.BotID)}
	}

	byChat, ok := f.state.chats[c.BotID]
	if !ok {
		byChat = make(map[int64]*ChatMembership)
		f.state.chats[c.BotID] = byChat
	}

	title := c.Title
	if title == "" {
		if existing, ok := byChat[c.ChatID]; ok {
			title = existing.Title
		}
	}

	entry := &ChatMembership{
		BotID:     c.BotID,
		ChatID:    c.ChatID,
		Title:     title,
		IsMember:  c.IsMember,
		ChangedAt: c.ChangedAt,
	}
	byChat[c.ChatID] = entry

	return &ApplyResult{Chat: ptr(entry.Clone())}
}

// ListChatRegistry returns every chat registry entry for botID — both
// currently-a-member and previously-removed — most-recently-changed first.
// Callers deciding "where can I publish right now" (BotAdmin.ListChats)
// filter to IsMember themselves; keeping removed entries here too is what
// lets an operator see "the bot WAS in this chat" rather than the record
// silently vanishing (see the doc comment on ChatMembership).
func (f *FSM) ListChatRegistry(botID string) []ChatMembership {
	f.mu.RLock()
	defer f.mu.RUnlock()

	byChat := f.state.chats[botID]
	out := make([]ChatMembership, 0, len(byChat))
	for _, entry := range byChat {
		out = append(out, entry.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ChangedAt.After(out[j].ChangedAt) })
	return out
}
