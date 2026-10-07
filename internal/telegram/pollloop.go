package telegram

import (
	"context"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// pollLoop runs getUpdates in a loop until ctx is cancelled.
//
// Delivery guarantee: every batch of updates is first committed to the
// replicated journal (CommandRecordUpdates, together with the next
// offset) and only then acknowledged to Telegram — acknowledgement happens
// implicitly, by passing the higher offset to the next getUpdates call. If
// the commit fails (lost leadership, cluster unavailable) the offset does
// not move, so Telegram redelivers the same updates to whichever node polls
// next; updates already committed are dropped by the FSM as duplicates.
// The starting offset is read from the replicated state, so a new leader
// continues exactly where the old one stopped. Subscribers therefore never
// miss an update, and see each one once (Update.sequence).
func (r *Runner) pollLoop(ctx context.Context, bot raftcluster.Bot, api *Client) {
	offset := r.cluster.PollOffset(bot.ID)
	retry := newBackoff()

	for {
		if ctx.Err() != nil {
			return
		}

		pollCtx, cancel := context.WithTimeout(ctx, r.cfg.longPollTimeout()+5*time.Second)
		updates, err := api.GetUpdates(pollCtx, offset, int(r.cfg.longPollTimeout().Seconds()), AllowedUpdateKinds)
		cancel()
		if ctx.Err() == nil {
			r.observe(bot.ID, err)
		}

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			class := ClassifyFailure(err)
			if class == raftcluster.FailureClassBot {
				r.reportBroken(bot, class, err.Error())
				return
			}

			wait := retry.next()
			if class == raftcluster.FailureClassRateLimit {
				if apiErr, ok := asAPIError(err); ok && apiErr.RetryAfter > 0 {
					wait = apiErr.RetryAfter
				}
			}
			r.logger.Warn("getUpdates failed",
				"event", "telegram.poll_error", "bot_id", bot.ID, "failure_class", class.String(), "retry_in", wait.String())

			if !sleepCtx(ctx, wait) {
				return
			}
			continue
		}
		if len(updates) == 0 {
			retry.reset()
			continue
		}

		next := offset
		batch := make([]raftcluster.IncomingUpdate, 0, len(updates))
		for _, u := range updates {
			if u.UpdateID >= next {
				next = u.UpdateID + 1
			}
			if in, ok := convertUpdate(u); ok {
				if in.Kind == raftcluster.JournalChatMemberChanged && raftcluster.ChatMemberStatusIsMember(in.NewChatMemberStatus) {
					in.ChatTitle = r.chatTitle(ctx, api, in.ChatID)
				}
				batch = append(batch, in)
			}
		}

		cmd := raftcluster.Command{
			Type:          raftcluster.CommandRecordUpdates,
			RecordUpdates: &raftcluster.RecordUpdatesCommand{BotID: bot.ID, NextOffset: next, Updates: batch},
		}
		if _, err := r.cluster.Apply(cmd, r.cfg.requestTimeout()); err != nil {
			// Обновления не подтверждены Telegram и не потеряются: их
			// получит этот же раннер при повторе или новый лидер. При
			// потере лидерства раннер остановит Manager (через ctx), а
			// при сорвавшейся передаче лидерства опрос должен продолжиться.
			wait := retry.next()
			r.logger.Warn("record_updates failed",
				"event", "telegram.record_updates_error", "bot_id", bot.ID, "updates", len(updates), "retry_in", wait.String(), "error", err.Error())
			if !sleepCtx(ctx, wait) {
				return
			}
			continue
		}
		retry.reset()
		offset = next
	}
}

// chatTitle fetches a chat's title with one best-effort getChat call, so a
// chat the bot was just added to carries a human-readable name in the chat
// registry. A failed lookup returns "" (the FSM keeps any previously known
// title).
func (r *Runner) chatTitle(ctx context.Context, api *Client, chatID int64) string {
	tctx, cancel := context.WithTimeout(ctx, r.cfg.requestTimeout())
	defer cancel()
	chat, err := api.GetChat(tctx, chatID)
	if err != nil {
		return ""
	}
	return chat.Title
}

// convertUpdate turns one raw Telegram update into a raftcluster
// IncomingUpdate, or reports ok=false for a kind we did not request/do not
// handle (the offset still advances past it).
func convertUpdate(u apiUpdate) (raftcluster.IncomingUpdate, bool) {
	now := time.Now().UTC()

	switch {
	case u.Message != nil:
		var fromID int64
		if u.Message.From != nil {
			fromID = u.Message.From.ID
		}
		return raftcluster.IncomingUpdate{
			Kind:       raftcluster.JournalIncomingMessage,
			UpdateID:   u.UpdateID,
			ChatID:     u.Message.Chat.ID,
			FromUserID: fromID,
			MessageID:  u.Message.MessageID,
			Text:       u.Message.Text,
			ReceivedAt: now,
		}, true

	case u.CallbackQuery != nil:
		ev := raftcluster.IncomingUpdate{
			Kind:            raftcluster.JournalCallbackQuery,
			UpdateID:        u.UpdateID,
			FromUserID:      u.CallbackQuery.From.ID,
			CallbackQueryID: u.CallbackQuery.ID,
			CallbackData:    u.CallbackQuery.Data,
			ReceivedAt:      now,
		}
		if u.CallbackQuery.Message != nil {
			ev.ChatID = u.CallbackQuery.Message.Chat.ID
			ev.MessageID = u.CallbackQuery.Message.MessageID
		}
		return ev, true

	case u.MyChatMember != nil:
		return raftcluster.IncomingUpdate{
			Kind:                raftcluster.JournalChatMemberChanged,
			UpdateID:            u.UpdateID,
			ChatID:              u.MyChatMember.Chat.ID,
			FromUserID:          u.MyChatMember.From.ID,
			NewChatMemberStatus: u.MyChatMember.NewChatMember.Status,
			ReceivedAt:          now,
		}, true

	default:
		// Вид обновления, который мы не запрашивали (allowed_updates) или
		// не умеем разбирать — пропускаем, не записываем и не считаем
		// ошибкой.
		return raftcluster.IncomingUpdate{}, false
	}
}
