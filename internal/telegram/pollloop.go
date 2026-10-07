package telegram

import (
	"context"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// pollLoop runs getUpdates in a loop until ctx is cancelled. Each received update is converted and published to
// r.bus — see convertUpdate. offset starts at 0 ("no updates confirmed
// yet") and advances to the highest seen update_id + 1 after each
// successful batch — Telegram's own at-least-once acknowledgement
// mechanism, matching the at-least-once/dedup-by-update_id contract
// promised downstream.
func (r *Runner) pollLoop(ctx context.Context, bot raftcluster.Bot, api *Client) {
	var offset int64
	retry := newBackoff()

	for {
		if ctx.Err() != nil {
			return
		}

		pollCtx, cancel := context.WithTimeout(ctx, r.cfg.longPollTimeout()+5*time.Second)
		updates, err := api.GetUpdates(pollCtx, offset, int(r.cfg.longPollTimeout().Seconds()), AllowedUpdateKinds)
		cancel()

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
		retry.reset()

		for _, u := range updates {
			if u.UpdateID >= offset {
				offset = u.UpdateID + 1
			}
			if ev, ok := convertUpdate(bot.ID, u); ok {
				r.bus.Publish(ev)
				if ev.Kind == UpdateKindChatMemberChanged {
					r.recordChatMembership(ctx, bot, api, ev)
				}
			}
		}
	}
}

// recordChatMembership persists one my_chat_member event into the
// replicated chat registry (raftcluster.ChatMembership) —
// alongside publishing it to r.bus above, from the same source event. A
// join (IsMember true) fetches the chat's title with one best-effort
// getChat call under the request timeout, so a chat the bot was just added
// to already carries a human-readable name in the registry, not just a
// bare id; a failed lookup leaves Title empty rather than blocking the
// membership record itself (applyUpdateChatMembership fills it from any
// previously known title instead). Apply failures are logged and
// swallowed, not fatal to the poll loop — the live Update already reached
// r.bus regardless, and a follower losing leadership mid-Apply is the
// normal "no longer leader, this bot's runner is about to be stopped"
// case, not a bug.
func (r *Runner) recordChatMembership(ctx context.Context, bot raftcluster.Bot, api *Client, ev Update) {
	isMember := chatMemberStatusIsMember(ev.NewChatMemberStatus)

	title := ""
	if isMember {
		tctx, cancel := context.WithTimeout(ctx, r.cfg.requestTimeout())
		if chat, err := api.GetChat(tctx, ev.ChatID); err == nil {
			title = chat.Title
		}
		cancel()
	}

	cmd := raftcluster.Command{
		Type: raftcluster.CommandUpdateChatMembership,
		UpdateChatMembership: &raftcluster.UpdateChatMembershipCommand{
			BotID:     bot.ID,
			ChatID:    ev.ChatID,
			Title:     title,
			IsMember:  isMember,
			ChangedAt: ev.ReceivedAt,
		},
	}
	if _, err := r.cluster.Apply(cmd, r.cfg.requestTimeout()); err != nil {
		r.logger.Warn("update_chat_membership failed",
			"event", "telegram.chat_membership_apply_error", "bot_id", bot.ID, "chat_id", ev.ChatID, "error", err.Error())
	}
}

// chatMemberStatusIsMember mirrors rpcserver's chatMemberRights reading of
// Telegram's my_chat_member.new_chat_member.status: "left"/"kicked" mean
// the bot is no longer a member, any other non-empty status means it is.
func chatMemberStatusIsMember(status string) bool {
	switch status {
	case "left", "kicked":
		return false
	default:
		return status != ""
	}
}

// convertUpdate turns one raw Telegram update into an Update for the bus,
// or reports ok=false for a kind we did not request/do not handle (offset
// advancement in pollLoop already happened by update_id regardless).
func convertUpdate(botID string, u apiUpdate) (Update, bool) {
	now := time.Now().UTC()

	switch {
	case u.Message != nil:
		var fromID int64
		if u.Message.From != nil {
			fromID = u.Message.From.ID
		}
		return Update{
			Kind:       UpdateKindIncomingMessage,
			BotID:      botID,
			UpdateID:   u.UpdateID,
			ChatID:     u.Message.Chat.ID,
			FromUserID: fromID,
			MessageID:  u.Message.MessageID,
			Text:       u.Message.Text,
			ReceivedAt: now,
		}, true

	case u.CallbackQuery != nil:
		ev := Update{
			Kind:            UpdateKindCallbackQuery,
			BotID:           botID,
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
		return Update{
			Kind:                UpdateKindChatMemberChanged,
			BotID:               botID,
			UpdateID:            u.UpdateID,
			ChatID:              u.MyChatMember.Chat.ID,
			FromUserID:          u.MyChatMember.From.ID,
			NewChatMemberStatus: u.MyChatMember.NewChatMember.Status,
			ReceivedAt:          now,
		}, true

	default:
		// Вид обновления, который мы не запрашивали (allowed_updates) или
		// не умеем разбирать — пропускаем, не публикуем и не считаем
		// ошибкой.
		return Update{}, false
	}
}
