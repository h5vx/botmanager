package telegram

import (
	"context"
	"sort"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// sendLoop drains bot.ID's PENDING/RETRYING messages from raftcluster and
// attempts delivery, honoring priority (critical обгоняет normal) and
// per-message backoff. It wakes on cluster.SubscribeApplied() (something
// was applied — cheap to over-trigger, and catches a fresh PENDING message
// immediately) and on a periodic ticker (catches RETRYING messages whose
// NextRetryAt has elapsed without any fresh Apply happening in between).
func (r *Runner) sendLoop(ctx context.Context, bot raftcluster.Bot, api *Client) {
	appliedCh, cancelApplied := r.cluster.SubscribeApplied()
	defer cancelApplied()

	ticker := time.NewTicker(r.cfg.sendPollInterval())
	defer ticker.Stop()

	var rateLimitedUntil time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-appliedCh:
			if !ok {
				return
			}
		case <-ticker.C:
		}

		if time.Now().Before(rateLimitedUntil) {
			continue
		}
		if wait := r.processOutbound(ctx, bot, api); wait > 0 {
			rateLimitedUntil = time.Now().Add(wait)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// processOutbound attempts every currently-ready message for bot once, in
// priority order. It returns a non-zero duration when a rate limit was hit
// (RateLimit → postpone and retry with a local send backoff —
// deliberately NOT written to raftcluster, see package doc's reaction
// table); sendLoop then pauses this bot's sends for that long.
func (r *Runner) processOutbound(ctx context.Context, bot raftcluster.Bot, api *Client) time.Duration {
	for _, msg := range r.readyMessages(bot.ID) {
		if ctx.Err() != nil {
			return 0
		}

		sendCtx, cancel := context.WithTimeout(ctx, r.cfg.requestTimeout())
		result, err := api.SendMessage(sendCtx, msg.ChatID, msg.Text, 0, msg.Buttons)
		cancel()

		if err == nil {
			r.updateDelivery(msg, raftcluster.DeliveryStatusSent, msg.Delivery.Retries, "", time.Time{}, time.Now().UTC(), result.MessageID)
			continue
		}

		class := ClassifyFailure(err)
		switch class {
		case raftcluster.FailureClassBot:
			// Проблема бота, не этого конкретного сообщения — не
			// трогаем его статус, останавливаем раннер целиком (см. doc.go).
			r.reportBroken(bot, class, err.Error())
			return 0

		case raftcluster.FailureClassRateLimit:
			wait := defaultRateLimitWait
			if apiErr, ok := asAPIError(err); ok && apiErr.RetryAfter > 0 {
				wait = apiErr.RetryAfter
			}
			r.logger.Warn("send rate limited",
				"event", "telegram.send_rate_limited", "bot_id", bot.ID, "chat_id", msg.ChatID, "retry_in", wait.String())
			return wait

		case raftcluster.FailureClassRecipient:
			r.logger.Info("message delivery failed permanently",
				"event", "telegram.send_failed_recipient", "bot_id", bot.ID, "chat_id", msg.ChatID)
			r.updateDelivery(msg, raftcluster.DeliveryStatusFailed, msg.Delivery.Retries, err.Error(), time.Time{}, time.Time{}, 0)

		default: // FailureClassNode или FailureClassUnspecified — локальный backoff с повтором
			retries := msg.Delivery.Retries + 1
			if retries > r.cfg.maxSendRetries() {
				r.logger.Warn("message retries exhausted",
					"event", "telegram.send_retries_exhausted", "bot_id", bot.ID, "chat_id", msg.ChatID, "retries", retries)
				r.updateDelivery(msg, raftcluster.DeliveryStatusFailed, retries, err.Error(), time.Time{}, time.Time{}, 0)
				continue
			}
			next := time.Now().UTC().Add(retryDelay(retries))
			r.updateDelivery(msg, raftcluster.DeliveryStatusRetrying, retries, err.Error(), next, time.Time{}, 0)
		}
	}
	return 0
}

// defaultRateLimitWait is used when a 429 response carries no retry_after
// at all (Telegram normally always sets one, but nothing guarantees it).
const defaultRateLimitWait = 30 * time.Second

// readyMessages returns bot.ID's PENDING messages and RETRYING messages
// whose NextRetryAt has elapsed, sorted critical-first
// then oldest-created-first within a priority for fairness.
func (r *Runner) readyMessages(botID string) []raftcluster.Message {
	now := time.Now()
	var ready []raftcluster.Message

	pending, _, err := r.cluster.ListMessages(
		raftcluster.ListMessagesFilter{BotID: botID, Status: raftcluster.DeliveryStatusPending}, 0, "")
	if err == nil {
		ready = append(ready, pending...)
	}

	retrying, _, err := r.cluster.ListMessages(
		raftcluster.ListMessagesFilter{BotID: botID, Status: raftcluster.DeliveryStatusRetrying}, 0, "")
	if err == nil {
		for _, m := range retrying {
			if m.Delivery.NextRetryAt.IsZero() || !m.Delivery.NextRetryAt.After(now) {
				ready = append(ready, m)
			}
		}
	}

	sort.SliceStable(ready, func(i, j int) bool {
		wi, wj := priorityWeight(ready[i].Priority), priorityWeight(ready[j].Priority)
		if wi != wj {
			return wi > wj
		}
		return ready[i].CreatedAt.Before(ready[j].CreatedAt)
	})
	return ready
}

func priorityWeight(p raftcluster.Priority) int {
	if p == raftcluster.PriorityCritical {
		return 1
	}
	return 0
}

func (r *Runner) updateDelivery(msg raftcluster.Message, status raftcluster.DeliveryStatus, retries int, lastError string, nextRetryAt, sentAt time.Time, messageID int64) {
	cmd := raftcluster.Command{
		Type: raftcluster.CommandUpdateDelivery,
		UpdateDelivery: &raftcluster.UpdateDeliveryCommand{
			IdempotencyKey: msg.IdempotencyKey,
			Status:         status,
			Retries:        retries,
			LastError:      lastError,
			NextRetryAt:    nextRetryAt,
			SentAt:         sentAt,
			MessageID:      messageID,
		},
	}
	if _, err := r.cluster.Apply(cmd, r.cfg.requestTimeout()); err != nil {
		r.logger.Warn("update_delivery failed",
			"event", "telegram.update_delivery_failed", "bot_id", msg.BotID, "status", status.String(), "error", err.Error())
	}
}

// retryDelay is a simple exponential backoff for message send retries,
// separate from the connection-level backoff type (backoff.go) — capped at
// 5 minutes so a persistently flaky send does not wait indefinitely between
// attempts.
func retryDelay(retries int) time.Duration {
	d := time.Second * time.Duration(1<<uint(min(retries, 8)))
	const capDelay = 5 * time.Minute
	if d > capDelay {
		d = capDelay
	}
	return d
}
