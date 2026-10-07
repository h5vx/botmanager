package raftcluster

import (
	"fmt"
	"slices"
	"strconv"
	"time"
)

// applyPutMessage records a new outgoing message (Messaging.Send).
//
// Idempotency: a repeated command with an IdempotencyKey already
// present in the index is treated as a no-op that returns the existing
// message unchanged, not an error. This matches the caller's expectation
// exactly: the caller calls Send with an idempotency key and only cares that
// *a* message with that key exists afterwards with some delivery status —
// whether this particular call created it or a previous one did is not
// observable from the result.
//
// Retention (see doc.go): after the new message is appended, trimRetention
// drops the oldest messages for this bot beyond retentionPerBot, in the
// same Apply call — this is what makes the trim a deterministic function
// of the applied command stream rather than a background process that
// could run at different times/orderings on different replicas.
func (f *FSM) applyPutMessage(c *PutMessageCommand) *ApplyResult {
	if c == nil || c.IdempotencyKey == "" || c.BotID == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: put_message: missing idempotency_key or bot_id", ErrInvalidCommand)}
	}
	if _, ok := f.state.bots[c.BotID]; !ok {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrBotNotFound, c.BotID)}
	}
	if existing, ok := f.state.msgIndex[c.IdempotencyKey]; ok {
		return &ApplyResult{Message: ptr(existing.Clone())}
	}

	msg := &Message{
		IdempotencyKey: c.IdempotencyKey,
		BotID:          c.BotID,
		ChatID:         c.ChatID,
		Text:           c.Text,
		Buttons:        slices.Clone(c.Buttons),
		Priority:       c.Priority,
		CreatedAt:      c.CreatedAt,
		Delivery:       DeliveryInfo{Status: DeliveryStatusPending, Retries: 0},
	}
	f.state.messages[c.BotID] = append(f.state.messages[c.BotID], msg)
	f.state.msgIndex[c.IdempotencyKey] = msg
	f.trimRetention(c.BotID)

	return &ApplyResult{Message: ptr(msg.Clone())}
}

// trimRetention drops the oldest messages for botID beyond
// f.retentionPerBot, keeping exactly the most recent retentionPerBot
// (or fewer). Removed messages are also dropped from msgIndex so a later
// idempotency-key lookup does not resurrect a pointer to a message that no
// longer appears in ListMessages/Snapshot output — reusing an evicted key
// after eviction creates a new message rather than being treated as a
// duplicate, an accepted consequence of bounded retention (see doc.go).
func (f *FSM) trimRetention(botID string) {
	msgs := f.state.messages[botID]
	excess := len(msgs) - f.retentionPerBot
	if excess <= 0 {
		return
	}

	for _, dropped := range msgs[:excess] {
		delete(f.state.msgIndex, dropped.IdempotencyKey)
	}

	remaining := make([]*Message, len(msgs)-excess)
	copy(remaining, msgs[excess:])
	f.state.messages[botID] = remaining
}

// applyUpdateDelivery updates the delivery-status fields of an existing
// message (status, retries, last_error, next_retry_at, sent_at).
func (f *FSM) applyUpdateDelivery(c *UpdateDeliveryCommand) *ApplyResult {
	if c == nil || c.IdempotencyKey == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: update_delivery: missing idempotency_key", ErrInvalidCommand)}
	}
	msg, ok := f.state.msgIndex[c.IdempotencyKey]
	if !ok {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrMessageNotFound, c.IdempotencyKey)}
	}

	msg.Delivery.Status = c.Status
	msg.Delivery.Retries = c.Retries
	msg.Delivery.LastError = c.LastError
	msg.Delivery.NextRetryAt = c.NextRetryAt
	msg.Delivery.SentAt = c.SentAt
	if c.MessageID != 0 {
		msg.MessageID = c.MessageID
	}

	return &ApplyResult{Message: ptr(msg.Clone())}
}

// applyCancelPending handles Messaging.CancelPending atomically: the
// PENDING/RETRYING-only check and the transition to DeliveryStatusCancelled
// happen inside this single Apply call, so there is no window between a
// caller reading the message's status and requesting the cancellation in
// which a concurrent send could race it to SENT/FAILED — see
// CommandCancelPending's doc comment in command.go. Already-cancelled is a
// no-op success (retry after a lost response, same idempotent-retry pattern
// as applyDeleteBot); SENT/FAILED are left untouched, and the caller tells
// "actually cancelled" from "already terminal" by checking whether the
// returned Message.Delivery.Status ended up Cancelled.
func (f *FSM) applyCancelPending(c *CancelPendingCommand) *ApplyResult {
	if c == nil || c.IdempotencyKey == "" {
		return &ApplyResult{Err: fmt.Errorf("%w: cancel_pending: missing idempotency_key", ErrInvalidCommand)}
	}
	msg, ok := f.state.msgIndex[c.IdempotencyKey]
	if !ok {
		return &ApplyResult{Err: fmt.Errorf("%w: %s", ErrMessageNotFound, c.IdempotencyKey)}
	}

	switch msg.Delivery.Status {
	case DeliveryStatusPending, DeliveryStatusRetrying:
		msg.Delivery.Status = DeliveryStatusCancelled
		msg.Delivery.LastError = "cancelled before send"
		msg.Delivery.NextRetryAt = time.Time{}
	case DeliveryStatusCancelled:
		// уже отменено — идемпотентный no-op.
	default:
		// SENT/FAILED — терминально в другую сторону, отмена невозможна;
		// оставляем сообщение как есть, вызывающая сторона увидит
		// cancelled = false по итоговому статусу.
	}

	return &ApplyResult{Message: ptr(msg.Clone())}
}

// ListMessagesFilter narrows ListMessages to one bot's history (BotID is
// required — matching ListMessagesRequest.bot_id in the .proto, which is
// not optional either), optionally further narrowed by chat and delivery
// status.
type ListMessagesFilter struct {
	BotID  string
	ChatID int64          // 0 = без фильтра по чату
	Status DeliveryStatus // DeliveryStatusUnspecified = без фильтра по статусу
	// Период по CreatedAt, полуоткрытый интервал [CreatedFrom, CreatedTo)
	// (интервалы времени везде полуоткрытые). Нулевое
	// значение time.Time для любой границы = без этой границы.
	CreatedFrom time.Time
	CreatedTo   time.Time
}

// ListMessages returns one page of a bot's message history, oldest-first
// within the filtered set, alongside an opaque token for the next page
// (empty when there is no more). This is intentionally minimal — an
// integer offset into the filtered slice — sufficient for the future
// Messaging.ListMessages RPC; it is not meant to be a rich query API.
//
// pageSize <= 0 returns every remaining matching message in one page.
func (f *FSM) ListMessages(filter ListMessagesFilter, pageSize int, pageToken string) ([]Message, string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	all := f.state.messages[filter.BotID]
	filtered := make([]Message, 0, len(all))
	for _, m := range all {
		if filter.ChatID != 0 && m.ChatID != filter.ChatID {
			continue
		}
		if filter.Status != DeliveryStatusUnspecified && m.Delivery.Status != filter.Status {
			continue
		}
		if !filter.CreatedFrom.IsZero() && m.CreatedAt.Before(filter.CreatedFrom) {
			continue
		}
		if !filter.CreatedTo.IsZero() && !m.CreatedAt.Before(filter.CreatedTo) {
			continue
		}
		filtered = append(filtered, m.Clone())
	}

	offset := 0
	if pageToken != "" {
		n, err := strconv.Atoi(pageToken)
		if err != nil || n < 0 {
			return nil, "", fmt.Errorf("%w: invalid page_token %q", ErrInvalidCommand, pageToken)
		}
		offset = n
	}
	if offset > len(filtered) {
		offset = len(filtered)
	}

	end := len(filtered)
	if pageSize > 0 && offset+pageSize < end {
		end = offset + pageSize
	}

	page := append([]Message(nil), filtered[offset:end]...)

	nextToken := ""
	if end < len(filtered) {
		nextToken = strconv.Itoa(end)
	}

	return page, nextToken, nil
}

// ListChatIDs returns the distinct chat IDs a bot has exchanged messages
// with, most-recently-active first (source for a chat picker in an admin
// UI). Computed by walking this bot's message history
// backwards and keeping the first (i.e. most recent) occurrence of each
// chat ID — the same storage-side computation ListMessages already does
// for filtering, not a caller-side scan over a page of results.
func (f *FSM) ListChatIDs(botID string) []int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	msgs := f.state.messages[botID]
	seen := make(map[int64]struct{}, len(msgs))
	out := make([]int64, 0, len(msgs))
	for i := len(msgs) - 1; i >= 0; i-- {
		chatID := msgs[i].ChatID
		if _, ok := seen[chatID]; ok {
			continue
		}
		seen[chatID] = struct{}{}
		out = append(out, chatID)
	}
	return out
}

// GetMessage looks up one message by idempotency key, regardless of bot —
// matching Messaging.GetMessage in the .proto.
func (f *FSM) GetMessage(idempotencyKey string) (Message, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	m, ok := f.state.msgIndex[idempotencyKey]
	if !ok {
		return Message{}, false
	}
	return m.Clone(), true
}
