package rpcserver

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/h5vx/botmanager/internal/raftcluster"
	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
)

// maxSendButtons — разумный предел числа кнопок в одном ряду (один ряд,
// без матрицы). Telegram сам ограничивает
// ширину клавиатуры устройством пользователя; это не попытка угадать его
// точный лимит, а защита от заведомо нерабочего запроса ещё на границе
// доверия.
const maxSendButtons = 8

// maxCallbackDataBytes — Telegram's own limit on callback_data (documented
// as 1-64 bytes). Checked here rather than left to Telegram to reject: by
// the time Telegram's answer comes back, the message is already in
// raftcluster and the caller has moved on — a rejection here is cheaper to
// diagnose.
const maxCallbackDataBytes = 64

// buttonsFromProto validates and converts SendRequest.buttons — the trust
// boundary for Messaging.Send/SendBatch (input shape, not auth).
// Rejected with codes.InvalidArgument rather than silently
// dropping a malformed button: a caller that thinks it attached a working
// link button should not get a text-only message with no explanation.
//
// Ровно одно из двух — url или callback_data — должно быть заполнено
// (в группах работает обычная URL-кнопка, web_app — нет):
// обе пустые — кнопке некуда вести, обе заданные — Telegram отклонит саму
// кнопку с непонятной для вызывающего ошибкой уже после отправки.
func buttonsFromProto(buttons []*botmanagerpb.InlineButton) ([]raftcluster.Button, error) {
	if len(buttons) == 0 {
		return nil, nil
	}
	if len(buttons) > maxSendButtons {
		return nil, status.Errorf(codes.InvalidArgument, "too many buttons: %d (max %d)", len(buttons), maxSendButtons)
	}
	out := make([]raftcluster.Button, len(buttons))
	for i, b := range buttons {
		text := b.GetText()
		url := b.GetUrl()
		callbackData := b.GetCallbackData()
		if text == "" {
			return nil, status.Errorf(codes.InvalidArgument, "button %d: text is required", i)
		}
		hasURL := url != ""
		hasCallback := callbackData != ""
		if hasURL == hasCallback {
			return nil, status.Errorf(codes.InvalidArgument, "button %d: exactly one of url or callback_data is required", i)
		}
		if hasURL && !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			return nil, status.Errorf(codes.InvalidArgument, "button %d: url must start with http:// or https://", i)
		}
		if hasCallback && len(callbackData) > maxCallbackDataBytes {
			return nil, status.Errorf(codes.InvalidArgument, "button %d: callback_data is %d bytes, max %d", i, len(callbackData), maxCallbackDataBytes)
		}
		out[i] = raftcluster.Button{Text: text, URL: url, CallbackData: callbackData}
	}
	return out, nil
}

// toProtoTime bridges raftcluster's time.Time (zero value = "unset", e.g.
// Message.Delivery.SentAt before SENT) to protobuf's *timestamppb.Timestamp
// (nil = unset field). Explicit conversion rather than a raw cast, per root
// CLAUDE.md "явное вместо магии" — and because the zero-value conventions
// differ (Go zero time vs. nil pointer).
func toProtoTime(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// timeFromProtoOrZero is toProtoTime's counterpart, needed since
// ListMessagesRequest.created_from/created_to (period
// filter, applied inside raftcluster.FSM.ListMessages, not over an already
// fetched page) are the first caller-supplied timestamps in this .proto.
// A nil field (not set by the caller) becomes the Go zero time.Time — the
// same "unset" convention ListMessagesFilter already uses.
func timeFromProtoOrZero(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// timestamppbNow stamps Update.occurred_at for events converted from
// raftcluster.Event (messaging.go's fsmEventToProto): FSM.Event carries no
// "when" of its own (Command/Bot/Message only — see fsm.go), and Bot/
// Message have no generic "last modified" field that would cover every
// command kind uniformly (Bot.UpdatedAt exists but Message does not have an
// equivalent). Subscribe's consumer sees each event promptly after Apply
// commits it, so "now" at conversion time is an accurate-enough
// approximation of "when this happened" for a live stream — unlike
// toProtoTime, which threads an actual recorded timestamp through.
func timestamppbNow() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().UTC())
}

// --- enums ---
//
// raftcluster's BotState/Priority/DeliveryStatus/FailureClass and their
// botmanagerpb counterparts share the same iota ordering by design (see
// the doc comments on each raftcluster type), but conversion goes through
// an explicit switch rather than a numeric cast: a cast would silently
// keep compiling if the two enums ever drift apart (a new value added to
// one but not the other), while a switch's default case fails loudly.

func botStateToProto(s raftcluster.BotState) botmanagerpb.BotState {
	switch s {
	case raftcluster.BotStateDisabled:
		return botmanagerpb.BotState_BOT_STATE_DISABLED
	case raftcluster.BotStateEnabled:
		return botmanagerpb.BotState_BOT_STATE_ENABLED
	case raftcluster.BotStateBroken:
		return botmanagerpb.BotState_BOT_STATE_BROKEN
	case raftcluster.BotStateDeleted:
		return botmanagerpb.BotState_BOT_STATE_DELETED
	default:
		return botmanagerpb.BotState_BOT_STATE_UNSPECIFIED
	}
}

func priorityToProto(p raftcluster.Priority) botmanagerpb.Priority {
	switch p {
	case raftcluster.PriorityNormal:
		return botmanagerpb.Priority_PRIORITY_NORMAL
	case raftcluster.PriorityCritical:
		return botmanagerpb.Priority_PRIORITY_CRITICAL
	default:
		return botmanagerpb.Priority_PRIORITY_UNSPECIFIED
	}
}

func priorityFromProto(p botmanagerpb.Priority) raftcluster.Priority {
	switch p {
	case botmanagerpb.Priority_PRIORITY_NORMAL:
		return raftcluster.PriorityNormal
	case botmanagerpb.Priority_PRIORITY_CRITICAL:
		return raftcluster.PriorityCritical
	default:
		return raftcluster.PriorityUnspecified
	}
}

func deliveryStatusToProto(s raftcluster.DeliveryStatus) botmanagerpb.DeliveryStatus {
	switch s {
	case raftcluster.DeliveryStatusPending:
		return botmanagerpb.DeliveryStatus_DELIVERY_STATUS_PENDING
	case raftcluster.DeliveryStatusRetrying:
		return botmanagerpb.DeliveryStatus_DELIVERY_STATUS_RETRYING
	case raftcluster.DeliveryStatusSent:
		return botmanagerpb.DeliveryStatus_DELIVERY_STATUS_SENT
	case raftcluster.DeliveryStatusFailed:
		return botmanagerpb.DeliveryStatus_DELIVERY_STATUS_FAILED
	case raftcluster.DeliveryStatusCancelled:
		return botmanagerpb.DeliveryStatus_DELIVERY_STATUS_CANCELLED
	default:
		return botmanagerpb.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED
	}
}

func deliveryStatusFromProto(s botmanagerpb.DeliveryStatus) raftcluster.DeliveryStatus {
	switch s {
	case botmanagerpb.DeliveryStatus_DELIVERY_STATUS_PENDING:
		return raftcluster.DeliveryStatusPending
	case botmanagerpb.DeliveryStatus_DELIVERY_STATUS_RETRYING:
		return raftcluster.DeliveryStatusRetrying
	case botmanagerpb.DeliveryStatus_DELIVERY_STATUS_SENT:
		return raftcluster.DeliveryStatusSent
	case botmanagerpb.DeliveryStatus_DELIVERY_STATUS_FAILED:
		return raftcluster.DeliveryStatusFailed
	case botmanagerpb.DeliveryStatus_DELIVERY_STATUS_CANCELLED:
		return raftcluster.DeliveryStatusCancelled
	default:
		return raftcluster.DeliveryStatusUnspecified
	}
}

func failureClassToProto(c raftcluster.FailureClass) botmanagerpb.FailureClass {
	switch c {
	case raftcluster.FailureClassNode:
		return botmanagerpb.FailureClass_FAILURE_CLASS_NODE
	case raftcluster.FailureClassBot:
		return botmanagerpb.FailureClass_FAILURE_CLASS_BOT
	case raftcluster.FailureClassRateLimit:
		return botmanagerpb.FailureClass_FAILURE_CLASS_RATE_LIMIT
	case raftcluster.FailureClassRecipient:
		return botmanagerpb.FailureClass_FAILURE_CLASS_RECIPIENT
	default:
		return botmanagerpb.FailureClass_FAILURE_CLASS_UNSPECIFIED
	}
}

// --- ProxyConfig ---

func proxyToProto(p *raftcluster.ProxyConfig) *botmanagerpb.ProxyConfig {
	if p == nil {
		return nil
	}
	return &botmanagerpb.ProxyConfig{Enabled: p.Enabled, Address: p.Address}
}

func proxyFromProto(p *botmanagerpb.ProxyConfig) *raftcluster.ProxyConfig {
	if p == nil {
		return nil
	}
	return &raftcluster.ProxyConfig{Enabled: p.GetEnabled(), Address: p.GetAddress()}
}

// --- Bot ---

// botToProto converts a raftcluster.Bot to botmanagerpb.Bot. Bot.Token is
// deliberately never read here — the token is never returned to callers.
func botToProto(b raftcluster.Bot) *botmanagerpb.Bot {
	return &botmanagerpb.Bot{
		Id:                b.ID,
		DisplayName:       b.DisplayName,
		Username:          b.Username,
		State:             botStateToProto(b.State),
		Proxy:             proxyToProto(b.Proxy),
		CreatedAt:         toProtoTime(b.CreatedAt),
		UpdatedAt:         toProtoTime(b.UpdatedAt),
		LastFailureClass:  failureClassToProto(b.LastFailureClass),
		LastFailureReason: b.LastFailureReason,
	}
}

// --- Message / DeliveryInfo ---

func deliveryToProto(d raftcluster.DeliveryInfo) *botmanagerpb.DeliveryInfo {
	return &botmanagerpb.DeliveryInfo{
		Status:      deliveryStatusToProto(d.Status),
		Retries:     int32(d.Retries),
		LastError:   d.LastError,
		NextRetryAt: toProtoTime(d.NextRetryAt),
		SentAt:      toProtoTime(d.SentAt),
	}
}

func messageToProto(m raftcluster.Message) *botmanagerpb.Message {
	return &botmanagerpb.Message{
		IdempotencyKey: m.IdempotencyKey,
		BotId:          m.BotID,
		ChatId:         m.ChatID,
		MessageId:      m.MessageID,
		Text:           m.Text,
		Delivery:       deliveryToProto(m.Delivery),
		Priority:       priorityToProto(m.Priority),
		CreatedAt:      toProtoTime(m.CreatedAt),
	}
}

// --- chat membership ---

// chatMemberRights derives GetChat's bot_is_member/bot_can_pin/
// bot_can_send from a getChatMember result for the bot's own membership.
// Telegram's documented statuses: "creator", "administrator", "member",
// "restricted", "left", "kicked" — "left"/"kicked" mean the bot is not in
// the chat at all, everything else means it is.
//
// "restricted" carries Telegram's own can_send_messages flag, which
// internal/telegram's ChatMember (wire.go) does not currently parse — we
// treat a restricted bot as unable to send rather than guessing true, since
// a false negative here just means GetChat under-reports a capability
// (caller can still try and get a clear error back), while a false positive
// would claim a capability that then fails at send time.
func chatMemberRights(status string, canPinMessages bool) (isMember, canPin, canSend bool) {
	switch status {
	case "creator":
		return true, true, true
	case "administrator":
		return true, canPinMessages, true
	case "member":
		return true, false, true
	case "restricted":
		return true, false, false
	default: // "left", "kicked", or an unrecognized future status
		return false, false, false
	}
}

// --- pagination ---
//
// raftcluster.Node.ListBots has no pagination (see CLAUDE.md) —
// BotAdmin.ListBots slices the full (already ID-sorted) list here, using
// the same "integer offset as opaque page_token" scheme
// raftcluster.FSM.ListMessages already uses for Messaging.ListMessages, so
// both RPCs behave identically from a caller's point of view.

func paginateBots(all []raftcluster.Bot, pageSize int, pageToken string) ([]raftcluster.Bot, string, error) {
	offset := 0
	if pageToken != "" {
		n, err := strconv.Atoi(pageToken)
		if err != nil || n < 0 {
			return nil, "", fmt.Errorf("invalid page_token %q", pageToken)
		}
		offset = n
	}
	if offset > len(all) {
		offset = len(all)
	}

	end := len(all)
	if pageSize > 0 && offset+pageSize < end {
		end = offset + pageSize
	}

	page := append([]raftcluster.Bot(nil), all[offset:end]...)

	next := ""
	if end < len(all) {
		next = strconv.Itoa(end)
	}
	return page, next, nil
}
