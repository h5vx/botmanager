package raftcluster

import (
	"slices"
	"time"
)

// BotState — жизненный цикл бота: disabled → enabled → broken/deleted, и обратно enabled → disabled.
// Значения соответствуют botmanagerpb.BotState по смыслу (см.
// proto/botmanager.proto), но это самостоятельный тип — raftcluster не
// зависит от сгенерированного protobuf-пакета, конвертация между ними —
// забота internal/rpcserver.
type BotState int

const (
	BotStateUnspecified BotState = iota
	BotStateDisabled
	BotStateEnabled
	BotStateBroken
	BotStateDeleted
)

func (s BotState) String() string {
	switch s {
	case BotStateDisabled:
		return "disabled"
	case BotStateEnabled:
		return "enabled"
	case BotStateBroken:
		return "broken"
	case BotStateDeleted:
		return "deleted"
	default:
		return "unspecified"
	}
}

// FailureClass — классификация сбоя (проблема узла / проблема бота /
// ограничение частоты / проблема адресата). Здесь значение только
// хранится рядом с ботом (last_failure_class) или сообщением (в last_error
// текстом) — сама классификация и реакция на неё реализуются в
// internal/telegram.
type FailureClass int

const (
	FailureClassUnspecified FailureClass = iota
	FailureClassNode
	FailureClassBot
	FailureClassRateLimit
	FailureClassRecipient
)

func (c FailureClass) String() string {
	switch c {
	case FailureClassNode:
		return "node"
	case FailureClassBot:
		return "bot"
	case FailureClassRateLimit:
		return "rate_limit"
	case FailureClassRecipient:
		return "recipient"
	default:
		return "unspecified"
	}
}

// Priority — приоритет исходящего сообщения (critical обгоняет
// normal при упоре в лимит Telegram). Сама приоритизация очереди — задача
// internal/telegram; здесь приоритет только сохраняется вместе с
// сообщением.
type Priority int

const (
	PriorityUnspecified Priority = iota
	PriorityNormal
	PriorityCritical
)

// DeliveryStatus — статус доставки сообщения.
type DeliveryStatus int

const (
	DeliveryStatusUnspecified DeliveryStatus = iota
	DeliveryStatusPending                    // принято, ещё не отправлено; retries = 0
	DeliveryStatusRetrying                   // попытка не удалась, запланирован повтор; retries > 0
	DeliveryStatusSent                       // Telegram принял сообщение
	DeliveryStatusFailed                     // терминальный отказ, повторы бессмысленны
	// DeliveryStatusCancelled — терминальный статус, добавленный на шаге
	// gRPC-реализации для Messaging.CancelPending: сообщение было PENDING
	// или RETRYING и вызывающая сторона отменила его до отправки. Не
	// переиспользует DeliveryStatusFailed — "отменено по запросу" и
	// "отправка не удалась" разные по смыслу события, и наблюдатель Messaging.Subscribe должен
	// уметь их различить. Применяется через существующий
	// CommandUpdateDelivery — новой команды FSM для этого не потребовалось.
	DeliveryStatusCancelled
)

func (s DeliveryStatus) String() string {
	switch s {
	case DeliveryStatusPending:
		return "pending"
	case DeliveryStatusRetrying:
		return "retrying"
	case DeliveryStatusSent:
		return "sent"
	case DeliveryStatusFailed:
		return "failed"
	case DeliveryStatusCancelled:
		return "cancelled"
	default:
		return "unspecified"
	}
}

// ProxyConfig — настройка прокси уровня бота или узла (значение по
// умолчанию для узла, переопределение для конкретного бота, отсутствие
// прокси как явный выбор — поэтому на Bot.Proxy это *ProxyConfig, а не
// значение: nil означает "использовать умолчание узла", не "прокси
// выключен").
type ProxyConfig struct {
	Enabled bool   `json:"enabled"`
	Address string `json:"address"` // socks5h://host:port
}

// Bot — реплицированное состояние одного бота. Это внутреннее
// представление raftcluster, а не protobuf-сообщение: в частности, здесь
// хранится Token (нужен internal/telegram для long polling), которого нет
// в botmanagerpb.Bot (токен намеренно не возвращается наружу).
// gRPC-слой обязан вычищать Token при конвертации в ответ клиенту.
type Bot struct {
	ID          string       `json:"id"`
	DisplayName string       `json:"display_name"`
	Token       string       `json:"token"`
	Username    string       `json:"username"` // заполняется internal/telegram (getMe); ни одна команда FSM его не устанавливает
	State       BotState     `json:"state"`
	Proxy       *ProxyConfig `json:"proxy,omitempty"` // nil = использовать умолчание узла
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`

	LastFailureClass  FailureClass `json:"last_failure_class,omitempty"` // заполнено, если State == BotStateBroken
	LastFailureReason string       `json:"last_failure_reason,omitempty"`
}

// Clone возвращает независимую копию (в т.ч. Proxy — отдельный указатель),
// чтобы вызывающий код не мог случайно замутировать состояние FSM через
// указатель, полученный из ListBots/GetBot.
func (b Bot) Clone() Bot {
	if b.Proxy != nil {
		p := *b.Proxy
		b.Proxy = &p
	}
	return b
}

// DeliveryInfo — статус доставки и счётчик попыток одного сообщения.
type DeliveryInfo struct {
	Status      DeliveryStatus `json:"status"`
	Retries     int            `json:"retries"`
	LastError   string         `json:"last_error,omitempty"`
	NextRetryAt time.Time      `json:"next_retry_at,omitempty"`
	SentAt      time.Time      `json:"sent_at,omitempty"`
}

// Button — одна кнопка инлайн-клавиатуры сообщения (в группах работает обычная URL-кнопка, web_app — нет, поэтому его тут
// нет). Ровно одно из двух полей заполнено — URL или CallbackData
// (проверяется на границе gRPC, rpcserver/convert.go:buttonsFromProto, не
// здесь: raftcluster — реплицированное хранилище, а не место валидации
// входа).
type Button struct {
	Text         string `json:"text"`
	URL          string `json:"url,omitempty"`
	CallbackData string `json:"callback_data,omitempty"`
}

// Message — одна запись в истории переписки бота.
// IdempotencyKey уникален по всему кластеру (не только в рамках бота) —
// так же, как GetMessage(idempotency_key) в .proto ищет сообщение без
// указания бота.
type Message struct {
	IdempotencyKey string       `json:"idempotency_key"`
	BotID          string       `json:"bot_id"`
	ChatID         int64        `json:"chat_id"`
	MessageID      int64        `json:"message_id,omitempty"` // id сообщения в Telegram, известен после SENT
	Text           string       `json:"text"`
	Buttons        []Button     `json:"buttons,omitempty"` // один ряд инлайн-кнопок, пусто = без клавиатуры
	Delivery       DeliveryInfo `json:"delivery"`
	Priority       Priority     `json:"priority"`
	CreatedAt      time.Time    `json:"created_at"`
}

// Clone возвращает независимую копию сообщения — в т.ч. Buttons отдельным
// слайсом, чтобы мутация копии (например, в вызывающем коде после
// ListMessages/GetMessage) не была видна оригиналу в состоянии FSM.
// До этого поля Buttons Message был чисто значимым типом и `return m`
// уже давало независимую копию; со слайсом это перестало быть так.
func (m Message) Clone() Message {
	m.Buttons = slices.Clone(m.Buttons)
	return m
}

// ChatMembership — одна запись реестра чатов бота ("chat_member_changed";
// нельзя полагаться на несуществующий в Bot API "список чатов бота").
// Заводится/обновляется исключительно из событий my_chat_member
// (internal/telegram, UpdateKindChatMemberChanged) — не из истории
// сообщений. Ключ записи — (BotID, ChatID); IsMember=false, когда бота
// исключили или он вышел — запись НЕ удаляется (полезно понимать, что бот
// там был), просто перестаёт предлагаться как цель публикации.
type ChatMembership struct {
	BotID     string    `json:"bot_id"`
	ChatID    int64     `json:"chat_id"`
	Title     string    `json:"title,omitempty"` // best-effort, снят через getChat в момент события
	IsMember  bool      `json:"is_member"`
	ChangedAt time.Time `json:"changed_at"`
}

func (c ChatMembership) Clone() ChatMembership { return c }
