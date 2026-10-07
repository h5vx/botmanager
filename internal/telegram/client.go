package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// AllowedUpdateKinds is the allowed_updates value pollLoop asks getUpdates
// for: message (входящие сообщения), callback_query (нажатия инлайн-кнопок),
// my_chat_member (смена членства/прав САМОГО бота в чате — for any other
// member's membership Telegram uses a separate "chat_member" update, which
// this package does not request).
var AllowedUpdateKinds = []string{"message", "callback_query", "my_chat_member"}

// Client is a minimal Telegram Bot API client for one bot: no SDK
// dependency, just the methods the gRPC API needs (see package doc). One
// Client is created per bot per Runner.Start so its http.Client/proxy
// wiring can differ per bot.
type Client struct {
	httpClient *http.Client
	baseURL    string // e.g. "https://api.telegram.org/bot<token>"
	apiBaseURL string // e.g. "https://api.telegram.org" — без "/bot<token>", нужен для скачивания файлов (/file/bot<token>/...)
	token      string // нужен отдельно от baseURL для той же причины (/file/bot<token>/...)
}

// NewClient builds a Client for one bot's token. httpClient controls
// transport/proxy (see newHTTPClient in proxy.go); apiBaseURL overrides the
// Telegram host ("" = DefaultAPIBaseURL) — tests point it at an
// httptest.Server.
func NewClient(token string, httpClient *http.Client, apiBaseURL string) *Client {
	if apiBaseURL == "" {
		apiBaseURL = DefaultAPIBaseURL
	}
	return &Client{
		httpClient: httpClient,
		baseURL:    apiBaseURL + "/bot" + token,
		apiBaseURL: apiBaseURL,
		token:      token,
	}
}

// do executes one Bot API method: POSTs params as a JSON body to
// baseURL/method, decodes the {"ok": ...} envelope, and unmarshals
// envelope.Result into out (nil for methods with no interesting result).
//
// The returned error is either a transport error (network/DNS/proxy — no
// response ever arrived: classify as FailureClassNode) or *APIError (a
// response arrived with ok=false: see ClassifyFailure). Callers must not
// assume every failure is *APIError.
func (c *Client) do(ctx context.Context, method string, params any, out any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("telegram: encode %s params: %w", method, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+method, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("telegram: build %s request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Транспортная ошибка — ответа не было вовсе ("проблема узла":
		// сеть, DNS, прокси). Намеренно НЕ оборачиваем в *APIError, чтобы
		// ClassifyFailure отличила её от разобранного ответа Telegram.
		return fmt.Errorf("telegram: %s: %w", method, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("telegram: %s: read response: %w", method, err)
	}

	var env apiEnvelope
	if jsonErr := json.Unmarshal(raw, &env); jsonErr != nil {
		// Ответ пришёл, но не в ожидаемом формате JSON — не транспортная
		// ошибка, но и не структурированный ответ Telegram; несём хотя бы
		// HTTP-статус, дальше ClassifyFailure отнесёт это к Unspecified.
		return &APIError{HTTPStatus: resp.StatusCode, Description: fmt.Sprintf("unparseable response: %v", jsonErr)}
	}

	if !env.OK {
		return &APIError{
			HTTPStatus:  resp.StatusCode,
			ErrorCode:   env.ErrorCode,
			Description: env.Description,
			RetryAfter:  retryAfterFrom(env, resp),
		}
	}

	if out != nil && len(env.Result) > 0 {
		if err := json.Unmarshal(env.Result, out); err != nil {
			return fmt.Errorf("telegram: %s: decode result: %w", method, err)
		}
	}
	return nil
}

// retryAfterFrom reads Telegram's requested retry delay from wherever it
// appears: parameters.retry_after in the body takes precedence (Telegram's
// documented mechanism for 429), falling back to a plain "Retry-After:
// <seconds>" HTTP header if the body did not carry it — real deployments
// (and reverse proxies in front of them) have been observed to use either.
func retryAfterFrom(env apiEnvelope, resp *http.Response) time.Duration {
	if env.Parameters != nil && env.Parameters.RetryAfter > 0 {
		return time.Duration(env.Parameters.RetryAfter) * time.Second
	}
	if h := resp.Header.Get("Retry-After"); h != "" {
		if n, err := strconv.Atoi(h); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 0
}

func (c *Client) GetMe(ctx context.Context) (*User, error) {
	var u User
	if err := c.do(ctx, "getMe", struct{}{}, &u); err != nil {
		return nil, err
	}
	return &u, nil
}

type getUpdatesParams struct {
	Offset         int64    `json:"offset,omitempty"`
	Timeout        int      `json:"timeout"`
	AllowedUpdates []string `json:"allowed_updates,omitempty"`
}

// GetUpdates calls getUpdates once (long polling: the call blocks up to
// timeoutSeconds server-side, per ctx's own deadline client-side — see
// pollLoop, which sets ctx's deadline a little longer than timeoutSeconds).
func (c *Client) GetUpdates(ctx context.Context, offset int64, timeoutSeconds int, allowedUpdates []string) ([]apiUpdate, error) {
	var updates []apiUpdate
	params := getUpdatesParams{Offset: offset, Timeout: timeoutSeconds, AllowedUpdates: allowedUpdates}
	if err := c.do(ctx, "getUpdates", params, &updates); err != nil {
		return nil, err
	}
	return updates, nil
}

type sendMessageParams struct {
	ChatID           int64           `json:"chat_id"`
	Text             string          `json:"text"`
	ReplyToMessageID int64           `json:"reply_to_message_id,omitempty"`
	ReplyMarkup      *inlineKeyboard `json:"reply_markup,omitempty"`
}

// inlineKeyboard is Telegram's InlineKeyboardMarkup, narrowed to one row of
// URL/callback buttons (see raftcluster.Button doc comment). ReplyMarkup above is a pointer so that "no buttons" serializes
// as an absent reply_markup key, not an empty object — Telegram
// distinguishes the two, and a caller not asking for a keyboard should not
// send one.
type inlineKeyboard struct {
	InlineKeyboard [][]inlineKeyboardButton `json:"inline_keyboard"`
}

// inlineKeyboardButton — both URL and CallbackData carry omitempty so that
// only the one raftcluster.Button actually set is present in the outgoing
// JSON: Telegram rejects a button carrying both url and callback_data.
type inlineKeyboardButton struct {
	Text         string `json:"text"`
	URL          string `json:"url,omitempty"`
	CallbackData string `json:"callback_data,omitempty"`
}

func replyMarkupFor(buttons []raftcluster.Button) *inlineKeyboard {
	if len(buttons) == 0 {
		return nil
	}
	row := make([]inlineKeyboardButton, len(buttons))
	for i, b := range buttons {
		row[i] = inlineKeyboardButton{Text: b.Text, URL: b.URL, CallbackData: b.CallbackData}
	}
	return &inlineKeyboard{InlineKeyboard: [][]inlineKeyboardButton{row}}
}

// SendMessage sends a text message, optionally with a single row of URL
// inline buttons (buttons == nil/empty omits reply_markup entirely).
// replyToMessageID == 0 omits reply_to_message_id. raftcluster.Message
// (the replicated message record) has no field carrying a reply
// target yet, so Runner always passes 0 today; the client already
// supports the parameter for when it does.
func (c *Client) SendMessage(ctx context.Context, chatID int64, text string, replyToMessageID int64, buttons []raftcluster.Button) (*SendResult, error) {
	var res apiMessageResult
	params := sendMessageParams{
		ChatID:           chatID,
		Text:             text,
		ReplyToMessageID: replyToMessageID,
		ReplyMarkup:      replyMarkupFor(buttons),
	}
	if err := c.do(ctx, "sendMessage", params, &res); err != nil {
		return nil, err
	}
	return &SendResult{MessageID: res.MessageID}, nil
}

type editMessageTextParams struct {
	ChatID    int64  `json:"chat_id"`
	MessageID int64  `json:"message_id"`
	Text      string `json:"text"`
}

func (c *Client) EditMessageText(ctx context.Context, chatID, messageID int64, text string) (*SendResult, error) {
	var res apiMessageResult
	params := editMessageTextParams{ChatID: chatID, MessageID: messageID, Text: text}
	if err := c.do(ctx, "editMessageText", params, &res); err != nil {
		return nil, err
	}
	return &SendResult{MessageID: res.MessageID}, nil
}

type chatMessageParams struct {
	ChatID    int64 `json:"chat_id"`
	MessageID int64 `json:"message_id"`
}

func (c *Client) DeleteMessage(ctx context.Context, chatID, messageID int64) error {
	return c.do(ctx, "deleteMessage", chatMessageParams{ChatID: chatID, MessageID: messageID}, nil)
}

func (c *Client) PinChatMessage(ctx context.Context, chatID, messageID int64) error {
	return c.do(ctx, "pinChatMessage", chatMessageParams{ChatID: chatID, MessageID: messageID}, nil)
}

type unpinChatMessageParams struct {
	ChatID    int64 `json:"chat_id"`
	MessageID int64 `json:"message_id,omitempty"` // 0 = отменить закреп последнего закреплённого
}

func (c *Client) UnpinChatMessage(ctx context.Context, chatID, messageID int64) error {
	return c.do(ctx, "unpinChatMessage", unpinChatMessageParams{ChatID: chatID, MessageID: messageID}, nil)
}

type answerCallbackQueryParams struct {
	CallbackQueryID string `json:"callback_query_id"`
	Text            string `json:"text,omitempty"`
	ShowAlert       bool   `json:"show_alert,omitempty"`
}

func (c *Client) AnswerCallbackQuery(ctx context.Context, callbackQueryID, text string, showAlert bool) error {
	params := answerCallbackQueryParams{CallbackQueryID: callbackQueryID, Text: text, ShowAlert: showAlert}
	return c.do(ctx, "answerCallbackQuery", params, nil)
}

type chatIDParams struct {
	ChatID int64 `json:"chat_id"`
}

func (c *Client) GetChat(ctx context.Context, chatID int64) (*Chat, error) {
	var res apiChatFull
	if err := c.do(ctx, "getChat", chatIDParams{ChatID: chatID}, &res); err != nil {
		return nil, err
	}
	return &Chat{ID: res.ID, Type: res.Type, Title: res.Title, Username: res.Username}, nil
}

type getChatMemberParams struct {
	ChatID int64 `json:"chat_id"`
	UserID int64 `json:"user_id"`
}

// GetChatMember reports one user's membership/rights in a chat.
//
// The GetChat RPC answers "is the bot a member, what rights" — that is really
// a question about the CALLING BOT's own membership, and Telegram's getChat
// does not answer it (getChat returns chat metadata only, for any caller,
// never the querying bot's own rights). getChatMember with user_id set to
// the bot's own id is Telegram's documented way to learn that — see
// GetBotMembership, the convenience wrapper that is the shape GetChat
// actually needs.
func (c *Client) GetChatMember(ctx context.Context, chatID, userID int64) (*ChatMember, error) {
	var res apiChatMember
	params := getChatMemberParams{ChatID: chatID, UserID: userID}
	if err := c.do(ctx, "getChatMember", params, &res); err != nil {
		return nil, err
	}
	return &ChatMember{
		Status:             res.Status,
		CanPostMessages:    res.CanPostMessages,
		CanDeleteMessages:  res.CanDeleteMessages,
		CanPinMessages:     res.CanPinMessages,
		CanInviteUsers:     res.CanInviteUsers,
		CanRestrictMembers: res.CanRestrictMembers,
		CanPromoteMembers:  res.CanPromoteMembers,
		CanChangeInfo:      res.CanChangeInfo,
		CanManageChat:      res.CanManageChat,
	}, nil
}

// GetBotMembership is GetChatMember(ctx, chatID, botUserID) — see the
// comment on GetChatMember for why this, not GetChat, answers "does
// the bot belong to this chat, what rights does it have".
func (c *Client) GetBotMembership(ctx context.Context, chatID, botUserID int64) (*ChatMember, error) {
	return c.GetChatMember(ctx, chatID, botUserID)
}

// maxProfilePhotoBytes ограничивает скачивание файла фотографии профиля —
// на практике аватары маленькие, но нельзя доверять неограниченному чтению
// от внешнего сервиса.
const maxProfilePhotoBytes = 5 << 20 // 5 МБ

type getUserProfilePhotosParams struct {
	UserID int64 `json:"user_id"`
	Limit  int   `json:"limit"`
}

type getFileParams struct {
	FileID string `json:"file_id"`
}

// GetUserProfilePhoto возвращает текущую (самую свежую) фотографию профиля
// пользователя Telegram в максимальном доступном размере. Возвращает (nil,
// nil), когда у пользователя нет ни одной фотографии профиля
// (getUserProfilePhotos.total_count == 0) — это штатный исход, не ошибка;
// вызывающая сторона (BotAdminServer.GetUserProfilePhoto) превращает его в
// found=false. Любая другая неудача любого из трёх вызовов
// (getUserProfilePhotos, getFile, скачивание) — настоящая ошибка.
func (c *Client) GetUserProfilePhoto(ctx context.Context, userID int64) (*ProfilePhoto, error) {
	var photos apiUserProfilePhotos
	params := getUserProfilePhotosParams{UserID: userID, Limit: 1}
	if err := c.do(ctx, "getUserProfilePhotos", params, &photos); err != nil {
		return nil, fmt.Errorf("telegram: get user profile photo: getUserProfilePhotos: %w", err)
	}
	if photos.TotalCount == 0 || len(photos.Photos) == 0 || len(photos.Photos[0]) == 0 {
		return nil, nil
	}

	// Самый крупный размер — последний элемент (Bot API отдаёт размеры по
	// возрастанию).
	sizes := photos.Photos[0]
	largest := sizes[len(sizes)-1]

	var file apiFile
	if err := c.do(ctx, "getFile", getFileParams(largest), &file); err != nil {
		return nil, fmt.Errorf("telegram: get user profile photo: getFile: %w", err)
	}
	if file.FilePath == "" {
		return nil, fmt.Errorf("telegram: get user profile photo: getFile: empty file_path")
	}

	data, err := c.downloadFile(ctx, file.FilePath)
	if err != nil {
		return nil, fmt.Errorf("telegram: get user profile photo: download: %w", err)
	}

	return &ProfilePhoto{Data: data, ContentType: contentTypeForFilePath(file.FilePath)}, nil
}

// downloadFile скачивает файл по пути, полученному от getFile — отдельный
// URL-паттерн Telegram ("/file/bot<token>/<file_path>"), не "/bot<token>/",
// поэтому строится из apiBaseURL напрямую, а не через baseURL.
func (c *Client) downloadFile(ctx context.Context, filePath string) ([]byte, error) {
	url := c.apiBaseURL + "/file/bot" + c.token + "/" + filePath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxProfilePhotoBytes))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return data, nil
}

// contentTypeForFilePath определяет Content-Type по расширению file_path.
// На практике фотографии профиля Telegram всегда .jpg — умолчание на
// image/jpeg этого не переусложняет.
func contentTypeForFilePath(filePath string) string {
	switch {
	case strings.HasSuffix(filePath, ".png"):
		return "image/png"
	case strings.HasSuffix(filePath, ".webp"):
		return "image/webp"
	case strings.HasSuffix(filePath, ".jpg"), strings.HasSuffix(filePath, ".jpeg"):
		return "image/jpeg"
	default:
		return "image/jpeg"
	}
}
