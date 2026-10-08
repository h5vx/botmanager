package telegram

import "encoding/json"

// apiEnvelope is the outer shape of every Telegram Bot API response
// (https://core.telegram.org/bots/api#making-requests).
type apiEnvelope struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result,omitempty"`
	ErrorCode   int             `json:"error_code,omitempty"`
	Description string          `json:"description,omitempty"`
	Parameters  *apiParameters  `json:"parameters,omitempty"`
}

// apiParameters is ResponseParameters — Telegram attaches retry_after here
// for 429 responses (rate-limit class).
type apiParameters struct {
	RetryAfter      int   `json:"retry_after,omitempty"`
	MigrateToChatID int64 `json:"migrate_to_chat_id,omitempty"`
}

// User is getMe's result.
type User struct {
	ID       int64  `json:"id"`
	IsBot    bool   `json:"is_bot"`
	Username string `json:"username"`
}

// SendResult is what SendMessage/EditMessageText hand back — only the
// message id; the rest of Telegram's Message object is not needed here.
type SendResult struct {
	MessageID int64
}

type apiMessageResult struct {
	MessageID int64 `json:"message_id"`
}

// Chat is GetChat's result — chat metadata only. It does NOT carry the
// bot's own membership/rights in that chat (Telegram's getChat never does,
// for any caller) — see Client.GetChatMember/GetBotMembership for that.
type Chat struct {
	ID       int64
	Type     string
	Title    string
	Username string
}

type apiChatFull struct {
	ID       int64  `json:"id"`
	Type     string `json:"type"`
	Title    string `json:"title,omitempty"`
	Username string `json:"username,omitempty"`
}

// ChatMember is GetChatMember's result, narrowed to what the GetChat RPC
// (is the bot a member, what rights it has) needs: membership status plus the admin
// permission booleans relevant to a group/supergroup bot.
type ChatMember struct {
	Status             string
	CanPostMessages    bool
	CanDeleteMessages  bool
	CanPinMessages     bool
	CanInviteUsers     bool
	CanRestrictMembers bool
	CanPromoteMembers  bool
	CanChangeInfo      bool
	CanManageChat      bool
}

type apiChatMember struct {
	Status             string `json:"status"`
	CanPostMessages    bool   `json:"can_post_messages,omitempty"`
	CanDeleteMessages  bool   `json:"can_delete_messages,omitempty"`
	CanPinMessages     bool   `json:"can_pin_messages,omitempty"`
	CanInviteUsers     bool   `json:"can_invite_users,omitempty"`
	CanRestrictMembers bool   `json:"can_restrict_members,omitempty"`
	CanPromoteMembers  bool   `json:"can_promote_members,omitempty"`
	CanChangeInfo      bool   `json:"can_change_info,omitempty"`
	CanManageChat      bool   `json:"can_manage_chat,omitempty"`
}

// apiPhotoSize is PhotoSize — один размер одной фотографии профиля.
type apiPhotoSize struct {
	FileID string `json:"file_id"`
}

// apiUserProfilePhotos is getUserProfilePhotos' result. Photos — по одной
// записи на фотографию (последняя загруженная — первая в списке), внутри
// каждой — размеры от меньшего к большему (см. Client.GetUserProfilePhoto:
// берётся последний элемент Photos[0], то есть самый крупный размер).
type apiUserProfilePhotos struct {
	TotalCount int              `json:"total_count"`
	Photos     [][]apiPhotoSize `json:"photos"`
}

// apiFile is getFile's result — FilePath используется для скачивания по
// {apiBaseURL}/file/bot<token>/<file_path>.
type apiFile struct {
	FilePath string `json:"file_path,omitempty"`
}

// ProfilePhoto is Client.GetUserProfilePhoto's result — байты изображения
// плюс content_type, определённый по расширению file_path.
type ProfilePhoto struct {
	Data        []byte
	ContentType string
}

// --- getUpdates wire shapes (only the fields this package uses) ---

type apiChat struct {
	ID int64 `json:"id"`
}

type apiUser struct {
	ID           int64  `json:"id"`
	IsBot        bool   `json:"is_bot"`
	FirstName    string `json:"first_name"`
	LastName     string `json:"last_name,omitempty"`
	Username     string `json:"username,omitempty"`
	LanguageCode string `json:"language_code,omitempty"`
}

type apiMessage struct {
	MessageID int64    `json:"message_id"`
	Chat      apiChat  `json:"chat"`
	From      *apiUser `json:"from,omitempty"`
	Text      string   `json:"text,omitempty"`
}

type apiCallbackQuery struct {
	ID      string      `json:"id"`
	From    apiUser     `json:"from"`
	Message *apiMessage `json:"message,omitempty"`
	Data    string      `json:"data,omitempty"`
}

// apiChatMemberUpdated is ChatMemberUpdated — used for the my_chat_member
// update (the bot's OWN membership changing, as opposed to chat_member,
// which covers any member and this package does not request — see
// AllowedUpdateKinds in client.go).
type apiChatMemberUpdated struct {
	Chat          apiChat       `json:"chat"`
	From          apiUser       `json:"from"`
	NewChatMember apiChatMember `json:"new_chat_member"`
}

type apiUpdate struct {
	UpdateID      int64                 `json:"update_id"`
	Message       *apiMessage           `json:"message,omitempty"`
	CallbackQuery *apiCallbackQuery     `json:"callback_query,omitempty"`
	MyChatMember  *apiChatMemberUpdated `json:"my_chat_member,omitempty"`
}
