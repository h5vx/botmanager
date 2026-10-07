package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// newTestServer builds an httptest.Server that emulates just enough of the
// real Telegram Bot API for one test: handler is called with the method
// name (the last path segment, e.g. "sendMessage") and the decoded request
// body, and returns the envelope to write back.
func newTestServer(t *testing.T, handler func(method string, body map[string]any) (status int, envelope map[string]any)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)

		// path is /bot<token>/<method>
		method := r.URL.Path
		for i := len(method) - 1; i >= 0; i-- {
			if method[i] == '/' {
				method = method[i+1:]
				break
			}
		}

		status, env := handler(method, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(env)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestClient_GetMe(t *testing.T) {
	srv := newTestServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		if method != "getMe" {
			t.Errorf("unexpected method %q", method)
		}
		return http.StatusOK, map[string]any{
			"ok":     true,
			"result": map[string]any{"id": 123, "is_bot": true, "username": "example_test_bot"},
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	user, err := client.GetMe(context.Background())
	if err != nil {
		t.Fatalf("GetMe: %v", err)
	}
	if user.ID != 123 || !user.IsBot || user.Username != "example_test_bot" {
		t.Errorf("GetMe = %+v, unexpected", user)
	}
}

func TestClient_SendMessage_Success(t *testing.T) {
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		if method != "sendMessage" {
			t.Errorf("unexpected method %q", method)
		}
		if body["chat_id"].(float64) != 42 {
			t.Errorf("chat_id = %v, want 42", body["chat_id"])
		}
		return http.StatusOK, map[string]any{
			"ok":     true,
			"result": map[string]any{"message_id": 555, "chat": map[string]any{"id": 42}, "text": body["text"]},
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	res, err := client.SendMessage(context.Background(), 42, "hello", 0, nil)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if res.MessageID != 555 {
		t.Errorf("MessageID = %d, want 555", res.MessageID)
	}
}

func TestClient_SendMessage_WithButtons(t *testing.T) {
	var gotBody map[string]any
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		gotBody = body
		return http.StatusOK, map[string]any{
			"ok":     true,
			"result": map[string]any{"message_id": 1, "chat": map[string]any{"id": 42}},
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	buttons := []raftcluster.Button{{Text: "Открыть", URL: "https://example.org/app"}}
	if _, err := client.SendMessage(context.Background(), 42, "hi", 0, buttons); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	markup, ok := gotBody["reply_markup"].(map[string]any)
	if !ok {
		t.Fatalf("reply_markup missing or wrong type: %#v", gotBody["reply_markup"])
	}
	rows, ok := markup["inline_keyboard"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("inline_keyboard = %#v, want one row", markup["inline_keyboard"])
	}
	row, ok := rows[0].([]any)
	if !ok || len(row) != 1 {
		t.Fatalf("row = %#v, want one button", rows[0])
	}
	btn, ok := row[0].(map[string]any)
	if !ok || btn["text"] != "Открыть" || btn["url"] != "https://example.org/app" {
		t.Errorf("button = %#v, unexpected", btn)
	}
	if _, present := btn["callback_data"]; present {
		t.Errorf("button = %#v, callback_data must be absent for a url button", btn)
	}
}

// TestClient_SendMessage_WithCallbackButton checks a callback_data button
// serializes with callback_data present and url absent — the two fields
// carry omitempty precisely so a callback button never sends url (Telegram
// rejects a button carrying both).
func TestClient_SendMessage_WithCallbackButton(t *testing.T) {
	var gotBody map[string]any
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		gotBody = body
		return http.StatusOK, map[string]any{
			"ok":     true,
			"result": map[string]any{"message_id": 1, "chat": map[string]any{"id": 42}},
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	buttons := []raftcluster.Button{{Text: "Буду", CallbackData: "avail:yes"}}
	if _, err := client.SendMessage(context.Background(), 42, "hi", 0, buttons); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	markup, ok := gotBody["reply_markup"].(map[string]any)
	if !ok {
		t.Fatalf("reply_markup missing or wrong type: %#v", gotBody["reply_markup"])
	}
	rows, ok := markup["inline_keyboard"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("inline_keyboard = %#v, want one row", markup["inline_keyboard"])
	}
	row, ok := rows[0].([]any)
	if !ok || len(row) != 1 {
		t.Fatalf("row = %#v, want one button", rows[0])
	}
	btn, ok := row[0].(map[string]any)
	if !ok || btn["callback_data"] != "avail:yes" {
		t.Fatalf("button = %#v, want callback_data=avail:yes", btn)
	}
	if _, present := btn["url"]; present {
		t.Errorf("button = %#v, url must be absent for a callback button", btn)
	}
}

func TestClient_SendMessage_NoButtons_OmitsReplyMarkup(t *testing.T) {
	var gotBody map[string]any
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		gotBody = body
		return http.StatusOK, map[string]any{
			"ok":     true,
			"result": map[string]any{"message_id": 1, "chat": map[string]any{"id": 42}},
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	if _, err := client.SendMessage(context.Background(), 42, "hi", 0, nil); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	if _, present := gotBody["reply_markup"]; present {
		t.Errorf("reply_markup present without buttons: %#v", gotBody["reply_markup"])
	}
}

func TestClient_SendMessage_401InvalidToken(t *testing.T) {
	srv := newTestServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		return http.StatusUnauthorized, map[string]any{
			"ok": false, "error_code": 401, "description": "Unauthorized",
		}
	})

	client := NewClient("123:bad-token", srv.Client(), srv.URL)
	_, err := client.SendMessage(context.Background(), 42, "hi", 0, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	apiErr, ok := asAPIError(err)
	if !ok {
		t.Fatalf("error is not *APIError: %v", err)
	}
	if apiErr.HTTPStatus != http.StatusUnauthorized || apiErr.ErrorCode != 401 {
		t.Errorf("apiErr = %+v, unexpected", apiErr)
	}
	if got := ClassifyFailure(err); got.String() != "bot" {
		t.Errorf("ClassifyFailure = %v, want bot", got)
	}
}

func TestClient_SendMessage_403BlockedByUser(t *testing.T) {
	srv := newTestServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		return http.StatusForbidden, map[string]any{
			"ok": false, "error_code": 403, "description": "Forbidden: bot was blocked by the user",
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	_, err := client.SendMessage(context.Background(), 42, "hi", 0, nil)
	if got := ClassifyFailure(err); got.String() != "recipient" {
		t.Errorf("ClassifyFailure = %v, want recipient", got)
	}
}

func TestClient_SendMessage_429RetryAfterInBody(t *testing.T) {
	srv := newTestServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		return http.StatusTooManyRequests, map[string]any{
			"ok": false, "error_code": 429, "description": "Too Many Requests: retry after 3",
			"parameters": map[string]any{"retry_after": 3},
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	_, err := client.SendMessage(context.Background(), 42, "hi", 0, nil)
	apiErr, ok := asAPIError(err)
	if !ok {
		t.Fatalf("error is not *APIError: %v", err)
	}
	if apiErr.RetryAfter != 3*time.Second {
		t.Errorf("RetryAfter = %v, want 3s", apiErr.RetryAfter)
	}
	if got := ClassifyFailure(err); got.String() != "rate_limit" {
		t.Errorf("ClassifyFailure = %v, want rate_limit", got)
	}
}

func TestClient_SendMessage_429RetryAfterHeaderOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": 429, "description": "Too Many Requests"})
	}))
	t.Cleanup(srv.Close)

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	_, err := client.SendMessage(context.Background(), 42, "hi", 0, nil)
	apiErr, ok := asAPIError(err)
	if !ok {
		t.Fatalf("error is not *APIError: %v", err)
	}
	if apiErr.RetryAfter != 7*time.Second {
		t.Errorf("RetryAfter = %v, want 7s (from header)", apiErr.RetryAfter)
	}
}

func TestClient_GetUpdates_TransportErrorClassifiesAsNode(t *testing.T) {
	// Точка, которая никогда не отвечает — используем закрытый listener,
	// чтобы гарантированно получить ошибку транспорта (connection refused),
	// а не таймаут.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // порт закрыт, соединение будет отклонено

	client := NewClient("123:test-token", http.DefaultClient, url)
	_, err := client.GetUpdates(context.Background(), 0, 1, AllowedUpdateKinds)
	if err == nil {
		t.Fatal("expected transport error, got nil")
	}
	if _, ok := asAPIError(err); ok {
		t.Fatalf("expected a transport error (not *APIError), got %#v", err)
	}
	if got := ClassifyFailure(err); got.String() != "node" {
		t.Errorf("ClassifyFailure = %v, want node", got)
	}
}

func TestClient_GetUpdates_ParsesMessageCallbackAndChatMember(t *testing.T) {
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		if method != "getUpdates" {
			t.Errorf("unexpected method %q", method)
		}
		return http.StatusOK, map[string]any{
			"ok": true,
			"result": []map[string]any{
				{
					"update_id": 1001,
					"message": map[string]any{
						"message_id": 10,
						"chat":       map[string]any{"id": 555},
						"from":       map[string]any{"id": 777},
						"text":       "привет",
					},
				},
				{
					"update_id": 1002,
					"callback_query": map[string]any{
						"id":   "cbq-1",
						"from": map[string]any{"id": 778},
						"data": "rsvp:yes",
						"message": map[string]any{
							"message_id": 11,
							"chat":       map[string]any{"id": 555},
						},
					},
				},
				{
					"update_id": 1003,
					"my_chat_member": map[string]any{
						"chat":            map[string]any{"id": 556},
						"from":            map[string]any{"id": 779},
						"new_chat_member": map[string]any{"status": "kicked"},
					},
				},
			},
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	updates, err := client.GetUpdates(context.Background(), 0, 1, AllowedUpdateKinds)
	if err != nil {
		t.Fatalf("GetUpdates: %v", err)
	}
	if len(updates) != 3 {
		t.Fatalf("got %d updates, want 3", len(updates))
	}

	msgEv, ok := convertUpdate("bot-1", updates[0])
	if !ok || msgEv.Kind != UpdateKindIncomingMessage || msgEv.Text != "привет" || msgEv.ChatID != 555 {
		t.Errorf("updates[0] converted = %+v (ok=%v)", msgEv, ok)
	}

	cbEv, ok := convertUpdate("bot-1", updates[1])
	if !ok || cbEv.Kind != UpdateKindCallbackQuery || cbEv.CallbackData != "rsvp:yes" || cbEv.ChatID != 555 {
		t.Errorf("updates[1] converted = %+v (ok=%v)", cbEv, ok)
	}

	memberEv, ok := convertUpdate("bot-1", updates[2])
	if !ok || memberEv.Kind != UpdateKindChatMemberChanged || memberEv.NewChatMemberStatus != "kicked" || memberEv.ChatID != 556 {
		t.Errorf("updates[2] converted = %+v (ok=%v)", memberEv, ok)
	}
}

func TestClient_GetChatMember(t *testing.T) {
	srv := newTestServer(t, func(method string, body map[string]any) (int, map[string]any) {
		if method != "getChatMember" {
			t.Errorf("unexpected method %q", method)
		}
		return http.StatusOK, map[string]any{
			"ok": true,
			"result": map[string]any{
				"status":              "administrator",
				"can_pin_messages":    true,
				"can_invite_users":    true,
				"can_delete_messages": false,
			},
		}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	member, err := client.GetBotMembership(context.Background(), 42, 123)
	if err != nil {
		t.Fatalf("GetBotMembership: %v", err)
	}
	if member.Status != "administrator" || !member.CanPinMessages || !member.CanInviteUsers || member.CanDeleteMessages {
		t.Errorf("member = %+v, unexpected", member)
	}
}

func TestClient_EditDeletePinUnpinAnswerCallback(t *testing.T) {
	seen := map[string]bool{}
	srv := newTestServer(t, func(method string, _ map[string]any) (int, map[string]any) {
		seen[method] = true
		if method == "editMessageText" {
			// Unlike delete/pin/unpin/answerCallbackQuery (which really do
			// return the bare boolean true), editMessageText returns the
			// edited Message object — only message_id matters here.
			return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"message_id": 2}}
		}
		return http.StatusOK, map[string]any{"ok": true, "result": true}
	})

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	ctx := context.Background()

	if _, err := client.EditMessageText(ctx, 1, 2, "new text"); err != nil {
		t.Errorf("EditMessageText: %v", err)
	}
	if err := client.DeleteMessage(ctx, 1, 2); err != nil {
		t.Errorf("DeleteMessage: %v", err)
	}
	if err := client.PinChatMessage(ctx, 1, 2); err != nil {
		t.Errorf("PinChatMessage: %v", err)
	}
	if err := client.UnpinChatMessage(ctx, 1, 2); err != nil {
		t.Errorf("UnpinChatMessage: %v", err)
	}
	if err := client.AnswerCallbackQuery(ctx, "cbq-1", "ok", false); err != nil {
		t.Errorf("AnswerCallbackQuery: %v", err)
	}

	for _, m := range []string{"editMessageText", "deleteMessage", "pinChatMessage", "unpinChatMessage", "answerCallbackQuery"} {
		if !seen[m] {
			t.Errorf("method %s was not called", m)
		}
	}
}

// newProfilePhotoServer builds an httptest.Server serving both the JSON Bot
// API methods (getUserProfilePhotos/getFile, path "/bot<token>/<method>")
// and the separate file-download convention ("/file/bot<token>/<file_path>")
// — Client.GetUserProfilePhoto talks to both.
func newProfilePhotoServer(t *testing.T, apiHandler func(method string, body map[string]any) (status int, envelope map[string]any), fileHandler func(filePath string) (status int, contentType string, data []byte)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/file/") {
			if fileHandler == nil {
				t.Fatalf("unexpected file download request: %s", r.URL.Path)
			}
			// path is /file/bot<token>/<file_path>
			parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/file/"), "/", 2)
			filePath := ""
			if len(parts) == 2 {
				filePath = parts[1]
			}
			status, contentType, data := fileHandler(filePath)
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(status)
			_, _ = w.Write(data)
			return
		}

		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		method := r.URL.Path
		for i := len(method) - 1; i >= 0; i-- {
			if method[i] == '/' {
				method = method[i+1:]
				break
			}
		}
		status, env := apiHandler(method, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(env)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestClient_GetUserProfilePhoto_PicksLargestSize(t *testing.T) {
	var gotLimit float64
	srv := newProfilePhotoServer(t,
		func(method string, body map[string]any) (int, map[string]any) {
			switch method {
			case "getUserProfilePhotos":
				gotLimit = body["limit"].(float64)
				return http.StatusOK, map[string]any{
					"ok": true,
					"result": map[string]any{
						"total_count": 1,
						"photos": []any{
							[]any{
								map[string]any{"file_id": "small"},
								map[string]any{"file_id": "large"},
							},
						},
					},
				}
			case "getFile":
				if body["file_id"] != "large" {
					t.Errorf("getFile file_id = %v, want %q (the largest size)", body["file_id"], "large")
				}
				return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"file_path": "photos/file_1.jpg"}}
			default:
				t.Errorf("unexpected method %q", method)
				return http.StatusOK, map[string]any{"ok": true}
			}
		},
		func(filePath string) (int, string, []byte) {
			if filePath != "photos/file_1.jpg" {
				t.Errorf("download file_path = %q, want %q", filePath, "photos/file_1.jpg")
			}
			return http.StatusOK, "image/jpeg", []byte("jpeg-bytes")
		},
	)

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	photo, err := client.GetUserProfilePhoto(context.Background(), 777)
	if err != nil {
		t.Fatalf("GetUserProfilePhoto: %v", err)
	}
	if photo == nil {
		t.Fatal("photo = nil, want a photo")
	}
	if string(photo.Data) != "jpeg-bytes" {
		t.Errorf("Data = %q, want %q", photo.Data, "jpeg-bytes")
	}
	if photo.ContentType != "image/jpeg" {
		t.Errorf("ContentType = %q, want image/jpeg", photo.ContentType)
	}
	if gotLimit != 1 {
		t.Errorf("getUserProfilePhotos limit = %v, want 1", gotLimit)
	}
}

func TestClient_GetUserProfilePhoto_NoPhoto(t *testing.T) {
	srv := newProfilePhotoServer(t,
		func(method string, _ map[string]any) (int, map[string]any) {
			if method != "getUserProfilePhotos" {
				t.Errorf("unexpected method %q", method)
			}
			return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"total_count": 0, "photos": []any{}}}
		},
		nil,
	)

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	photo, err := client.GetUserProfilePhoto(context.Background(), 777)
	if err != nil {
		t.Fatalf("GetUserProfilePhoto: %v", err)
	}
	if photo != nil {
		t.Fatalf("photo = %+v, want nil (no photo is not an error)", photo)
	}
}

func TestClient_GetUserProfilePhoto_GetUserProfilePhotosError(t *testing.T) {
	srv := newProfilePhotoServer(t,
		func(method string, _ map[string]any) (int, map[string]any) {
			return http.StatusBadRequest, map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: user not found"}
		},
		nil,
	)

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	_, err := client.GetUserProfilePhoto(context.Background(), 777)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestClient_GetUserProfilePhoto_GetFileError(t *testing.T) {
	srv := newProfilePhotoServer(t,
		func(method string, _ map[string]any) (int, map[string]any) {
			switch method {
			case "getUserProfilePhotos":
				return http.StatusOK, map[string]any{
					"ok": true,
					"result": map[string]any{
						"total_count": 1,
						"photos":      []any{[]any{map[string]any{"file_id": "f1"}}},
					},
				}
			case "getFile":
				return http.StatusBadRequest, map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: file not found"}
			default:
				t.Errorf("unexpected method %q", method)
				return http.StatusOK, map[string]any{"ok": true}
			}
		},
		nil,
	)

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	_, err := client.GetUserProfilePhoto(context.Background(), 777)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestClient_GetUserProfilePhoto_DownloadError(t *testing.T) {
	srv := newProfilePhotoServer(t,
		func(method string, _ map[string]any) (int, map[string]any) {
			switch method {
			case "getUserProfilePhotos":
				return http.StatusOK, map[string]any{
					"ok": true,
					"result": map[string]any{
						"total_count": 1,
						"photos":      []any{[]any{map[string]any{"file_id": "f1"}}},
					},
				}
			case "getFile":
				return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"file_path": "photos/file_1.jpg"}}
			default:
				t.Errorf("unexpected method %q", method)
				return http.StatusOK, map[string]any{"ok": true}
			}
		},
		func(filePath string) (int, string, []byte) {
			return http.StatusInternalServerError, "text/plain", []byte("boom")
		},
	)

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	_, err := client.GetUserProfilePhoto(context.Background(), 777)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestClient_GetUserProfilePhoto_ContextTimeout checks the download request
// honors ctx's deadline (the part most likely to hang if done carelessly):
// the file handler blocks past the context's deadline, and the call must
// return once ctx is done rather than waiting for the (slow) handler.
func TestClient_GetUserProfilePhoto_ContextTimeout(t *testing.T) {
	block := make(chan struct{})

	srv := newProfilePhotoServer(t,
		func(method string, _ map[string]any) (int, map[string]any) {
			switch method {
			case "getUserProfilePhotos":
				return http.StatusOK, map[string]any{
					"ok": true,
					"result": map[string]any{
						"total_count": 1,
						"photos":      []any{[]any{map[string]any{"file_id": "f1"}}},
					},
				}
			case "getFile":
				return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"file_path": "photos/file_1.jpg"}}
			default:
				t.Errorf("unexpected method %q", method)
				return http.StatusOK, map[string]any{"ok": true}
			}
		},
		nil,
	)
	// Replace the download handler with one that blocks past the deadline
	// by wrapping srv: simplest is a second server just for the file route.
	fileSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	t.Cleanup(fileSrv.Close)

	client := NewClient("123:test-token", srv.Client(), srv.URL)
	client.apiBaseURL = fileSrv.URL // force the download hop at the blocking server

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.GetUserProfilePhoto(ctx, 777)
	close(block) // unblock the handler so fileSrv.Close() (t.Cleanup) does not hang
	if err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
}
