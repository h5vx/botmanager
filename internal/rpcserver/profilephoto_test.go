package rpcserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
)

// newFakeTelegramFileServer is newFakeTelegramServer (messaging_test.go)
// extended with the separate file-download route
// ("/file/bot<token>/<file_path>") that GetUserProfilePhoto needs on top of
// the ordinary JSON Bot API methods.
func newFakeTelegramFileServer(t *testing.T, apiHandler func(method string, body map[string]any) (status int, envelope map[string]any), fileHandler func(filePath string) (status int, contentType string, data []byte)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/file/") {
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

// TestBotAdmin_GetUserProfilePhoto_Found covers the success path: a photo
// exists, botmanager fetches it and returns found=true with the image
// bytes and content type.
func TestBotAdmin_GetUserProfilePhoto_Found(t *testing.T) {
	srv := newFakeTelegramFileServer(t,
		func(method string, body map[string]any) (int, map[string]any) {
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
				return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"file_path": "photos/avatar.jpg"}}
			default:
				t.Errorf("unexpected method %q", method)
				return http.StatusOK, map[string]any{"ok": true}
			}
		},
		func(filePath string) (int, string, []byte) {
			return http.StatusOK, "image/jpeg", []byte("jpeg-bytes")
		},
	)

	node := newTestNode(t)
	botAdmin, _, _ := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	res, err := botAdmin.GetUserProfilePhoto(ctx, &botmanagerpb.GetUserProfilePhotoRequest{BotId: bot.GetId(), UserId: 777})
	if err != nil {
		t.Fatalf("GetUserProfilePhoto: %v", err)
	}
	if !res.GetFound() {
		t.Fatalf("Found = false, want true")
	}
	if string(res.GetData()) != "jpeg-bytes" {
		t.Errorf("Data = %q, want %q", res.GetData(), "jpeg-bytes")
	}
	if res.GetContentType() != "image/jpeg" {
		t.Errorf("ContentType = %q, want image/jpeg", res.GetContentType())
	}
}

// TestBotAdmin_GetUserProfilePhoto_NotFound covers the found=false case: a
// user with no profile photo is a normal result, not an error.
func TestBotAdmin_GetUserProfilePhoto_NotFound(t *testing.T) {
	srv := newFakeTelegramFileServer(t,
		func(method string, _ map[string]any) (int, map[string]any) {
			if method != "getUserProfilePhotos" {
				t.Errorf("unexpected method %q", method)
			}
			return http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"total_count": 0, "photos": []any{}}}
		},
		nil,
	)

	node := newTestNode(t)
	botAdmin, _, _ := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	res, err := botAdmin.GetUserProfilePhoto(ctx, &botmanagerpb.GetUserProfilePhotoRequest{BotId: bot.GetId(), UserId: 777})
	if err != nil {
		t.Fatalf("GetUserProfilePhoto: %v", err)
	}
	if res.GetFound() {
		t.Fatalf("Found = true, want false (no photo is not an error)")
	}
}

// TestBotAdmin_GetUserProfilePhoto_TelegramError covers a real Telegram
// failure (as opposed to found=false) mapping to a gRPC status via
// telegramError.
func TestBotAdmin_GetUserProfilePhoto_TelegramError(t *testing.T) {
	srv := newFakeTelegramFileServer(t,
		func(method string, _ map[string]any) (int, map[string]any) {
			return http.StatusUnauthorized, map[string]any{"ok": false, "error_code": 401, "description": "Unauthorized"}
		},
		nil,
	)

	node := newTestNode(t)
	botAdmin, _, _ := newTestClientsWithBaseURL(t, node, srv.URL)
	ctx := context.Background()

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:bad"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}

	_, err = botAdmin.GetUserProfilePhoto(ctx, &botmanagerpb.GetUserProfilePhotoRequest{BotId: bot.GetId(), UserId: 777})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (bot failure class)", status.Code(err))
	}
}

// TestBotAdmin_GetUserProfilePhoto_BotNotFound_And_Deleted covers the two
// bot-lookup edge cases, same pattern as GetChat/VerifyInitData.
func TestBotAdmin_GetUserProfilePhoto_BotNotFound_And_Deleted(t *testing.T) {
	node := newTestNode(t)
	botAdmin, _, _ := newTestClients(t, node)
	ctx := context.Background()

	if _, err := botAdmin.GetUserProfilePhoto(ctx, &botmanagerpb.GetUserProfilePhotoRequest{BotId: "does-not-exist", UserId: 777}); status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}

	bot, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{DisplayName: "B", Token: "123:tok"})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}
	if _, err := botAdmin.DeleteBot(ctx, &botmanagerpb.DeleteBotRequest{Id: bot.GetId()}); err != nil {
		t.Fatalf("DeleteBot: %v", err)
	}

	if _, err := botAdmin.GetUserProfilePhoto(ctx, &botmanagerpb.GetUserProfilePhotoRequest{BotId: bot.GetId(), UserId: 777}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition (bot deleted)", status.Code(err))
	}
}
