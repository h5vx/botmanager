package rpcserver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
)

// signInitDataForTest independently signs a minimal initData fixture per
// Telegram's WebApp algorithm. Deliberately not shared with
// internal/telegram/initdata_test.go's own buildInitData helper: this
// package's tests exist to check RPC wiring (bot lookup by id, state
// handling, that the response never carries a token) — the hashing itself
// is already exhaustively covered by internal/telegram/initdata_test.go, so
// depending on that package's test code here would blur what each test
// suite is actually proving.
func signInitDataForTest(token string, authDate time.Time, userID int64) string {
	fields := map[string]string{
		"auth_date": fmt.Sprintf("%d", authDate.Unix()),
		"user":      fmt.Sprintf(`{"id":%d,"username":"tester"}`, userID),
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, k+"="+fields[k])
	}
	dataCheckString := strings.Join(pairs, "\n")

	secretKeyMAC := hmac.New(sha256.New, []byte("WebAppData"))
	secretKeyMAC.Write([]byte(token))
	secretKey := secretKeyMAC.Sum(nil)

	dataMAC := hmac.New(sha256.New, secretKey)
	dataMAC.Write([]byte(dataCheckString))
	hash := hex.EncodeToString(dataMAC.Sum(nil))

	parts := append(append([]string{}, pairs...), "hash="+hash)
	return strings.Join(parts, "&")
}

// TestVerifyInitData_RPC exercises BotAdminServer.VerifyInitData over a
// real gRPC round trip: signature success against the right bot, failure
// against a different bot's token, an unknown bot_id, and a deleted bot —
// plus the freshly-created (DISABLED) bot case that CLAUDE.md's
// "VerifyInitData" section calls out explicitly: the bot need not be
// enabled to verify a Mini App launch against it.
func TestVerifyInitData_RPC(t *testing.T) {
	node := newTestNode(t)
	botAdmin, _, _ := newTestClients(t, node)
	ctx := context.Background()

	const token = "123456789:AAtestTokenForRPCVerify"
	created, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{
		DisplayName: "Mini App bot", Token: token,
	})
	if err != nil {
		t.Fatalf("CreateBot: %v", err)
	}
	if got, err := botAdmin.GetBot(ctx, &botmanagerpb.GetBotRequest{Id: created.GetId()}); err != nil {
		t.Fatalf("GetBot: %v", err)
	} else if got.GetState() != botmanagerpb.BotState_BOT_STATE_DISABLED {
		t.Fatalf("freshly created bot state = %v, want DISABLED (precondition for the rest of this test)", got.GetState())
	}

	now := time.Now().UTC()
	initData := signInitDataForTest(token, now.Add(-1*time.Minute), 555)

	result, err := botAdmin.VerifyInitData(ctx, &botmanagerpb.VerifyInitDataRequest{
		BotId: created.GetId(), InitData: initData,
	})
	if err != nil {
		t.Fatalf("VerifyInitData: %v", err)
	}
	if !result.GetValid() {
		t.Fatalf("VerifyInitData.Valid = false (reason %q), want true — a DISABLED bot must still verify", result.GetInvalidReason())
	}
	if result.GetTelegramUserId() != 555 {
		t.Errorf("TelegramUserId = %d, want 555", result.GetTelegramUserId())
	}
	if result.GetAuthDate() == nil {
		t.Errorf("AuthDate is nil, want set")
	}

	otherCreated, err := botAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{
		DisplayName: "Other bot", Token: "999999999:BBotherTokenEntirely",
	})
	if err != nil {
		t.Fatalf("CreateBot (other): %v", err)
	}

	t.Run("wrong bot for signature", func(t *testing.T) {
		mismatched, err := botAdmin.VerifyInitData(ctx, &botmanagerpb.VerifyInitDataRequest{
			BotId: otherCreated.GetId(), InitData: initData,
		})
		if err != nil {
			t.Fatalf("VerifyInitData: %v", err)
		}
		if mismatched.GetValid() {
			t.Fatalf("Valid = true for initData signed with a different bot's token")
		}
		if mismatched.GetInvalidReason() == "" {
			t.Errorf("InvalidReason is empty, want a reason")
		}
	})

	t.Run("unknown bot_id", func(t *testing.T) {
		_, err := botAdmin.VerifyInitData(ctx, &botmanagerpb.VerifyInitDataRequest{
			BotId: "does-not-exist", InitData: initData,
		})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("code = %v, want NotFound", status.Code(err))
		}
	})

	t.Run("deleted bot", func(t *testing.T) {
		if _, err := botAdmin.DeleteBot(ctx, &botmanagerpb.DeleteBotRequest{Id: otherCreated.GetId()}); err != nil {
			t.Fatalf("DeleteBot: %v", err)
		}
		_, err := botAdmin.VerifyInitData(ctx, &botmanagerpb.VerifyInitDataRequest{
			BotId: otherCreated.GetId(), InitData: initData,
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("code = %v, want FailedPrecondition", status.Code(err))
		}
	})
}
