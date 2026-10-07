package telegram

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

const testBotToken = "123456789:AAHtestBotTokenForUnitTestsOnly"

// buildInitData is an independent test fixture builder — it computes the
// HMAC-SHA256 signature itself, using stdlib crypto/hmac and crypto/sha256
// directly, and does NOT call VerifyInitData (or share any helper with it)
// to produce a fixture. A bug shared between the production hashing and a
// test-side helper reusing the same code would make the corresponding test
// pass regardless of whether the algorithm is actually correct — this
// keeps the two independent, per the task's own instruction.
//
// fieldOrder controls the order fields are emitted in the raw query string
// (nil = sorted order); the data-check-string used for the signature is
// always built from a fresh sort, matching what a real Telegram client
// does (field order in the wire format is unspecified) and what
// TestVerifyInitData_FieldOrderAndEncoding exercises explicitly.
func buildInitData(t *testing.T, token string, fields map[string]string, fieldOrder []string) string {
	t.Helper()

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

	order := fieldOrder
	if order == nil {
		order = keys
	}
	parts := make([]string, 0, len(order)+1)
	for _, k := range order {
		parts = append(parts, k+"="+url.QueryEscape(fields[k]))
	}
	parts = append(parts, "hash="+hash)
	return strings.Join(parts, "&")
}

// userJSON builds the JSON value of initData's "user" field. allowsWriteToPm
// == nil omits the field entirely (old-client / "unknown" case).
func userJSON(id int64, username string, allowsWriteToPm *bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"id":%d`, id)
	if username != "" {
		fmt.Fprintf(&b, `,"username":%q`, username)
	}
	if allowsWriteToPm != nil {
		fmt.Fprintf(&b, `,"allows_write_to_pm":%t`, *allowsWriteToPm)
	}
	b.WriteString("}")
	return b.String()
}

func boolPtr(b bool) *bool { return &b }

func TestVerifyInitData_ValidSignature(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	authDate := now.Add(-5 * time.Minute)
	fields := map[string]string{
		"auth_date":   fmt.Sprintf("%d", authDate.Unix()),
		"query_id":    "AAHtestQueryId",
		"user":        userJSON(555, "alice", nil),
		"start_param": "poll_42",
	}
	initData := buildInitData(t, testBotToken, fields, nil)

	got, err := VerifyInitData(initData, testBotToken, 24*time.Hour, now)
	if err != nil {
		t.Fatalf("VerifyInitData: %v", err)
	}
	if got.User.ID != 555 {
		t.Errorf("User.ID = %d, want 555", got.User.ID)
	}
	if got.User.Username != "alice" {
		t.Errorf("User.Username = %q, want %q", got.User.Username, "alice")
	}
	if got.StartParam != "poll_42" {
		t.Errorf("StartParam = %q, want %q", got.StartParam, "poll_42")
	}
	if !got.AuthDate.Equal(authDate) {
		t.Errorf("AuthDate = %v, want %v", got.AuthDate, authDate)
	}
	if got.User.AllowsWriteToPm != nil {
		t.Errorf("AllowsWriteToPm = %v, want nil (field absent)", *got.User.AllowsWriteToPm)
	}
}

func TestVerifyInitData_CorruptedHash(t *testing.T) {
	now := time.Now().UTC()
	fields := map[string]string{
		"auth_date": fmt.Sprintf("%d", now.Unix()),
		"user":      userJSON(1, "bob", nil),
	}
	initData := buildInitData(t, testBotToken, fields, nil)

	// Flip the last character of the hash — a one-character corruption of
	// an otherwise validly-signed string.
	corrupted := []byte(initData)
	last := len(corrupted) - 1
	if corrupted[last] == '0' {
		corrupted[last] = '1'
	} else {
		corrupted[last] = '0'
	}

	_, err := VerifyInitData(string(corrupted), testBotToken, 24*time.Hour, now)
	if !errors.Is(err, ErrInitDataBadSignature) {
		t.Fatalf("err = %v, want ErrInitDataBadSignature", err)
	}
}

func TestVerifyInitData_WrongTokenAtVerification(t *testing.T) {
	now := time.Now().UTC()
	fields := map[string]string{
		"auth_date": fmt.Sprintf("%d", now.Unix()),
		"user":      userJSON(1, "carol", nil),
	}
	initData := buildInitData(t, testBotToken, fields, nil)

	_, err := VerifyInitData(initData, "999999999:BBotherBotTokenEntirely", 24*time.Hour, now)
	if !errors.Is(err, ErrInitDataBadSignature) {
		t.Fatalf("err = %v, want ErrInitDataBadSignature", err)
	}
}

func TestVerifyInitData_Expired(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	authDate := now.Add(-25 * time.Hour) // older than the 24h window used below
	fields := map[string]string{
		"auth_date": fmt.Sprintf("%d", authDate.Unix()),
		"user":      userJSON(1, "dave", nil),
	}
	initData := buildInitData(t, testBotToken, fields, nil)

	_, err := VerifyInitData(initData, testBotToken, 24*time.Hour, now)
	if !errors.Is(err, ErrInitDataExpired) {
		t.Fatalf("err = %v, want ErrInitDataExpired", err)
	}
	if errors.Is(err, ErrInitDataBadSignature) {
		t.Fatalf("expired auth_date must be a distinct reason from ErrInitDataBadSignature, got err satisfying both: %v", err)
	}
}

func TestVerifyInitData_ExpiredUsesDefaultWindow(t *testing.T) {
	// maxAuthAge <= 0 falls back to DefaultMaxInitDataAge (24h) — exercised
	// here with 0 explicitly, the value VerifyInitDataRequest.max_auth_age_seconds
	// carries when the caller does not set it.
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	authDate := now.Add(-DefaultMaxInitDataAge - time.Minute)
	fields := map[string]string{
		"auth_date": fmt.Sprintf("%d", authDate.Unix()),
		"user":      userJSON(1, "erin", nil),
	}
	initData := buildInitData(t, testBotToken, fields, nil)

	_, err := VerifyInitData(initData, testBotToken, 0, now)
	if !errors.Is(err, ErrInitDataExpired) {
		t.Fatalf("err = %v, want ErrInitDataExpired (default window)", err)
	}
}

func TestVerifyInitData_AllowsWriteToPm(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name string
		in   *bool
	}{
		{"absent", nil},
		{"true", boolPtr(true)},
		{"false", boolPtr(false)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fields := map[string]string{
				"auth_date": fmt.Sprintf("%d", now.Unix()),
				"user":      userJSON(1, "eve", c.in),
			}
			initData := buildInitData(t, testBotToken, fields, nil)

			got, err := VerifyInitData(initData, testBotToken, 24*time.Hour, now)
			if err != nil {
				t.Fatalf("VerifyInitData: %v", err)
			}
			gotPresent := got.User.AllowsWriteToPm != nil
			wantPresent := c.in != nil
			if gotPresent != wantPresent {
				t.Fatalf("AllowsWriteToPm presence = %v, want %v", gotPresent, wantPresent)
			}
			if wantPresent && *got.User.AllowsWriteToPm != *c.in {
				t.Fatalf("AllowsWriteToPm = %v, want %v", *got.User.AllowsWriteToPm, *c.in)
			}
		})
	}
}

// TestVerifyInitData_FieldOrderAndEncoding checks the algorithm step
// "sort before hashing" is actually applied (a check string built without
// sorting would fail to verify whenever the wire order differs from sorted
// order) and that values needing URL-encoding (raw "&"/"=" and Cyrillic
// text, both inside the JSON "user" value and inside start_param) round-trip
// correctly.
func TestVerifyInitData_FieldOrderAndEncoding(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	authDate := now.Add(-1 * time.Minute)
	const username = "Иван & Петров=Тест"
	const startParam = "poll_7&school=1"

	fields := map[string]string{
		"auth_date":   fmt.Sprintf("%d", authDate.Unix()),
		"query_id":    "AAHexampleQueryId",
		"start_param": startParam,
		"user":        userJSON(777, username, boolPtr(true)),
	}

	orders := map[string][]string{
		"sorted":        {"auth_date", "query_id", "start_param", "user"},
		"reverse":       {"user", "start_param", "query_id", "auth_date"},
		"interleaved":   {"start_param", "auth_date", "user", "query_id"},
		"user_first":    {"user", "auth_date", "query_id", "start_param"},
		"hash_adjacent": {"query_id", "user", "auth_date", "start_param"},
	}

	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			initData := buildInitData(t, testBotToken, fields, order)

			got, err := VerifyInitData(initData, testBotToken, 24*time.Hour, now)
			if err != nil {
				t.Fatalf("VerifyInitData: %v", err)
			}
			if got.User.Username != username {
				t.Errorf("Username = %q, want %q", got.User.Username, username)
			}
			if got.StartParam != startParam {
				t.Errorf("StartParam = %q, want %q", got.StartParam, startParam)
			}
			if got.User.ID != 777 {
				t.Errorf("User.ID = %d, want 777", got.User.ID)
			}
			if got.User.AllowsWriteToPm == nil || !*got.User.AllowsWriteToPm {
				t.Errorf("AllowsWriteToPm = %v, want true", got.User.AllowsWriteToPm)
			}
		})
	}
}

func TestVerifyInitData_MissingHash(t *testing.T) {
	now := time.Now().UTC()
	_, err := VerifyInitData("auth_date="+fmt.Sprintf("%d", now.Unix()), testBotToken, 24*time.Hour, now)
	if !errors.Is(err, ErrInitDataBadSignature) {
		t.Fatalf("err = %v, want ErrInitDataBadSignature", err)
	}
}
