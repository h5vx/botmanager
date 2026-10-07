package telegram

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultMaxInitDataAge is the fallback freshness window for VerifyInitData
// when the caller does not request a specific one (VerifyInitDataRequest's
// max_auth_age_seconds == 0). 24h is chosen because a Mini App session
// is opened interactively by a human tapping a link/button — there is no
// legitimate reason for a signed initData string to be replayed a day
// later, but 24h comfortably covers clock skew between this server and
// Telegram's, and a user who opened the app and left the tab open
// overnight before the JS actually calls the server (Telegram itself
// does not re-sign on every network request, only on (re)launch).
const DefaultMaxInitDataAge = 24 * time.Hour

// ErrInitDataBadSignature is returned when initData's hash does not match
// the HMAC computed from botToken, or the string is malformed
// (unparseable query string, missing hash/auth_date, unparseable user
// JSON) — anything short of "the signature checked out but is too old".
var ErrInitDataBadSignature = errors.New("initdata: bad signature")

// ErrInitDataExpired is returned when the signature is valid but auth_date
// is older than maxAuthAge — a distinct reason from ErrInitDataBadSignature
// (callers/UIs benefit from telling a user "open the app
// again" apart from "something is tampering with requests").
var ErrInitDataExpired = errors.New("initdata: expired")

// InitDataUser is the parsed "user" field of Telegram.WebApp.initData —
// unsafe in the sense Telegram's own docs use the term (it is signed and
// therefore authentic, but merely what the Telegram client reported, not
// independently re-checked against anything else).
type InitDataUser struct {
	ID       int64
	Username string // "" if absent

	// AllowsWriteToPm distinguishes three states: nil = the field was absent from user entirely
	// (older Telegram client — treat as "unknown", request permission
	// again), non-nil = the client reported it explicitly (true = do not
	// re-request, false = request).  A bare bool could not represent
	// "absent" without an extra flag, and collapsing the two would lose
	// information.
	AllowsWriteToPm *bool
}

// InitData is VerifyInitData's parsed result once the signature and
// freshness check both pass.
type InitData struct {
	User       InitDataUser
	AuthDate   time.Time
	StartParam string // "" if absent — the Mini App's start parameter
}

// VerifyInitData checks a Telegram Mini App initData string against
// botToken per Telegram's documented WebApp algorithm (distinct from the
// Login Widget algorithm — see the package doc comment). now and
// maxAuthAge are explicit parameters rather than time.Now() so the check
// is deterministic and testable (injectable clock); maxAuthAge <= 0 falls back to DefaultMaxInitDataAge.
//
// Algorithm (see https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app):
//  1. Parse initData as a query string, URL-decoding values.
//  2. Extract and remove "hash".
//  3. Sort the remaining key=value pairs by key (byte order) and join with
//     "\n" — the data-check-string.
//  4. secret_key = HMAC-SHA256(key="WebAppData", data=botToken) — the
//     literal "WebAppData" is the HMAC KEY, botToken is the MESSAGE (this
//     is the detail most often reversed).
//  5. computed_hash = hex(HMAC-SHA256(key=secret_key, data=data-check-string)).
//  6. Compare computed_hash to the received hash in constant time.
//  7. Check auth_date's age.
//  8. Parse "user" (JSON) for id/username/allows_write_to_pm.
func VerifyInitData(initData, botToken string, maxAuthAge time.Duration, now time.Time) (InitData, error) {
	values, err := url.ParseQuery(initData)
	if err != nil {
		return InitData{}, fmt.Errorf("%w: parse query string: %v", ErrInitDataBadSignature, err)
	}

	receivedHash := values.Get("hash")
	if receivedHash == "" {
		return InitData{}, fmt.Errorf("%w: missing hash", ErrInitDataBadSignature)
	}
	values.Del("hash")

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, k+"="+values.Get(k))
	}
	dataCheckString := strings.Join(pairs, "\n")

	secretKeyMAC := hmac.New(sha256.New, []byte("WebAppData"))
	secretKeyMAC.Write([]byte(botToken))
	secretKey := secretKeyMAC.Sum(nil)

	dataMAC := hmac.New(sha256.New, secretKey)
	dataMAC.Write([]byte(dataCheckString))
	computedHash := hex.EncodeToString(dataMAC.Sum(nil))

	if !hmac.Equal([]byte(computedHash), []byte(receivedHash)) {
		return InitData{}, fmt.Errorf("%w: hash mismatch", ErrInitDataBadSignature)
	}

	authDateUnix, err := strconv.ParseInt(values.Get("auth_date"), 10, 64)
	if err != nil {
		return InitData{}, fmt.Errorf("%w: missing or malformed auth_date", ErrInitDataBadSignature)
	}
	authDate := time.Unix(authDateUnix, 0).UTC()

	age := maxAuthAge
	if age <= 0 {
		age = DefaultMaxInitDataAge
	}
	if now.Sub(authDate) > age {
		return InitData{}, fmt.Errorf("%w: auth_date %s older than %s", ErrInitDataExpired, authDate, age)
	}

	var user InitDataUser
	if userJSON := values.Get("user"); userJSON != "" {
		var wire struct {
			ID              int64  `json:"id"`
			Username        string `json:"username"`
			AllowsWriteToPm *bool  `json:"allows_write_to_pm"`
		}
		if err := json.Unmarshal([]byte(userJSON), &wire); err != nil {
			return InitData{}, fmt.Errorf("%w: malformed user field: %v", ErrInitDataBadSignature, err)
		}
		user = InitDataUser{
			ID:              wire.ID,
			Username:        wire.Username,
			AllowsWriteToPm: wire.AllowsWriteToPm,
		}
	}

	return InitData{
		User:       user,
		AuthDate:   authDate,
		StartParam: values.Get("start_param"),
	}, nil
}
