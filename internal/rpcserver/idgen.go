package rpcserver

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// newBotID generates a random, effectively-unique bot ID for
// BotAdmin.CreateBot. raftcluster.CreateBotCommand.ID must be decided by
// the caller *before* Node.Apply (see command.go's determinism note —
// FSM.Apply never generates identifiers itself, so every replica agrees
// on the same ID), so this runs once per CreateBot call.
//
// crypto/rand + hex rather than a UUID library: 16 random bytes give the
// same collision resistance a random UUIDv4 does (122 bits of entropy
// either way) without adding a dependency — botmanager's go.mod has none
// today beyond what grpc/raft/prometheus already require.
func newBotID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("rpcserver: generate bot id: %w", err)
	}
	return "bot_" + hex.EncodeToString(buf[:]), nil
}
