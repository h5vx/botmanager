package raftcluster

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// encryptedTokenPrefix marks a Bot.Token value encrypted by TokenCipher.
// Values without it are plaintext (written with no key configured, or by
// an older version) and are returned as-is, so a key can be introduced on a
// running cluster without a migration step: new and updated tokens get
// encrypted, old ones keep working until they are next updated.
const encryptedTokenPrefix = "enc:v1:"

// TokenCipher encrypts bot tokens with AES-256-GCM before they enter the
// Raft log, so tokens are never stored in plaintext in BoltDB, snapshots or
// state exports, and never travel between nodes in plaintext. Every node of
// a cluster must use the same key.
type TokenCipher struct {
	aead cipher.AEAD
}

// NewTokenCipher builds a TokenCipher from a 32-byte key.
func NewTokenCipher(key []byte) (*TokenCipher, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("raftcluster: token key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: token cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: token cipher: %w", err)
	}
	return &TokenCipher{aead: aead}, nil
}

// Encrypt returns the encrypted form of plain. An empty token stays empty.
func (c *TokenCipher) Encrypt(plain string) (string, error) {
	if plain == "" || strings.HasPrefix(plain, encryptedTokenPrefix) {
		return plain, nil
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("raftcluster: token nonce: %w", err)
	}
	sealed := c.aead.Seal(nonce, nonce, []byte(plain), nil)
	return encryptedTokenPrefix + base64.RawStdEncoding.EncodeToString(sealed), nil
}

// Decrypt reverses Encrypt. Values without the encrypted prefix are
// returned unchanged (see encryptedTokenPrefix).
func (c *TokenCipher) Decrypt(value string) (string, error) {
	if !strings.HasPrefix(value, encryptedTokenPrefix) {
		return value, nil
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, encryptedTokenPrefix))
	if err != nil {
		return "", fmt.Errorf("raftcluster: decode token: %w", err)
	}
	ns := c.aead.NonceSize()
	if len(raw) < ns {
		return "", errors.New("raftcluster: encrypted token too short")
	}
	plain, err := c.aead.Open(nil, raw[:ns], raw[ns:], nil)
	if err != nil {
		return "", fmt.Errorf("raftcluster: decrypt token (wrong key?): %w", err)
	}
	return string(plain), nil
}
