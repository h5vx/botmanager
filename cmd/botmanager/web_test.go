package main

import (
	"bytes"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestRunHashPassword(t *testing.T) {
	var out bytes.Buffer
	if err := runHashPassword(strings.NewReader("correct horse battery\n"), &out); err != nil {
		t.Fatal(err)
	}
	hash := strings.TrimSpace(out.String())
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte("correct horse battery")) != nil {
		t.Fatalf("hash %q does not match the password", hash)
	}
	if err := runHashPassword(strings.NewReader("short\n"), &out); err == nil {
		t.Fatal("short password accepted")
	}
}
