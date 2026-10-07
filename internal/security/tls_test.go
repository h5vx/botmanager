package security

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/devcerts"
)

func TestLoadTokenKey(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	good := filepath.Join(dir, "good.key")
	_ = os.WriteFile(good, []byte(base64.StdEncoding.EncodeToString(key)+"\n"), 0o600)
	got, err := LoadTokenKey(good)
	if err != nil || string(got) != string(key) {
		t.Fatalf("LoadTokenKey = %v, %v", got, err)
	}

	short := filepath.Join(dir, "short.key")
	_ = os.WriteFile(short, []byte(base64.StdEncoding.EncodeToString(key[:16])), 0o600)
	if _, err := LoadTokenKey(short); err == nil {
		t.Fatal("16-byte key accepted")
	}
	garbage := filepath.Join(dir, "garbage.key")
	_ = os.WriteFile(garbage, []byte("not base64 !!!"), 0o600)
	if _, err := LoadTokenKey(garbage); err == nil {
		t.Fatal("garbage key accepted")
	}
}

func TestNewMTLS(t *testing.T) {
	ca, err := devcerts.NewCA("ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue("node", []string{"127.0.0.1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMTLS(ca.CertPEM, leaf.CertPEM, leaf.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	if m.Server.ClientCAs == nil || m.Client.RootCAs == nil || len(m.Server.Certificates) != 1 {
		t.Fatalf("incomplete config: %+v", m)
	}
	if _, err := NewMTLS([]byte("x"), leaf.CertPEM, leaf.KeyPEM); err == nil {
		t.Fatal("bad CA accepted")
	}
	if _, err := NewMTLS(ca.CertPEM, leaf.CertPEM, ca.KeyPEM); err == nil {
		t.Fatal("mismatched key accepted")
	}
}
