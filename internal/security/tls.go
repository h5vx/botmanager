// Package security builds the mutual-TLS configurations botmanager uses for
// its gRPC API, for forwarding writes between nodes, and for the Raft
// transport, and loads the bot-token encryption key.
//
// The trust model is a single private CA: every node and every API client
// presents a certificate signed by it, and any such certificate is
// authorized. Issue client certificates only to trusted services.
package security

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
)

// MTLS holds the server- and client-side TLS configurations derived from
// one CA plus one certificate/key pair.
type MTLS struct {
	Server *tls.Config
	Client *tls.Config
}

// LoadMTLS reads PEM files and builds an MTLS.
func LoadMTLS(caFile, certFile, keyFile string) (*MTLS, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("security: read ca file: %w", err)
	}
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("security: read cert file: %w", err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("security: read key file: %w", err)
	}
	return NewMTLS(caPEM, certPEM, keyPEM)
}

// NewMTLS builds an MTLS from PEM-encoded CA certificate(s), certificate
// and private key.
func NewMTLS(caPEM, certPEM, keyPEM []byte) (*MTLS, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("security: no CA certificates found")
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("security: load key pair: %w", err)
	}
	return &MTLS{
		Server: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{cert},
			ClientCAs:    pool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		},
		Client: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
		},
	}, nil
}

// LoadTokenKey reads a 32-byte AES-256 key stored base64-encoded (standard
// or URL alphabet, padding optional) in path.
func LoadTokenKey(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("security: read token key: %w", err)
	}
	s := strings.TrimSpace(string(raw))
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if key, err := enc.DecodeString(s); err == nil {
			if len(key) != 32 {
				return nil, fmt.Errorf("security: token key must decode to 32 bytes, got %d", len(key))
			}
			return key, nil
		}
	}
	return nil, fmt.Errorf("security: token key file must contain base64-encoded 32 bytes")
}
