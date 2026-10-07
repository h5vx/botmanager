// Command botmanager-certs creates a private CA, node/client certificates
// signed by it, and a bot-token encryption key — everything
// security.* in the botmanager config needs, for development and for small
// deployments without their own PKI.
//
//	botmanager-certs init  -out certs
//	botmanager-certs issue -out certs -name node-1 -hosts 10.0.0.1,bm-1.internal
//	botmanager-certs issue -out certs -name my-service
//
// init writes ca.pem, ca-key.pem and token.key; issue writes <name>.pem and
// <name>-key.pem. Every issued certificate works both as a server and as a
// client certificate. Keep ca-key.pem and token.key secret.
package main

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/h5vx/botmanager/internal/devcerts"
)

const (
	caValidity   = 10 * 365 * 24 * time.Hour
	leafValidity = 2 * 365 * 24 * time.Hour
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "init":
		err = runInit(os.Args[2:])
	case "issue":
		err = runIssue(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "botmanager-certs:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: botmanager-certs init -out DIR | issue -out DIR -name NAME [-hosts h1,h2]")
	os.Exit(2)
}

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	out := fs.String("out", "certs", "output directory")
	_ = fs.Parse(args)

	if err := os.MkdirAll(*out, 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(*out, "ca.pem")); err == nil {
		return errors.New(filepath.Join(*out, "ca.pem") + " already exists; refusing to overwrite")
	}
	ca, err := devcerts.NewCA("botmanager CA", caValidity)
	if err != nil {
		return err
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	files := map[string][]byte{
		"ca.pem":     ca.CertPEM,
		"ca-key.pem": ca.KeyPEM,
		"token.key":  []byte(base64.StdEncoding.EncodeToString(key) + "\n"),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(*out, name), data, 0o600); err != nil {
			return err
		}
	}
	fmt.Printf("wrote %s/{ca.pem,ca-key.pem,token.key}\n", *out)
	return nil
}

func runIssue(args []string) error {
	fs := flag.NewFlagSet("issue", flag.ExitOnError)
	out := fs.String("out", "certs", "directory with ca.pem/ca-key.pem; output goes here too")
	name := fs.String("name", "", "certificate name (node id or client service name)")
	hosts := fs.String("hosts", "", "comma-separated DNS names/IPs the certificate is valid for (nodes need their advertise hosts)")
	_ = fs.Parse(args)

	if *name == "" {
		return errors.New("-name is required")
	}
	certPEM, err := os.ReadFile(filepath.Join(*out, "ca.pem"))
	if err != nil {
		return err
	}
	keyPEM, err := os.ReadFile(filepath.Join(*out, "ca-key.pem"))
	if err != nil {
		return err
	}
	ca, err := devcerts.LoadCA(certPEM, keyPEM)
	if err != nil {
		return err
	}

	var hostList []string
	for _, h := range strings.Split(*hosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			hostList = append(hostList, h)
		}
	}
	leaf, err := ca.Issue(*name, hostList, leafValidity)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, *name+".pem"), leaf.CertPEM, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, *name+"-key.pem"), leaf.KeyPEM, 0o600); err != nil {
		return err
	}
	fmt.Printf("wrote %s/%s.pem and %s/%s-key.pem\n", *out, *name, *out, *name)
	return nil
}
