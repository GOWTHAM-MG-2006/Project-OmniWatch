// OmniWatch — Agent
// Component: tls (mTLS handshake proof)
// Phase: industry-ready (IND-1)
// Purpose: Prove mutual TLS handshake with self-signed CA via agent TLS config
// Inputs: Temp CA + server/client certs minted in-test
// Outputs: go test result (mutual verification OK, no-cert client rejected)
package tls

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
)

// Future-checklist item 2: mTLS handshake proof for omniwatch-agent.
// Mints a throwaway CA + two leaf certs (server + client), starts a real
// gRPC server requiring client certs, dials it with the agent's own
// TLSCredentials (file-fallback path, same code OTLP export uses), and
// verifies mutual verification. No SPIRE server, no cluster, no new deps.

func hsMakeCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, *x509.CertPool, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "omniwatch-hs-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return ca, key, pool, der
}

func hsMintLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn, spiffeID string) (certDER, keyDER []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	var uris []*url.URL
	if spiffeID != "" {
		u, err := url.Parse(spiffeID)
		if err != nil {
			t.Fatalf("parse SPIFFE ID: %v", err)
		}
		uris = []*url.URL{u}
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         uris,
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err = x509.CreateCertificate(rand.Reader, tpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf %q: %v", cn, err)
	}
	keyDER, err = x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key %q: %v", cn, err)
	}
	return certDER, keyDER
}

func hsWritePEM(t *testing.T, dir, name, blockType string, der []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func hsWaitReady(t *testing.T, cc *grpc.ClientConn, wantReady bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		st := cc.GetState()
		if wantReady && st == connectivity.Ready {
			return
		}
		if !cc.WaitForStateChange(ctx, st) {
			if wantReady {
				t.Fatalf("client never reached READY (stuck at %v)", cc.GetState())
			}
			if cc.GetState() == connectivity.Ready {
				t.Fatal("uncertified client reached READY, want rejection")
			}
			return
		}
		if !wantReady && cc.GetState() == connectivity.Ready {
			t.Fatal("uncertified client reached READY, want rejection")
		}
	}
}

func TestMTLSHandshakeMutualVerification(t *testing.T) {
	ca, caKey, pool, caDER := hsMakeCA(t)
	srvDER, srvKeyDER := hsMintLeaf(t, ca, caKey, "hs-server", "spiffe://example.org/omniwatch/hs-server")
	cliDER, cliKeyDER := hsMintLeaf(t, ca, caKey, "hs-client", "spiffe://example.org/omniwatch/hs-client")

	dir := t.TempDir()
	cliCertFile := hsWritePEM(t, dir, "client.crt", "CERTIFICATE", cliDER)
	cliKeyFile := hsWritePEM(t, dir, "client.key", "PRIVATE KEY", cliKeyDER)
	caFile := hsWritePEM(t, dir, "ca.crt", "CERTIFICATE", caDER)

	// gRPC server: presents server cert, REQUIRES + verifies client cert.
	srvCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: srvKeyDER}),
	)
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	seenClient := make(chan []byte, 4)
	srvCreds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{srvCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return x509.CertificateInvalidError{}
			}
			select {
			case seenClient <- raw[0]:
			default:
			}
			leaf, err := x509.ParseCertificate(raw[0])
			if err != nil {
				return err
			}
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
				return err
			}
			return nil
		},
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer(grpc.Creds(srvCreds))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	addr := lis.Addr().String()

	// gRPC client: built from the AGENT's own TLS config (file-fallback
	// path of NewTLSCredentials — same constructor OTLP export uses).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	agentCreds, err := NewTLSCredentials(ctx, Options{
		SpiffeSocketPath:     "tcp://127.0.0.1:1", // dead: forces file fallback
		TrustDomain:          "example.org",
		CertRotationInterval: time.Hour,
		CertFile:             cliCertFile,
		KeyFile:              cliKeyFile,
		CAFile:               caFile,
	}, nil)
	if err != nil {
		t.Fatalf("NewTLSCredentials: %v", err)
	}
	defer agentCreds.Close()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(agentCreds.ClientCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer cc.Close()
	cc.Connect()
	hsWaitReady(t, cc, true)

	// Mutual verification: server saw the client's SPIFFE ID.
	select {
	case raw := <-seenClient:
		leaf, err := x509.ParseCertificate(raw)
		if err != nil {
			t.Fatalf("parse observed client cert: %v", err)
		}
		if len(leaf.URIs) == 0 || leaf.URIs[0].String() != "spiffe://example.org/omniwatch/hs-client" {
			t.Fatalf("server saw client SPIFFE IDs %v, want hs-client ID", leaf.URIs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never observed the client certificate")
	}
}

func TestMTLSHandshakeRejectsUncertifiedClient(t *testing.T) {
	ca, caKey, pool, _ := hsMakeCA(t)
	srvDER, srvKeyDER := hsMintLeaf(t, ca, caKey, "hs-server", "spiffe://example.org/omniwatch/hs-server")

	srvCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: srvKeyDER}),
	)
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{srvCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	})))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	// Client presents NO certificate — only trusts the CA.
	naked := credentials.NewTLS(&tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
		ServerName: "localhost",
	})
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(naked))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer cc.Close()
	cc.Connect()
	hsWaitReady(t, cc, false)
}
