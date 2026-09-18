// OmniWatch — Agent
// Component: tls (tests)
// Phase: industry-ready (IND-1)
// Purpose: Real-behavior unit tests for SPIFFE attestation, rotation, fallback gating
// Inputs: In-process stub Workload API, local test CA, temp cert files
// Outputs: go test results
package tls

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// testPKI is a throwaway CA + leaf factory. Every cert is really minted and
// really verified — no stubbed assertions anywhere in this file.
type testPKI struct {
	caDER   []byte
	caCert  *x509.Certificate
	caKey   *ecdsa.PrivateKey
	pool    *x509.CertPool
	spiffe  string
	counter int
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "omniwatch-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return &testPKI{caDER: caDER, caCert: caCert, caKey: caKey, pool: pool,
		spiffe: "spiffe://example.org/omniwatch/agent"}
}

// mintLeaf returns DER cert + PKCS8 DER key for a fresh SPIFFE-ID leaf.
func (p *testPKI) mintLeaf(t *testing.T) (certDER, keyDER []byte) {
	t.Helper()
	p.counter++
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(int64(100 + p.counter)),
		Subject:      pkix.Name{CommonName: fmt.Sprintf("omniwatch-agent-%d", p.counter)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:         mustParseURI(t, p.spiffe),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err = x509.CreateCertificate(rand.Reader, tpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	keyDER, err = x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return certDER, keyDER
}

func mustParseURI(t *testing.T, s string) []*url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatalf("parse URI %q: %v", s, err)
	}
	return []*url.URL{u}
}

// stubWorkloadServer is a REAL Workload API server (same proto the SPIRE
// agent speaks) serving one minted SVID over TCP so tests avoid AF_UNIX on
// Windows while exercising the identical client code path.
type stubWorkloadServer struct {
	workload.UnimplementedSpiffeWorkloadAPIServer
	spiffeID string
	svidDER  []byte
	keyDER   []byte
	bundle   []byte
}

func (s *stubWorkloadServer) FetchX509SVID(_ *workload.X509SVIDRequest, stream workload.SpiffeWorkloadAPI_FetchX509SVIDServer) error {
	return stream.Send(&workload.X509SVIDResponse{
		Svids: []*workload.X509SVID{{
			SpiffeId:    s.spiffeID,
			X509Svid:    s.svidDER,
			X509SvidKey: s.keyDER,
			Bundle:      s.bundle,
		}},
	})
}

// startStub serves the Workload API on 127.0.0.1:0 and returns its tcp:// address.
func startStub(t *testing.T, pki *testPKI, svidDER, keyDER []byte) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	workload.RegisterSpiffeWorkloadAPIServer(srv, &stubWorkloadServer{
		spiffeID: pki.spiffe, svidDER: svidDER, keyDER: keyDER, bundle: pki.caDER,
	})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return "tcp://" + lis.Addr().String()
}

// mtlsServer requires verified client certs and records the leaf it saw.
func mtlsServer(t *testing.T, pki *testPKI, srvDER, srvKeyDER []byte, seen chan<- []byte) (addr string, stop func()) {
	t.Helper()
	srvCert, err := tls.X509KeyPair(pemCert(srvDER), pemKey(srvKeyDER))
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	lis, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{srvCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pki.pool,
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				buf := make([]byte, 1)
				_, _ = conn.Read(buf)
				if tc, ok := conn.(*tls.Conn); ok {
					if st := tc.ConnectionState(); len(st.PeerCertificates) > 0 {
						select {
						case seen <- st.PeerCertificates[0].Raw:
						default:
						}
					}
				}
			}(c)
		}
	}()
	return lis.Addr().String(), func() { _ = lis.Close() }
}

// handshake performs the REAL gRPC client handshake through TransportCredentials.
func handshake(t *testing.T, creds credentials.TransportCredentials, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	wrapped, _, err := creds.ClientHandshake(ctx, "localhost", raw)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("client handshake: %v", err)
	}
	_, _ = wrapped.Write([]byte("x"))
	_ = wrapped.Close()
}

func TestSPIFFEAttestationSuccess(t *testing.T) {
	pki := newTestPKI(t)
	svidDER, keyDER := pki.mintLeaf(t)
	addr := startStub(t, pki, svidDER, keyDER)

	att, err := NewSPIFFEAttestor(addr, "example.org")
	if err != nil {
		t.Fatalf("NewSPIFFEAttestor: %v", err)
	}
	svid, err := att.FetchSVID(context.Background())
	if err != nil {
		t.Fatalf("FetchSVID: %v", err)
	}
	if got := svid.ID.String(); got != pki.spiffe {
		t.Fatalf("SVID ID = %q, want %q", got, pki.spiffe)
	}
	// The fetched leaf must chain to the test CA bundle.
	if _, err := svid.Certificates[0].Verify(x509.VerifyOptions{Roots: pki.pool}); err != nil {
		t.Fatalf("SVID does not verify against test CA: %v", err)
	}
}

func TestSPIFFEAttestationFailure(t *testing.T) {
	if _, err := NewSPIFFEAttestor("tcp://127.0.0.1:1", "not a trust domain!!"); err == nil {
		t.Fatal("expected trust-domain validation error, got nil")
	}
	if _, err := NewSPIFFEAttestor("::not-a-uri::", "example.org"); err == nil {
		t.Fatal("expected address validation error, got nil")
	}
	att, err := NewSPIFFEAttestor("tcp://127.0.0.1:1", "example.org")
	if err != nil {
		t.Fatalf("NewSPIFFEAttestor: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := att.FetchSVID(ctx); err == nil {
		t.Fatal("expected fetch error against dead endpoint, got nil")
	}
}

func TestMTLSHandshakeAgainstLocalCA(t *testing.T) {
	pki := newTestPKI(t)
	svidDER, keyDER := pki.mintLeaf(t)
	addr := startStub(t, pki, svidDER, keyDER)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	creds, err := NewTLSCredentials(ctx, Options{
		SpiffeSocketPath:     addr,
		TrustDomain:          "example.org",
		CertRotationInterval: time.Hour,
	}, slog.Default())
	if err != nil {
		t.Fatalf("NewTLSCredentials: %v", err)
	}
	defer creds.Close()

	srvDER, srvKeyDER := pki.mintLeaf(t)
	seen := make(chan []byte, 4)
	srvAddr, stop := mtlsServer(t, pki, srvDER, srvKeyDER, seen)
	defer stop()

	handshake(t, creds.ClientCredentials(), srvAddr)

	select {
	case raw := <-seen:
		leaf, err := x509.ParseCertificate(raw)
		if err != nil {
			t.Fatalf("parse peer cert: %v", err)
		}
		if len(leaf.URIs) == 0 || leaf.URIs[0].String() != pki.spiffe {
			t.Fatalf("server saw SPIFFE ID %v, want %q", leaf.URIs, pki.spiffe)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never observed the client certificate")
	}
}

func writePEMFile(t *testing.T, dir, name string, blockType string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func pemCert(der []byte) []byte { return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}) }
func pemKey(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func fileOpts(t *testing.T, pki *testPKI, dir string, certDER, keyDER []byte) Options {
	t.Helper()
	return Options{
		SpiffeSocketPath:     "tcp://127.0.0.1:1", // dead: forces file fallback
		TrustDomain:          "example.org",
		CertRotationInterval: time.Hour,
		CertFile:             writePEMFile(t, dir, "agent.crt", "CERTIFICATE", certDER),
		KeyFile:              writePEMFile(t, dir, "agent.key", "PRIVATE KEY", keyDER),
		CAFile:               writePEMFile(t, dir, "ca.crt", "CERTIFICATE", pki.caDER),
	}
}

func TestCertRotationReloadWithoutRestart(t *testing.T) {
	pki := newTestPKI(t)
	dir := t.TempDir()
	cert1, key1 := pki.mintLeaf(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	creds, err := NewTLSCredentials(ctx, fileOpts(t, pki, dir, cert1, key1), slog.Default())
	if err != nil {
		t.Fatalf("NewTLSCredentials: %v", err)
	}
	defer creds.Close()
	before := creds.LastRotation()

	srvDER, srvKeyDER := pki.mintLeaf(t)
	seen := make(chan []byte, 8)
	srvAddr, stop := mtlsServer(t, pki, srvDER, srvKeyDER, seen)
	defer stop()

	handshake(t, creds.ClientCredentials(), srvAddr)
	var first []byte
	select {
	case first = <-seen:
	case <-time.After(5 * time.Second):
		t.Fatal("no client cert observed before rotation")
	}

	// Rotate: overwrite the cert files with a FRESH leaf, reload in place.
	time.Sleep(10 * time.Millisecond)
	cert2, key2 := pki.mintLeaf(t)
	writePEMFile(t, dir, "agent.crt", "CERTIFICATE", cert2)
	writePEMFile(t, dir, "agent.key", "PRIVATE KEY", key2)
	if err := creds.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if !creds.LastRotation().After(before) {
		t.Fatalf("LastRotation did not advance: before=%v after=%v", before, creds.LastRotation())
	}

	handshake(t, creds.ClientCredentials(), srvAddr)
	select {
	case second := <-seen:
		if string(second) == string(first) {
			t.Fatal("server saw the SAME leaf after rotation — reload did not take effect")
		}
		if _, err := x509.ParseCertificate(second); err != nil {
			t.Fatalf("rotated peer cert unparsable: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no client cert observed after rotation")
	}
}

func TestRotationLoopFires(t *testing.T) {
	pki := newTestPKI(t)
	dir := t.TempDir()
	certDER, keyDER := pki.mintLeaf(t)
	opts := fileOpts(t, pki, dir, certDER, keyDER)
	opts.CertRotationInterval = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	creds, err := NewTLSCredentials(ctx, opts, slog.Default())
	if err != nil {
		t.Fatalf("NewTLSCredentials: %v", err)
	}
	defer creds.Close()
	before := creds.LastRotation()

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	creds.StartRotation(runCtx)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if creds.LastRotation().After(before) {
			return // ticker reloaded without restart
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotation loop never fired: LastRotation stuck at %v", before)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRotationSurvivesCancelledParent(t *testing.T) {
	pki := newTestPKI(t)
	dir := t.TempDir()
	certDER, keyDER := pki.mintLeaf(t)
	opts := fileOpts(t, pki, dir, certDER, keyDER)
	opts.CertRotationInterval = 50 * time.Millisecond

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer setupCancel()
	creds, err := NewTLSCredentials(setupCtx, opts, slog.Default())
	if err != nil {
		t.Fatalf("NewTLSCredentials: %v", err)
	}
	defer creds.Close()
	before := creds.LastRotation()

	// Mirror exporter.New wiring: the parent ctx is cancelled right after
	// setup (main.go expCancel). Rotation must still fire on the detached ctx.
	parent, cancel := context.WithCancel(context.Background())
	creds.StartRotation(context.WithoutCancel(parent))
	cancel()

	deadline := time.Now().Add(3 * time.Second)
	for {
		if creds.LastRotation().After(before) {
			return // detached rotation fired despite parent cancel
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotation died with parent ctx: LastRotation stuck at %v", before)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestInsecureFallbackGating(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	dead := Options{
		SpiffeSocketPath:     "tcp://127.0.0.1:1",
		TrustDomain:          "example.org",
		CertRotationInterval: time.Hour,
	}

	// Explicit insecure flag: insecure creds, no error, loud warning.
	creds, tc, err := Resolve(context.Background(), dead, true, logger)
	if err != nil {
		t.Fatalf("insecure Resolve: %v", err)
	}
	if tc != nil {
		t.Fatalf("insecure Resolve returned TLSCredentials, want nil")
	}
	if creds == nil {
		t.Fatal("insecure Resolve returned nil credentials")
	}
	if out := logs.String(); !strings.Contains(out, "dev only") {
		t.Fatalf("insecure fallback did not log the loud warning, logs=%q", out)
	}

	// mTLS mode with dead SPIRE and no files: MUST hard-fail, never silently insecure.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	creds, tc, err = Resolve(ctx, dead, false, logger)
	if err == nil {
		if tc != nil {
			_ = tc.Close()
		}
		t.Fatal("mTLS Resolve against dead SPIRE succeeded, want hard error")
	}
	if creds != nil {
		t.Fatal("mTLS Resolve returned credentials alongside error")
	}
}

func TestFileModeRejectsBadCA(t *testing.T) {
	pki := newTestPKI(t)
	dir := t.TempDir()
	certDER, keyDER := pki.mintLeaf(t)
	opts := fileOpts(t, pki, dir, certDER, keyDER)
	if err := os.WriteFile(opts.CAFile, []byte("not a pem"), 0o600); err != nil {
		t.Fatalf("corrupt CA: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := NewTLSCredentials(ctx, opts, slog.Default()); err == nil {
		t.Fatal("expected error for corrupt CA file, got nil")
	}
}
