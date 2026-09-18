// OmniWatch — Agent
// Component: auth tests
// Phase: industry-ready (IND-3)
// Purpose: Table tests for API key validation, mTLS verification, dev bypass, and chain ordering
// Inputs: Synthetic http.Requests, temp CA files, self-signed test certificates
// Outputs: go test ./internal/auth/... verdicts
package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func reqWithKey(key string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	if key != "" {
		r.Header.Set(APIKeyHeader, key)
	}
	return r
}

func TestAPIKeyModes(t *testing.T) {
	auth := NewAPIKeyAuthenticator([]string{"valid", " second "})
	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"valid key", "valid", false},
		{"valid key trimmed", "second", false},
		{"invalid key", "wrong", true},
		{"missing key", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := auth.Authenticate(reqWithKey(tc.key))
			if (err != nil) != tc.wantErr {
				t.Fatalf("Authenticate() err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestAPIKeyEmptySetRejects(t *testing.T) {
	auth := NewAPIKeyAuthenticator(nil)
	if err := auth.Authenticate(reqWithKey("anything")); err == nil {
		t.Fatal("expected error with no keys configured")
	}
}

func TestDevBypass(t *testing.T) {
	var d DevAuthenticator
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	if err := d.Authenticate(r); err != nil {
		t.Fatalf("dev bypass must allow all: %v", err)
	}
}

func TestMiddlewareMatrix(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	auth := NewAPIKeyAuthenticator([]string{"valid"})
	h := Middleware(auth)(next)

	for _, tc := range []struct {
		name string
		key  string
		want int
	}{
		{"valid -> 200", "valid", http.StatusOK},
		{"invalid -> 401", "bad", http.StatusUnauthorized},
		{"missing -> 401", "", http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, reqWithKey(tc.key))
			if rec.Code != tc.want {
				t.Fatalf("got %d want %d", rec.Code, tc.want)
			}
		})
	}

	// dev mode: no header -> 200
	dev := Middleware(DevAuthenticator{})(next)
	rec := httptest.NewRecorder()
	dev.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("dev bypass got %d want 200", rec.Code)
	}
}

// --- mTLS helpers: self-signed CA + leaf certs ---

func testCA(t *testing.T) (caPEM []byte, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return pemEncode(t, der, caKey), caCert, caKey
}

func pemEncode(t *testing.T, caDER []byte, _ *ecdsa.PrivateKey) []byte {
	t.Helper()
	return certDERToPEM(caDER)
}

func certDERToPEM(der []byte) []byte {
	return pemWrap(der, "CERTIFICATE")
}

func pemWrap(der []byte, typ string) []byte {
	// minimal PEM writer without extra imports
	const lineLen = 64
	enc := base64.StdEncoding.EncodeToString(der)
	out := "-----BEGIN " + typ + "-----\n"
	for i := 0; i < len(enc); i += lineLen {
		end := i + lineLen
		if end > len(enc) {
			end = len(enc)
		}
		out += enc[i:end] + "\n"
	}
	out += "-----END " + typ + "-----\n"
	return []byte(out)
}

func leafCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, cn string) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{cn},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return leaf
}

func TestMTLSVerify(t *testing.T) {
	caPEM, caCert, caKey := testCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := NewMTLSAuthenticator(caFile)
	if err != nil {
		t.Fatal(err)
	}

	good := leafCert(t, caCert, caKey, "good-client")
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{good}}
	if err := auth.Authenticate(r); err != nil {
		t.Fatalf("good cert must pass: %v", err)
	}

	t.Run("bad cert rejected", func(t *testing.T) {
		otherPEM, otherCert, otherKey := testCA(t)
		_ = otherPEM
		evil := leafCert(t, otherCert, otherKey, "evil-client")
		rb := httptest.NewRequest(http.MethodGet, "/health", nil)
		rb.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{evil}}
		if err := auth.Authenticate(rb); err == nil {
			t.Fatal("expected bad-cert rejection")
		}
	})

	t.Run("missing cert rejected", func(t *testing.T) {
		rn := httptest.NewRequest(http.MethodGet, "/health", nil)
		if err := auth.Authenticate(rn); err == nil {
			t.Fatal("expected missing-cert rejection")
		}
	})

	t.Run("bad ca file errors", func(t *testing.T) {
		if _, err := NewMTLSAuthenticator(filepath.Join(dir, "missing.pem")); err == nil {
			t.Fatal("expected error for missing CA file")
		}
		badFile := filepath.Join(dir, "bad.pem")
		if err := os.WriteFile(badFile, []byte("not a cert"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewMTLSAuthenticator(badFile); err == nil {
			t.Fatal("expected error for invalid CA PEM")
		}
	})
}

func TestChainOrdering(t *testing.T) {
	// mTLS-first chain: request with valid API key passes even without cert.
	api := NewAPIKeyAuthenticator([]string{"k1"})
	mtls, err := NewMTLSAuthenticator("")
	if err != nil {
		t.Skipf("no system pool: %v", err)
	}
	ch, err := NewChain(mtls, api, DevAuthenticator{})
	if err != nil {
		t.Fatal(err)
	}
	// API key path (no TLS state) must succeed via second member.
	if err := ch.Authenticate(reqWithKey("k1")); err != nil {
		t.Fatalf("chain should pass via apikey member: %v", err)
	}
	// Garbage request fails only when dev is not in the chain.
	strict, _ := NewChain(mtls, NewAPIKeyAuthenticator(nil))
	if err := strict.Authenticate(httptest.NewRequest(http.MethodGet, "/health", nil)); err == nil {
		t.Fatal("strict chain must reject anonymous request")
	}
}

func TestNewForMode(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want string
	}{
		{"dev", ModeDev},
		{"", ModeDev},
		{"apikey", ModeAPIKey},
		{"mtls", ModeMTLS},
	} {
		got, err := NewForMode(tc.mode, []string{"k"}, "")
		if tc.mode == "mtls" {
			// mtls with empty CA uses host roots; may fail only on broken pools.
			if err != nil {
				t.Skipf("mtls init: %v", err)
			}
		}
		if got.Mode() != tc.want {
			t.Fatalf("mode %q: got %q want %q", tc.mode, got.Mode(), tc.want)
		}
	}
}
