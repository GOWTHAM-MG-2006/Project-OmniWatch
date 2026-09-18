// OmniWatch — Agent
// Component: auth (AuthZ/AuthN for health endpoints)
// Phase: industry-ready (IND-3)
// Purpose: Authenticator chain (mTLS client-cert verification, API key header, dev bypass) for /health + /ready
// Inputs: HTTP requests (TLS connection state, X-OmniWatch-Key header), auth mode + API keys + trusted CA file
// Outputs: nil on success, typed AuthError on failure; Middleware enforcing 401 JSON responses
package auth

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Modes for the auth section. OMNIWATCH_AUTH_MODE=dev disables all auth.
const (
	ModeDev    = "dev"
	ModeAPIKey = "apikey"
	ModeMTLS   = "mtls"
)

// APIKeyHeader is the only header API keys are accepted on.
const APIKeyHeader = "X-OmniWatch-Key"

// AuthError is returned by Authenticate on failure; Code is the HTTP status
// the middleware maps it to (always 401 for auth failures).
type AuthError struct {
	Reason string
	Code   int
}

func (e *AuthError) Error() string { return e.Reason }

func unauthorized(reason string) *AuthError {
	return &AuthError{Reason: reason, Code: http.StatusUnauthorized}
}

// Authenticator verifies a single inbound request.
type Authenticator interface {
	Authenticate(r *http.Request) error
	Mode() string
}

// DevAuthenticator allows every request (OMNIWATCH_AUTH_MODE=dev).
type DevAuthenticator struct{}

func (DevAuthenticator) Authenticate(_ *http.Request) error { return nil }
func (DevAuthenticator) Mode() string                       { return ModeDev }

// APIKeyAuthenticator validates the X-OmniWatch-Key header against a set of
// keys loaded from ENV only (OMNIWATCH_AUTH_API_KEYS comma-list).
type APIKeyAuthenticator struct {
	keys map[string]struct{}
}

// NewAPIKeyAuthenticator builds an authenticator from raw key material.
// Empty/blank entries are ignored; an empty set rejects every request.
func NewAPIKeyAuthenticator(keys []string) *APIKeyAuthenticator {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k != "" {
			set[k] = struct{}{}
		}
	}
	return &APIKeyAuthenticator{keys: set}
}

func (a *APIKeyAuthenticator) Authenticate(r *http.Request) error {
	if len(a.keys) == 0 {
		return unauthorized("apikey auth: no keys configured")
	}
	got := strings.TrimSpace(r.Header.Get(APIKeyHeader))
	if got == "" {
		return unauthorized("apikey auth: missing credentials")
	}
	if _, ok := a.keys[got]; !ok {
		return unauthorized("apikey auth: invalid credentials")
	}
	return nil
}

func (a *APIKeyAuthenticator) Mode() string { return ModeAPIKey }

// MTLSAuthenticator verifies the client certificate presented on the TLS
// connection. Trust roots come from trusted_ca_file; when no CA file is
// configured it requires a verified peer chain against the host roots.
// It reuses the stdlib x509 verification (same primitive the internal/tls
// SPIFFE credentials build on) rather than reimplementing cert parsing.
type MTLSAuthenticator struct {
	pool *x509.CertPool
}

// NewMTLSAuthenticator loads trust roots from caFile. An empty caFile means
// "use host roots"; the file must parse when provided.
func NewMTLSAuthenticator(caFile string) (*MTLSAuthenticator, error) {
	if strings.TrimSpace(caFile) == "" {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("mtls auth: system cert pool: %w", err)
		}
		return &MTLSAuthenticator{pool: pool}, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("mtls auth: read trusted CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("mtls auth: no valid certs in CA file %s", caFile)
	}
	return &MTLSAuthenticator{pool: pool}, nil
}

func (a *MTLSAuthenticator) Authenticate(r *http.Request) error {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return unauthorized("mtls auth: no client certificate presented")
	}
	leaf := r.TLS.PeerCertificates[0]
	opts := x509.VerifyOptions{Roots: a.pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if _, err := leaf.Verify(opts); err != nil {
		return unauthorized("mtls auth: client certificate verification failed (" + err.Error() + ")")
	}
	return nil
}

func (a *MTLSAuthenticator) Mode() string { return ModeMTLS }

// Chain tries each authenticator in order (mTLS -> API key -> dev bypass):
// the first nil error wins. It powers multi-verifier setups; single-mode
// deployments are built via NewForMode.
type Chain struct {
	chain []Authenticator
}

// NewChain builds an ordered chain; at least one member is required.
func NewChain(auths ...Authenticator) (*Chain, error) {
	if len(auths) == 0 {
		return nil, errors.New("auth: chain requires at least one authenticator")
	}
	return &Chain{chain: auths}, nil
}

func (c *Chain) Authenticate(r *http.Request) error {
	var last error
	for _, a := range c.chain {
		if err := a.Authenticate(r); err == nil {
			return nil
		} else {
			last = err
		}
	}
	if last == nil {
		return unauthorized("auth: denied")
	}
	return last
}

func (c *Chain) Mode() string { return "chain" }

// NewForMode builds the authenticator for a config mode string.
// Unknown modes fall back to dev bypass so probes never hard-fail on typo;
// callers that need strictness should validate the mode beforehand.
func NewForMode(mode string, apiKeys []string, trustedCAFile string) (Authenticator, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", ModeDev:
		return DevAuthenticator{}, nil
	case ModeAPIKey:
		return NewAPIKeyAuthenticator(apiKeys), nil
	case ModeMTLS:
		return NewMTLSAuthenticator(trustedCAFile)
	default:
		return DevAuthenticator{}, fmt.Errorf("auth: unknown mode %q, falling back to dev bypass", mode)
	}
}

// Middleware enforces auth on every request: failures get 401 JSON,
// successes pass through to next.
func Middleware(auth Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if auth == nil {
				next.ServeHTTP(w, r)
				return
			}
			if err := auth.Authenticate(r); err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = fmt.Fprintf(w, `{"status":"unauthorized","error":%q}`, err.Error())
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
