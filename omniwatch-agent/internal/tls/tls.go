// OmniWatch — Agent
// Component: tls (mTLS credentials + SPIRE workload attestation)
// Phase: industry-ready (IND-1)
// Purpose: Build reloadable mTLS client credentials for OTLP gRPC export
// Inputs: TLS options (SPIRE socket path, trust domain, rotation interval, optional cert files) + insecure flag
// Outputs: gRPC transport credentials (mTLS via SPIFFE SVID or insecure fallback when explicitly flagged)
package tls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// DefaultSpiffeSocketPath is the SPIRE agent Workload API endpoint.
	DefaultSpiffeSocketPath = "unix:///tmp/spire-agent/public/api.sock"
	// DefaultTrustDomain is the SPIFFE trust domain (matches k8s/spire-agent config).
	DefaultTrustDomain = "example.org"
	// DefaultCertRotationInterval reloads the workload identity without restart.
	DefaultCertRotationInterval = 24 * time.Hour
	// fetchTimeout bounds a single Workload API attestation attempt.
	fetchTimeout = 5 * time.Second
	// InsecureFallbackWarning is logged whenever mTLS is bypassed via the env flag.
	InsecureFallbackWarning = "WARN mTLS disabled via insecure flag — dev only"
)

// Options configures how mTLS client credentials are obtained.
type Options struct {
	// SpiffeSocketPath is the Workload API address (unix:///… or tcp://…).
	SpiffeSocketPath string
	// TrustDomain is the SPIFFE trust domain (e.g. example.org).
	TrustDomain string
	// CertRotationInterval reloads the identity on this cadence without restart.
	CertRotationInterval time.Duration
	// CertFile/KeyFile/CAFile optionally pin a file-based identity used when
	// the SPIRE agent is unreachable (dev/test). Empty means SPIRE-only.
	CertFile string
	KeyFile  string
	CAFile   string
}

// DefaultOptions returns production-safe defaults: 24h rotation, standard
// SPIRE socket, no insecure behavior.
func DefaultOptions() Options {
	return Options{
		SpiffeSocketPath:     DefaultSpiffeSocketPath,
		TrustDomain:          DefaultTrustDomain,
		CertRotationInterval: DefaultCertRotationInterval,
	}
}

// SPIFFEAttestor fetches X.509 SVIDs from the SPIRE Workload API.
type SPIFFEAttestor struct {
	socketPath  string
	trustDomain spiffeid.TrustDomain
}

// NewSPIFFEAttestor validates the socket address and trust domain.
func NewSPIFFEAttestor(socketPath, trustDomain string) (*SPIFFEAttestor, error) {
	if socketPath == "" {
		return nil, fmt.Errorf("tls: SPIFFE socket path is empty")
	}
	if err := workloadapi.ValidateAddress(socketPath); err != nil {
		return nil, fmt.Errorf("tls: invalid SPIFFE socket address %q: %w", socketPath, err)
	}
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return nil, fmt.Errorf("tls: invalid trust domain %q: %w", trustDomain, err)
	}
	return &SPIFFEAttestor{socketPath: socketPath, trustDomain: td}, nil
}

// FetchSVID attests the workload and returns its first X.509 SVID.
func (a *SPIFFEAttestor) FetchSVID(ctx context.Context) (*x509svid.SVID, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	client, err := workloadapi.New(ctx, workloadapi.WithAddr(a.socketPath))
	if err != nil {
		return nil, fmt.Errorf("tls: workload API client: %w", err)
	}
	defer client.Close()
	svid, err := client.FetchX509SVID(ctx)
	if err != nil {
		return nil, fmt.Errorf("tls: fetch X509 SVID: %w", err)
	}
	return svid, nil
}

// TrustDomain returns the configured trust domain.
func (a *SPIFFEAttestor) TrustDomain() spiffeid.TrustDomain { return a.trustDomain }

// SocketPath returns the configured Workload API address.
func (a *SPIFFEAttestor) SocketPath() string { return a.socketPath }

// TLSCredentials holds reloadable mTLS client credentials for gRPC dial.
// In SPIRE mode the underlying X509Source streams updates from the Workload
// API; in file mode the identity is re-read from disk on every Reload, so
// rotation never requires a process restart.
type TLSCredentials struct {
	mu       sync.RWMutex
	opts     Options
	logger   *slog.Logger
	source   *workloadapi.X509Source // non-nil in SPIRE mode
	tlsCfg   *tls.Config
	lastRot  time.Time
	closed   bool
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewTLSCredentials attests via SPIRE (preferred) or loads file-based certs.
func NewTLSCredentials(ctx context.Context, opts Options, logger *slog.Logger) (*TLSCredentials, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if opts.SpiffeSocketPath == "" {
		opts.SpiffeSocketPath = DefaultSpiffeSocketPath
	}
	if opts.TrustDomain == "" {
		opts.TrustDomain = DefaultTrustDomain
	}
	if opts.CertRotationInterval <= 0 {
		opts.CertRotationInterval = DefaultCertRotationInterval
	}
	c := &TLSCredentials{opts: opts, logger: logger, stopCh: make(chan struct{})}

	// Preferred: live X509Source backed by the SPIRE Workload API.
	src, err := workloadapi.NewX509Source(ctx,
		workloadapi.WithClientOptions(workloadapi.WithAddr(opts.SpiffeSocketPath)))
	if err == nil {
		if err := src.WaitUntilUpdated(ctx); err == nil {
			c.source = src
			c.tlsCfg = tlsconfig.MTLSClientConfig(src, src, tlsconfig.AuthorizeAny())
			c.lastRot = time.Now()
			logger.Info("tls: mTLS identity acquired from SPIRE",
				"socket", opts.SpiffeSocketPath, "trust_domain", opts.TrustDomain)
			return c, nil
		}
		src.Close()
		logger.Warn("tls: SPIRE source never updated, trying file identity", "error", err)
	} else {
		logger.Warn("tls: SPIRE X509Source unavailable, trying file identity", "error", err)
	}

	// Fallback: file-based identity (dev/test only path when SPIRE is absent).
	if opts.CertFile == "" || opts.KeyFile == "" {
		return nil, fmt.Errorf("tls: no workload identity: SPIRE unreachable at %s and no cert files configured",
			opts.SpiffeSocketPath)
	}
	if err := c.reloadFiles(); err != nil {
		return nil, err
	}
	logger.Info("tls: mTLS identity loaded from files",
		"cert", opts.CertFile, "trust_domain", opts.TrustDomain)
	return c, nil
}

// ClientCredentials returns gRPC transport credentials backed by the current identity.
func (c *TLSCredentials) ClientCredentials() credentials.TransportCredentials {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return credentials.NewTLS(withGRPCDefaults(c.tlsCfg.Clone()))
}

// withGRPCDefaults ensures the config negotiates HTTP/2 (grpc-go >= 1.67
// rejects handshakes without a selected ALPN protocol).
func withGRPCDefaults(cfg *tls.Config) *tls.Config {
	for _, p := range cfg.NextProtos {
		if p == "h2" {
			return cfg
		}
	}
	cfg.NextProtos = append([]string{"h2"}, cfg.NextProtos...)
	return cfg
}

// Reload re-fetches the identity (SVID refresh in SPIRE mode, disk re-read in
// file mode) and stamps LastRotation. No restart required.
func (c *TLSCredentials) Reload() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("tls: credentials closed")
	}
	if c.source != nil {
		svid, err := c.source.GetX509SVID()
		if err != nil {
			return fmt.Errorf("tls: reload SVID: %w", err)
		}
		_ = svid
		c.tlsCfg = tlsconfig.MTLSClientConfig(c.source, c.source, tlsconfig.AuthorizeAny())
		c.lastRot = time.Now()
		c.logger.Info("tls: identity reloaded from SPIRE")
		return nil
	}
	return c.reloadFilesLocked()
}

// LastRotation reports when the identity was last (re)loaded.
func (c *TLSCredentials) LastRotation() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastRot
}

// StartRotation reloads the identity on every CertRotationInterval tick until
// ctx is done or Close is called. Safe to call once.
func (c *TLSCredentials) StartRotation(ctx context.Context) {
	go func() {
		t := time.NewTicker(c.opts.CertRotationInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-t.C:
				if err := c.Reload(); err != nil {
					c.logger.Warn("tls: scheduled rotation failed", "error", err)
				}
			}
		}
	}()
}

// Close releases the underlying X509Source and stops rotation.
func (c *TLSCredentials) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	c.stopOnce.Do(func() { close(c.stopCh) })
	if c.source != nil {
		err = c.source.Close()
		c.source = nil
	}
	c.closed = true
	return err
}

func (c *TLSCredentials) reloadFiles() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reloadFilesLocked()
}

func (c *TLSCredentials) reloadFilesLocked() error {
	cert, err := tls.LoadX509KeyPair(c.opts.CertFile, c.opts.KeyFile)
	if err != nil {
		return fmt.Errorf("tls: load cert files: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if c.opts.CAFile != "" {
		pem, err := os.ReadFile(c.opts.CAFile)
		if err != nil {
			return fmt.Errorf("tls: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("tls: no valid certs in CA file %s", c.opts.CAFile)
		}
		cfg.RootCAs = pool
	}
	c.tlsCfg = cfg
	c.lastRot = time.Now()
	c.logger.Info("tls: file identity reloaded")
	return nil
}

// Resolve builds gRPC dial credentials from opts. When insecure is true it
// returns insecure credentials and logs a loud warning (dev-only fallback);
// otherwise it returns mTLS credentials backed by SPIRE/file identity.
func Resolve(ctx context.Context, opts Options, insecureFlag bool, logger *slog.Logger) (credentials.TransportCredentials, *TLSCredentials, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if insecureFlag {
		logger.Warn(InsecureFallbackWarning)
		return insecure.NewCredentials(), nil, nil
	}
	creds, err := NewTLSCredentials(ctx, opts, logger)
	if err != nil {
		return nil, nil, err
	}
	return creds.ClientCredentials(), creds, nil
}
