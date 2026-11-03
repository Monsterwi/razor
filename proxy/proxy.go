package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Monsterwi/razor/internal/budget"
	"time"

	"github.com/Monsterwi/razor/balancer"
	"github.com/go-resty/resty/v2"
)

var (
	XRealIP       = http.CanonicalHeaderKey("X-Real-IP")
	XProxy        = http.CanonicalHeaderKey("X-Proxy")
	XForwardedFor = http.CanonicalHeaderKey("X-Forwarded-For")
	ReverseProxy  = "Balancer-Reverse-Proxy"
)

// HTTPProxy shares a connection pool across upstream requests.
type HTTPProxy struct {
	counters
	limits          Limits
	budgets         map[string]*budget.Limiter
	customTransport bool
	caDigest        [32]byte
	cacheNamespace  string
	upstreams       []string
	algorithm       string
	roots           *x509.CertPool
	ctx             context.Context
	cancel          context.CancelFunc
	healthMu        sync.Mutex
	healthWG        sync.WaitGroup
	healthCancel    context.CancelFunc
	healthStopped   bool
	trustedProxies  []netip.Prefix
	targets         map[string]*url.URL
	lb              balancer.Balancer
	transport       http.RoundTripper
	sync.RWMutex
	alive map[string]bool
}

// Option configures a proxy before it starts serving requests.
type Option func(*HTTPProxy) error

// WithTransport supplies a shared transport, for example with private CA roots.
// The caller owns its connection pool and must disable transparent decompression.
func WithTransport(transport http.RoundTripper) Option {
	return func(h *HTTPProxy) error {
		if transport == nil {
			return fmt.Errorf("upstream transport is nil")
		}
		h.transport = transport
		h.customTransport = true
		return nil
	}
}

// Reloadable reports whether a new configured proxy can preserve transport behavior.
func (h *HTTPProxy) Reloadable() bool { return !h.customTransport }

// Upstreams returns the configured pool without exposing mutable proxy state.
func (h *HTTPProxy) Upstreams() ([]string, string) {
	return append([]string(nil), h.upstreams...), h.algorithm
}

// CacheNamespace identifies effective routing and forwarding/trust policy. The
// configured pool is assumed to contain interchangeable full-object replicas.
func (h *HTTPProxy) CacheNamespace() string { return h.cacheNamespace }

// WithTrustedProxies accepts forwarded identity only from these CIDR networks.
func WithTrustedProxies(networks []string) Option {
	return func(h *HTTPProxy) error {
		for _, network := range networks {
			prefix, err := netip.ParsePrefix(network)
			if err != nil {
				return fmt.Errorf("trusted proxy %q: %w", network, err)
			}
			h.trustedProxies = append(h.trustedProxies, prefix)
		}
		return nil
	}
}

func (h *HTTPProxy) trusted(ip netip.Addr) bool {
	for _, prefix := range h.trustedProxies {
		if prefix.Contains(ip.Unmap()) {
			return true
		}
	}
	return false
}

func (h *HTTPProxy) forwarding(r *http.Request) (chain, clientIP, scheme string) {
	if publicScheme, ok := r.Context().Value(publicRequestKey{}).(string); ok {
		return "", "", publicScheme
	}
	clientIP = GetIP(r)
	chain = clientIP
	scheme = "http"
	if r.TLS != nil {
		scheme = "https"
	}
	peer, err := netip.ParseAddr(clientIP)
	if err != nil || !h.trusted(peer) {
		return
	}
	headers := r.Header.Clone()
	stripHopHeaders(headers)
	if proto := headers.Get("X-Forwarded-Proto"); proto == "http" || proto == "https" {
		scheme = proto
	}
	// Walk from the trusted peer toward the client; ignore any untrusted prefix.
	values := strings.Split(strings.Join(headers.Values(XForwardedFor), ","), ",")
	for i := len(values) - 1; i >= 0; i-- {
		ip, err := netip.ParseAddr(strings.TrimSpace(values[i]))
		if err != nil {
			break
		}
		clientIP = ip.Unmap().String()
		chain = clientIP + ", " + chain
		if !h.trusted(ip) {
			break
		}
	}
	return
}

// NewHTTPProxy preserves each upstream's scheme, authority, base path and query.
func NewHTTPProxy(targetHosts []string, algorithm string, options ...Option) (*HTTPProxy, error) {
	h := &HTTPProxy{targets: make(map[string]*url.URL), alive: make(map[string]bool), limits: DefaultLimits()}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	for _, option := range options {
		if err := option(h); err != nil {
			return nil, err
		}
	}
	hosts := make([]string, 0, len(targetHosts))
	for _, targetHost := range targetHosts {
		target, err := url.Parse(targetHost)
		if err != nil {
			return nil, err
		}
		if (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" || target.User != nil || target.Fragment != "" {
			return nil, fmt.Errorf("invalid upstream URL %q", targetHost)
		}
		if port := target.Port(); port != "" {
			if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
				return nil, fmt.Errorf("upstream TCP port must be between 1 and 65535")
			}
		}
		key := target.String()
		if _, exists := h.targets[key]; exists {
			continue
		}
		h.targets[key] = target
		h.alive[key] = true
		hosts = append(hosts, key)
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("at least one upstream is required")
	}
	var err error
	h.lb, err = balancer.Build(algorithm, hosts)
	if err != nil {
		return nil, err
	}
	if err := h.ConfigureLimits(h.limits); err != nil {
		h.cancel()
		return nil, err
	}
	policy, _ := json.Marshal(struct {
		Targets   []string
		Algorithm string
		Trusted   []netip.Prefix
		CA        [32]byte
	}{hosts, algorithm, h.trustedProxies, h.caDigest})
	h.upstreams, h.algorithm = append([]string(nil), hosts...), algorithm
	h.cacheNamespace = fmt.Sprintf("%x", sha256.Sum256(policy))
	return h, nil
}

func (h *HTTPProxy) rewrite(p *httputil.ProxyRequest, target *url.URL) {
	stripHopHeaders(p.Out.Header)
	// Forward the same original query bytes used by cache identity.
	p.Out.URL.RawQuery = p.In.URL.RawQuery
	p.SetURL(target)
	for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-IP"} {
		p.Out.Header.Del(name)
	}
	chain, ip, scheme := h.forwarding(p.In)
	if ip != "" {
		p.Out.Header.Set(XForwardedFor, chain)
		p.Out.Header.Set(XRealIP, ip)
	}
	p.Out.Header.Set(XProxy, ReverseProxy)
	p.Out.Header.Set("X-Forwarded-Host", p.In.Host)
	p.Out.Header.Set("X-Forwarded-Proto", scheme)
	p.Out.RequestURI = ""
	p.Out.Close = false
}

// CacheKey uses the same trusted scheme as the forwarded upstream request.
func (h *HTTPProxy) CacheKey(r *http.Request) string {
	_, _, scheme := h.forwarding(r)
	copy := *r
	target := *r.URL
	target.Scheme = scheme
	copy.URL = &target
	return GetCacheKey(&copy)
}

// ServeHTTP and Proxy use the same request construction and upstream transport.
func (h *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host, err := h.lb.Balance(h.CacheKey(r))
	if err != nil {
		http.Error(w, "no available upstream", http.StatusBadGateway)
		return
	}
	p := &httputil.ReverseProxy{
		Rewrite:   func(p *httputil.ProxyRequest) { h.rewrite(p, h.targets[host]) },
		Transport: originTransport{h, host},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			status := http.StatusBadGateway
			if errors.Is(err, ErrOverloaded) {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, "Upstream unavailable", status)
		},
		FlushInterval:  -1,
		ModifyResponse: func(r *http.Response) error { stripHopHeaders(r.Header); return nil },
	}
	p.ServeHTTP(w, r)
}

// Proxy returns an unbuffered response. The caller must close RawBody.
func (h *HTTPProxy) Proxy(r *http.Request) (*resty.Response, error) {
	resp, _, err := h.ProxyWithOriginHeaders(r)
	return resp, err
}

// ProxyWithOriginHeaders also returns a header snapshot before hop-by-hop removal
// so cache admission cannot lose privacy or freshness signals.
func (h *HTTPProxy) ProxyWithOriginHeaders(r *http.Request) (*resty.Response, http.Header, error) {
	origin, err := h.SelectOrigin(r)
	if err != nil {
		return nil, nil, err
	}
	return origin.ProxyWithOriginHeaders(r)
}

// Origin pins one configured target. Its owning HTTPProxy must remain open for
// the complete operation; this handle does not own a separate connection pool.
type Origin struct {
	proxy *HTTPProxy
	host  string
}

func (h *HTTPProxy) SelectOrigin(r *http.Request) (*Origin, error) {
	host, err := h.lb.Balance(h.CacheKey(r))
	if err != nil {
		return nil, fmt.Errorf("balance: %w", err)
	}
	return &Origin{proxy: h, host: host}, nil
}

// ConfiguredOrigins returns pinned handles in stable configuration order, including
// unhealthy targets for cache lookup. Handles share this proxy's pool and lifetime.
func (h *HTTPProxy) ConfiguredOrigins() []*Origin {
	origins := make([]*Origin, 0, len(h.upstreams))
	for _, host := range h.upstreams {
		origins = append(origins, &Origin{proxy: h, host: host})
	}
	return origins
}

// Available reports health without re-enabling a target removed from selection.
func (o *Origin) Available() bool { return o.proxy.ReadAlive(o.host) }

// Identity includes the configured scheme, authority, base path and fixed query.
func (o *Origin) Identity() string { return o.host }

// ProxyWithOriginHeaders uses this target even if later selections choose a
// different origin. Failures never switch a partially fetched representation.
func (o *Origin) ProxyWithOriginHeaders(r *http.Request) (*resty.Response, http.Header, error) {
	h := o.proxy
	out := r.Clone(r.Context())
	h.rewrite(&httputil.ProxyRequest{In: r, Out: out}, h.targets[o.host])
	resp, err := h.roundTrip(out, o.host)
	if err != nil {
		return nil, nil, err
	}
	originHeaders := resp.Header.Clone()
	stripHopHeaders(resp.Header)
	return &resty.Response{RawResponse: resp}, originHeaders, nil
}

type trackedBody struct {
	io.ReadCloser
	once          sync.Once
	done          func()
	cancel        context.CancelFunc
	idle          time.Duration
	bytes, errors *atomic.Uint64
}

func (b *trackedBody) Read(p []byte) (int, error) {
	timer := time.AfterFunc(b.idle, b.cancel)
	n, err := b.ReadCloser.Read(p)
	timer.Stop()
	if n > 0 {
		b.bytes.Add(uint64(n))
	}
	if err != nil && err != io.EOF {
		b.errors.Add(1)
	}
	if err != nil {
		b.once.Do(b.done)
	}
	return n, err
}

func (b *trackedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
}

// health check start
// ReadAlive reads the alive status of the site
func (h *HTTPProxy) ReadAlive(url string) bool {
	h.RLock()
	defer h.RUnlock()
	return h.alive[url]
}

// SetAlive sets the alive status to the site
func (h *HTTPProxy) SetAlive(url string, alive bool) {
	h.Lock()
	defer h.Unlock()
	h.alive[url] = alive
}

// health check end

// ConnectionTimeout refers to connection timeout for health check
var ConnectionTimeout = 3 * time.Second

// GetIP get client IP
func GetIP(r *http.Request) string {
	clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return ""
	}
	ip := net.ParseIP(clientIP)
	if ip == nil {
		return ""
	}
	return ip.String()
}

func stripHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(name)
	}
}

// GetHost get the hostname, looks like IP:Port
func GetHost(url *url.URL) string {
	if _, _, err := net.SplitHostPort(url.Host); err == nil {
		return url.Host
	}
	if url.Scheme == "http" {
		return fmt.Sprintf("%s:%s", url.Host, "80")
	} else if url.Scheme == "https" {
		return fmt.Sprintf("%s:%s", url.Host, "443")
	}
	return url.Host
}

// IsBackendAlive Attempt to establish a tcp connection to determine whether the site is alive
func IsBackendAlive(host string) bool {
	addr, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return false
	}
	conn, err := net.DialTimeout("tcp", addr.String(), ConnectionTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
