package proxy_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/Monsterwi/razor/balancer"
	"github.com/Monsterwi/razor/logger"
	"github.com/Monsterwi/razor/proxy"
)

func TestProxyPreservesTargetURLMethodAndHeaders(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/base/a%2Fb?fixed=1&x=2&x=3" {
			t.Errorf("origin URI = %q", r.URL.RequestURI())
		}
		if r.Method != http.MethodHead {
			t.Errorf("origin method = %q", r.Method)
		}
		if len(r.Header.Values("X-Multi")) != 2 || r.Header.Values("X-Multi")[0] != "one" {
			t.Errorf("request values = %v", r.Header.Values("X-Multi"))
		}
		w.Header().Add("X-Multi", "first")
		w.Header().Add("X-Multi", "second")
		w.Header().Set("Location", "/redirected")
		w.WriteHeader(http.StatusFound)
	}))
	defer origin.Close()
	p, err := proxy.NewHTTPProxy([]string{origin.URL + "/base?fixed=1"}, balancer.R2Balancer)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodHead, "http://client/a%2Fb?x=2&x=3", nil)
	r.Header.Add("X-Multi", "one")
	r.Header.Add("X-Multi", "two")
	resp, err := p.Proxy(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.RawBody().Close()
	if resp.StatusCode() != http.StatusFound {
		t.Errorf("status = %d", resp.StatusCode())
	}
	if len(resp.Header().Values("X-Multi")) != 2 {
		t.Errorf("response headers = %v", resp.Header())
	}
	body, err := io.ReadAll(resp.RawBody())
	if err != nil || len(body) != 0 {
		t.Errorf("HEAD body = %q, %v", body, err)
	}
}

func TestProxyFiltersHopHeadersAndUntrustedForwarding(t *testing.T) {
	for _, direct := range []bool{true, false} {
		t.Run(map[bool]string{true: "Proxy", false: "ServeHTTP"}[direct], func(t *testing.T) {
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for _, name := range []string{"Connection", "X-Remove", "Proxy-Authorization", "Forwarded", "Keep-Alive", "Upgrade"} {
					if got := r.Header.Get(name); got != "" {
						t.Errorf("request %s = %q", name, got)
					}
				}
				if got := r.Header.Get("X-Forwarded-For"); got != "192.0.2.10" {
					t.Errorf("XFF = %q", got)
				}
				if got := r.Header.Get("X-Real-IP"); got != "192.0.2.10" {
					t.Errorf("real IP = %q", got)
				}
				if got := r.Header.Get("X-Proxy"); got != proxy.ReverseProxy {
					t.Errorf("proxy header = %q", got)
				}
				if got := r.Header.Get("X-Forwarded-Proto"); got != "http" {
					t.Errorf("scheme = %q", got)
				}
				if got := r.Header.Get("X-Forwarded-Host"); got != "public.example" {
					t.Errorf("host = %q", got)
				}
				if r.Host != r.URL.Host && r.Host == "public.example" {
					t.Error("origin received client Host")
				}
				w.Header().Set("Connection", "X-Remove-Response")
				w.Header().Set("X-Remove-Response", "secret")
				w.Header().Set("Proxy-Authenticate", "private")
				w.Header().Add("X-Multi", "one")
				w.Header().Add("X-Multi", "two")
				w.Write([]byte("ok"))
			}))
			defer origin.Close()
			p, err := proxy.NewHTTPProxy([]string{origin.URL}, balancer.R2Balancer)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodGet, "http://public.example/", nil)
			r.RemoteAddr = "192.0.2.10:1234"
			r.Close = true
			r.Header.Set("Connection", "X-Remove, X-Proxy, X-Forwarded-For")
			r.Header.Set("X-Remove", "private")
			r.Header.Set("Proxy-Authorization", "private")
			r.Header.Set("Forwarded", "for=spoofed")
			r.Header.Set("X-Forwarded-For", "203.0.113.1")
			r.Header.Set("X-Real-IP", "203.0.113.1")
			r.Header.Set("X-Forwarded-Proto", "https")
			r.Header.Set("X-Forwarded-Host", "spoofed.example")
			r.Header.Set("Keep-Alive", "timeout=10")
			r.Header.Set("Upgrade", "websocket")
			var resp *http.Response
			if direct {
				result, err := p.Proxy(r)
				if err != nil {
					t.Fatal(err)
				}
				resp = result.RawResponse
			} else {
				w := httptest.NewRecorder()
				p.ServeHTTP(w, r)
				resp = w.Result()
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			for _, name := range []string{"Connection", "X-Remove-Response", "Proxy-Authenticate"} {
				if got := resp.Header.Get(name); got != "" {
					t.Errorf("response %s = %q", name, got)
				}
			}
			if len(resp.Header.Values("X-Multi")) != 2 {
				t.Errorf("response values = %v", resp.Header.Values("X-Multi"))
			}
			if r.Header.Get("X-Remove") != "private" {
				t.Error("input request mutated")
			}
		})
	}
}

func TestProxyTrustsOnlyConfiguredForwardingChain(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Seen-XFF", r.Header.Get("X-Forwarded-For"))
		w.Header().Set("Seen-IP", r.Header.Get("X-Real-IP"))
		w.Header().Set("Seen-Proto", r.Header.Get("X-Forwarded-Proto"))
	}))
	defer origin.Close()
	p, err := proxy.NewHTTPProxy([]string{origin.URL}, balancer.R2Balancer, proxy.WithTrustedProxies([]string{"192.0.2.0/24"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ chain, want, ip string }{
		{"spoofed, 198.51.100.7, 192.0.2.9", "198.51.100.7, 192.0.2.9, 192.0.2.10", "198.51.100.7"},
		{"invalid", "192.0.2.10", "192.0.2.10"},
	} {
		for _, direct := range []bool{true, false} {
			r := httptest.NewRequest(http.MethodGet, "http://public.example/", nil)
			r.RemoteAddr = "192.0.2.10:1234"
			r.Header.Set("X-Forwarded-For", tc.chain)
			r.Header.Set("X-Forwarded-Proto", "https")
			var resp *http.Response
			if direct {
				result, err := p.Proxy(r)
				if err != nil {
					t.Fatal(err)
				}
				resp = result.RawResponse
			} else {
				w := httptest.NewRecorder()
				p.ServeHTTP(w, r)
				resp = w.Result()
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if got := resp.Header.Get("Seen-XFF"); got != tc.want {
				t.Errorf("XFF = %q; want %q", got, tc.want)
			}
			if got := resp.Header.Get("Seen-IP"); got != tc.ip {
				t.Errorf("IP = %q; want %q", got, tc.ip)
			}
			if got := resp.Header.Get("Seen-Proto"); got != "https" {
				t.Errorf("proto = %q", got)
			}
		}
	}
	if _, err := proxy.NewHTTPProxy([]string{origin.URL}, balancer.R2Balancer, proxy.WithTrustedProxies([]string{"bad-network"})); err == nil {
		t.Fatal("invalid trusted CIDR accepted")
	}
}

func TestProxyHTTPSVerifiesCertificateAndTargetName(t *testing.T) {
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS.ServerName != "example.com" {
			t.Errorf("SNI = %q", r.TLS.ServerName)
		}
		if r.Host != "example.com" {
			t.Errorf("Host = %q", r.Host)
		}
		w.Write([]byte("secure"))
	}))
	defer origin.Close()
	p, err := proxy.NewHTTPProxy([]string{origin.URL}, balancer.R2Balancer)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "http://client/", nil)
	if resp, err := p.Proxy(r); err == nil {
		resp.RawBody().Close()
		t.Fatal("untrusted certificate accepted")
	}

	roots := x509.NewCertPool()
	roots.AddCert(origin.Certificate())
	u, _ := url.Parse(origin.URL)
	tr := &http.Transport{
		TLSClientConfig:    &tls.Config{RootCAs: roots},
		DisableCompression: true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, u.Host)
		},
	}
	defer tr.CloseIdleConnections()
	p, err = proxy.NewHTTPProxy([]string{"https://example.com"}, balancer.R2Balancer, proxy.WithTransport(tr))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Proxy(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.RawBody().Close()
	body, err := io.ReadAll(resp.RawBody())
	if err != nil || string(body) != "secure" {
		t.Fatalf("body = %q; error = %v", body, err)
	}
}

func TestProxyCancellationStopsOrigin(t *testing.T) {
	for _, sendHeaders := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-headers", true: "during-body"}[sendHeaders], func(t *testing.T) {
			started := make(chan struct{})
			canceled := make(chan struct{})
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if sendHeaders {
					w.Write([]byte("first"))
					http.NewResponseController(w).Flush()
				}
				close(started)
				<-r.Context().Done()
				close(canceled)
			}))
			defer origin.Close()
			p, err := proxy.NewHTTPProxy([]string{origin.URL}, balancer.R2Balancer)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := httptest.NewRequest(http.MethodGet, "http://client/", nil).WithContext(ctx)
			done := make(chan error, 1)
			go func() {
				resp, err := p.Proxy(r)
				if err == nil {
					_, err = io.ReadAll(resp.RawBody())
					resp.RawBody().Close()
				}
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("origin never started")
			}
			cancel()
			select {
			case <-canceled:
			case <-time.After(2 * time.Second):
				t.Fatal("origin did not observe cancellation")
			}
			select {
			case err := <-done:
				if err == nil {
					t.Error("cancellation returned success")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("proxy did not finish")
			}
		})
	}
}

func TestProxyStreamsThroughAccessLogBeforeOriginFinishes(t *testing.T) {
	release := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "11")
		w.Write([]byte("first"))
		http.NewResponseController(w).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Write([]byte("second"))
	}))
	defer origin.Close()
	p, err := proxy.NewHTTPProxy([]string{origin.URL}, balancer.R2Balancer)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(logger.AccessLogMiddleware(filepath.Join(t.TempDir(), "access.log"))(p))
	defer front.Close()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(front.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, first); err != nil || string(first) != "first" {
		t.Fatalf("first chunk = %q, error %v", first, err)
	}
	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(rest) != "second" {
		t.Fatalf("rest = %q, error %v", rest, err)
	}
}

func TestProxyPreservesEncodedBodyAndReusesConnections(t *testing.T) {
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	gz.Write([]byte("body"))
	gz.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "" {
			t.Errorf("added Accept-Encoding: %q", r.Header.Get("Accept-Encoding"))
		}
		w.Header().Set("X-Peer", r.RemoteAddr)
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(compressed.Bytes())
	}))
	defer origin.Close()
	p, err := proxy.NewHTTPProxy([]string{origin.URL}, balancer.R2Balancer)
	if err != nil {
		t.Fatal(err)
	}
	var peer string
	for i := 0; i < 2; i++ {
		resp, err := p.Proxy(httptest.NewRequest(http.MethodGet, "http://client/", nil))
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.RawBody())
		resp.RawBody().Close()
		if err != nil || !bytes.Equal(body, compressed.Bytes()) || resp.Header().Get("Content-Encoding") != "gzip" {
			t.Fatalf("encoded response changed: body %x, error %v, headers %v", body, err, resp.Header())
		}
		if i == 0 {
			peer = resp.Header().Get("X-Peer")
		} else if resp.Header().Get("X-Peer") != peer {
			t.Error("upstream connection was not reused")
		}
	}
}

func TestProxyEntryPointsPreserveRawQuery(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Seen-URI", r.URL.RequestURI())
	}))
	defer origin.Close()
	p, err := proxy.NewHTTPProxy([]string{origin.URL + "/base?fixed=1"}, balancer.R2Balancer)
	if err != nil {
		t.Fatal(err)
	}
	for _, direct := range []bool{true, false} {
		r := httptest.NewRequest(http.MethodGet, "http://public.example/a%2Fb?x=one;two&encoded=%2f&x=3", nil)
		var resp *http.Response
		if direct {
			result, err := p.Proxy(r)
			if err != nil {
				t.Fatal(err)
			}
			resp = result.RawResponse
		} else {
			w := httptest.NewRecorder()
			p.ServeHTTP(w, r)
			resp = w.Result()
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if got := resp.Header.Get("Seen-URI"); got != "/base/a%2Fb?fixed=1&x=one;two&encoded=%2f&x=3" {
			t.Errorf("direct=%v URI=%q", direct, got)
		}
	}
}

func TestProxyCacheKeyUsesTrustedForwardedScheme(t *testing.T) {
	p, err := proxy.NewHTTPProxy([]string{"http://origin.example"}, balancer.R2Balancer, proxy.WithTrustedProxies([]string{"192.0.2.0/24"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, peer, proto, connection, want string }{
		{"direct", "198.51.100.1:1234", "", "", "http-v4:http://public.example/a%2Fb?x=1"},
		{"spoofed", "198.51.100.1:1234", "https", "", "http-v4:http://public.example/a%2Fb?x=1"},
		{"trusted-http", "192.0.2.10:1234", "http", "", "http-v4:http://public.example/a%2Fb?x=1"},
		{"trusted-https", "192.0.2.10:1234", "https", "", "http-v4:https://public.example/a%2Fb?x=1"},
		{"hop-header", "192.0.2.10:1234", "https", "X-Forwarded-Proto", "http-v4:http://public.example/a%2Fb?x=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "http://public.example/a%2Fb?x=1", nil)
			r.RemoteAddr = tc.peer
			r.Header.Set("X-Forwarded-Proto", tc.proto)
			r.Header.Set("Connection", tc.connection)
			if got := p.CacheKey(r); got != tc.want {
				t.Fatalf("key = %q; want %q", got, tc.want)
			}
			if r.URL.Scheme != "http" || r.Header.Get("X-Forwarded-Proto") != tc.proto {
				t.Fatal("cache key mutated incoming request")
			}
		})
	}
}

func TestBackendProbeAcceptsIPv6Authority(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer listener.Close()
	if !proxy.IsBackendAlive(listener.Addr().String()) {
		t.Fatal("IPv6 backend reported unreachable")
	}
}

func TestNewHTTPProxyValidatesUpstreamTCPPort(t *testing.T) {
	for _, target := range []string{"http://origin.test", "https://origin.test", "http://origin.test:80", "https://origin.test:443", "http://origin.test:65535", "https://[::1]:65535"} {
		t.Run(target, func(t *testing.T) {
			p, err := proxy.NewHTTPProxy([]string{target}, balancer.R2Balancer)
			if err != nil {
				t.Fatalf("valid upstream rejected: %v", err)
			}
			p.Close()
		})
	}
	for _, port := range []string{"0", "65536", "18446744073709551616"} {
		t.Run(port, func(t *testing.T) {
			p, err := proxy.NewHTTPProxy([]string{"http://127.0.0.1:" + port}, balancer.R2Balancer)
			if p != nil {
				p.Close()
			}
			if err == nil {
				t.Fatal("invalid destination TCP port accepted")
			}
		})
	}
}
