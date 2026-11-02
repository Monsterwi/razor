package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Monsterwi/razor/logger"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
	"github.com/Monsterwi/razor/server"
)

func TestPublicStaticHundredClientsShareCanonicalOriginRequest(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	headers := make(chan http.Header, 100)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		headers <- r.Header.Clone()
		<-release
		w.Header().Set("Cache-Control", "public,max-age=3600")
		fmt.Fprint(w, "complete shared public body")
	}))
	t.Cleanup(origin.Close)
	cfg := config.GetDefaultConfig()
	cfg.SharedFillPublicStatic = 1
	cfg.PurgeToken = "public-test-purge"
	cfg.TrustedProxies = []string{"192.0.2.0/24"}
	s := lifecycleServer(t, origin.URL, cfg)
	const clients = 100
	results := make(chan *httptest.ResponseRecorder, clients)
	for i := 0; i < clients; i++ {
		go func(i int) {
			query := "b=2&a=1"
			r := httptest.NewRequest("GET", "http://cache.test/item?"+query, nil)
			r.RemoteAddr = fmt.Sprintf("192.0.2.%d:12345", i+1)
			r.Header.Set("User-Agent", fmt.Sprintf("client-%d", i))
			for _, field := range []string{"Forwarded", "X-Forwarded-For", "X-Real-IP", "Traceparent", "Tracestate", "Baggage", "X-Request-Id", "X-Correlation-Id"} {
				r.Header.Set(field, fmt.Sprint(i))
			}
			r.Header.Set("X-Forwarded-Proto", "https")
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, r)
			results <- rec
		}(i)
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == clients })
	once.Do(func() { close(release) })
	for i := 0; i < clients; i++ {
		rec := <-results
		if rec.Code != 200 || rec.Body.String() != "complete shared public body" {
			t.Fatalf("response %d %q", rec.Code, rec.Body.String())
		}
	}
	if len(headers) != 1 {
		t.Fatalf("origin calls=%d, want 1", len(headers))
	}
	h := <-headers
	if h.Get("User-Agent") != "Razor" || h.Get("X-Forwarded-Proto") != "https" || h.Get("X-Forwarded-Host") != "cache.test" {
		t.Fatalf("outbound=%v", h)
	}
	for _, field := range []string{"Forwarded", "X-Forwarded-For", "X-Real-IP", "Traceparent", "Tracestate", "Baggage", "X-Request-Id", "X-Correlation-Id"} {
		if _, ok := h[field]; ok {
			t.Fatalf("outbound exposes %s: %v", field, h)
		}
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
}

func publicCacheServer(t *testing.T, handler http.HandlerFunc) (*server.Server, *config.Config) {
	t.Helper()
	origin := httptest.NewServer(handler)
	t.Cleanup(origin.Close)
	cfg := config.GetDefaultConfig()
	cfg.SharedFillPublicStatic = 1
	cfg.PurgeToken = "public-test-purge"
	cfg.TrustedProxies = []string{"192.0.2.0/24"}
	return lifecycleServer(t, origin.URL, cfg), cfg
}

func publicRequest(s *server.Server, ctx context.Context, field, value string) <-chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		r := httptest.NewRequest("GET", "http://cache.test/item", nil).WithContext(ctx)
		// Keep these whole-object sharing cases outside identity block classification.
		r.Header.Set("Accept-Encoding", "gzip")
		r.RemoteAddr = "192.0.2.1:12345"
		if field != "" {
			r.Header.Set(field, value)
		}
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, r)
		done <- rec
	}()
	return done
}

func TestPublicStaticRetainsVariantAndUnknownHeaderIsolation(t *testing.T) {
	for _, field := range []string{"Accept-Language", "Accept-Encoding", "X-Client-Variant"} {
		t.Run(field, func(t *testing.T) {
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				entered <- struct{}{}
				<-release
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("Vary", field)
				fmt.Fprint(w, r.Header.Get(field))
			})
			a := publicRequest(s, context.Background(), field, "alpha")
			b := publicRequest(s, context.Background(), field, "beta")
			for range 2 {
				select {
				case <-entered:
				case <-time.After(2 * time.Second):
					t.Fatal("distinct requests incorrectly coalesced")
				}
			}
			once.Do(func() { close(release) })
			if got := <-a; got.Code != 200 || got.Body.String() != "alpha" {
				t.Fatalf("first %d %q", got.Code, got.Body.String())
			}
			if got := <-b; got.Code != 200 || got.Body.String() != "beta" {
				t.Fatalf("second %d %q", got.Code, got.Body.String())
			}
		})
	}
}

func TestPublicStaticPrivateResponsesRemainExclusive(t *testing.T) {
	for _, mode := range []string{"private", "cookie", "vary", "vary-star"} {
		t.Run(mode, func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var calls atomic.Int32
			s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				calls.Add(1)
				<-release
				w.Header().Set("Cache-Control", "public,max-age=3600")
				switch mode {
				case "private":
					w.Header().Set("Cache-Control", "private")
				case "cookie":
					w.Header().Set("Set-Cookie", "secret=1")
				case "vary":
					w.Header().Set("Vary", "User-Agent")
				case "vary-star":
					w.Header().Set("Vary", "*")
				}
				fmt.Fprint(w, "creator only")
			})
			first := publicRequest(s, context.Background(), "User-Agent", "first")
			waitFillStats(t, s, func(st server.Stats) bool { return st.Origin.InFlight == 1 && calls.Load() == 1 })
			follower := publicRequest(s, context.Background(), "User-Agent", "second")
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 && st.SharedFillJoined > 0 })
			once.Do(func() { close(release) })
			if got := <-first; got.Code != 200 || got.Body.String() != "creator only" {
				t.Fatalf("creator %d %q", got.Code, got.Body.String())
			}
			if got := <-follower; got.Code != 503 || strings.Contains(got.Body.String(), "creator only") {
				t.Fatalf("follower %d %q", got.Code, got.Body.String())
			}
			waitFillStats(t, s, func(st server.Stats) bool {
				return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
			})
			if calls.Load() != 1 {
				t.Fatalf("origin retry storm: %d", calls.Load())
			}
		})
	}
}

func TestPublicStaticAuthenticatedRequestsRetainProxyIdentity(t *testing.T) {
	for _, field := range []string{"Authorization", "Cookie"} {
		t.Run(field, func(t *testing.T) {
			s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "public,max-age=3600")
				fmt.Fprintf(w, "%s|%s|%s|%s", r.Header.Get(field), r.Header.Get("User-Agent"), r.Header.Get("X-Real-IP"), r.Header.Get("X-Request-Id"))
			})
			for range 2 {
				r := httptest.NewRequest("GET", "http://cache.test/item", nil)
				r.RemoteAddr = "192.0.2.8:12345"
				r.Header.Set(field, "private-client")
				r.Header.Set("User-Agent", "original-client")
				r.Header.Set("X-Request-Id", "original-trace")
				rec := httptest.NewRecorder()
				s.ServeHTTP(rec, r)
				if rec.Code != 200 || rec.Body.String() != "private-client|original-client|192.0.2.8|original-trace" {
					t.Fatalf("bypass %d %q", rec.Code, rec.Body.String())
				}
			}
			if s.Stats().Origin.Requests != 2 || s.Stats().SharedFillJoined != 0 {
				t.Fatalf("private entered cache: %+v", s.Stats())
			}
		})
	}
}

func TestPublicStaticSchemeRawQueryPurgeAndRestartBoundary(t *testing.T) {
	var calls atomic.Int32
	s, cfg := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		calls.Add(1)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Seen-Query", r.URL.RawQuery)
		fmt.Fprintf(w, "%s|%s|%s", r.Header.Get("X-Forwarded-Proto"), r.URL.Query().Get("a"), r.URL.Query().Get("b"))
	})
	request := func(scheme, query string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "http://cache.test/item?"+query, nil)
		r.RemoteAddr = "192.0.2.1:12345"
		r.Header.Set("X-Forwarded-Proto", scheme)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, r)
		return rec
	}
	for _, item := range []struct{ scheme, query string }{{"https", "b=2&a=1"}, {"http", "b=2&a=1"}, {"https", "a=1&b=2"}} {
		for range 2 {
			if got := request(item.scheme, item.query); got.Code != 200 || got.Body.String() != item.scheme+"|1|2" || got.Header().Get("Seen-Query") != item.query {
				t.Fatalf("%d %q", got.Code, got.Body.String())
			}
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("cache identity aliased or lost: %d", calls.Load())
	}
	if got := request("https", "a=9&b=2"); got.Code != 200 || got.Body.String() != "https|9|2" || calls.Load() != 4 {
		t.Fatalf("distinct query value collided: status=%d body=%q calls=%d", got.Code, got.Body.String(), calls.Load())
	}
	purgeRequest := httptest.NewRequest("PURGE", "http://cache.test/item?a=1&b=2", nil)
	purgeRequest.RemoteAddr = "192.0.2.1:12345"
	purgeRequest.Header.Set("X-Forwarded-Proto", "https")
	purgeRequest.Header.Set("Authorization", "Bearer public-test-purge")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, purgeRequest)
	if rec.Code != 200 {
		t.Fatalf("PURGE public selector %d", rec.Code)
	}
	for _, item := range []struct{ scheme, query string }{{"http", "b=2&a=1"}, {"https", "a=9&b=2"}, {"https", "b=2&a=1"}} {
		if got := request(item.scheme, item.query); got.Code != 200 || calls.Load() != 4 {
			t.Fatalf("PURGE evicted distinct identity: status=%d origin=%d", got.Code, calls.Load())
		}
	}
	if got := request("https", "a=1&b=2"); got.Code != 200 || calls.Load() != 5 {
		t.Fatalf("public PURGE missed namespace: status=%d origin=%d", got.Code, calls.Load())
	}
	candidate := *cfg
	candidate.SharedFillPublicStatic = 0
	if err := s.Reload(&candidate); err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("mode hot reload: %v", err)
	}
	if s.Stats().RuntimeVersion != 1 {
		t.Fatal("rejected reload mutated runtime")
	}
}

func TestPublicStaticCreatorCancellationKeepsFollower(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Cache-Control", "public,max-age=3600")
		fmt.Fprint(w, "complete")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := publicRequest(s, ctx, "User-Agent", "creator")
	<-entered
	follower := publicRequest(s, context.Background(), "User-Agent", "follower")
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 && st.SharedFillJoined > 0 })
	cancel()
	<-first
	once.Do(func() { close(release) })
	if got := <-follower; got.Code != 200 || got.Body.String() != "complete" {
		t.Fatalf("follower %d %q", got.Code, got.Body.String())
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
}

func TestPublicStaticUnsupportedRangeBypassRetainsClientHeaders(t *testing.T) {
	seen := make(chan string, 2)
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Method + ":" + r.Header.Get("User-Agent")
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `W/"weak"`)
		http.ServeContent(w, r, "item", time.Time{}, strings.NewReader("abcdef"))
	})
	got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-2"}, "User-Agent": {"original-client"}})
	if got.Code != 206 || got.Body.String() != "abc" {
		t.Fatalf("range fallback %d %q", got.Code, got.Body.String())
	}
	if a, b := <-seen, <-seen; a != "HEAD:Razor" || b != "GET:original-client" {
		t.Fatalf("probe/bypass headers %q %q", a, b)
	}
}

func TestPublicStaticRangeManifestAndBlockUseCanonicalProducer(t *testing.T) {
	seen := make(chan string, 3)
	data := bytes.Repeat([]byte("range-body"), 1<<16)
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		for _, field := range []string{"Forwarded", "X-Forwarded-For", "X-Real-IP", "Traceparent", "X-Request-Id"} {
			if r.Header.Get(field) != "" {
				t.Errorf("producer exposes %s", field)
			}
		}
		seen <- r.Method + ":" + r.Header.Get("User-Agent") + ":" + r.Header.Get("X-Forwarded-Proto")
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"range-v1"`)
		http.ServeContent(w, r, "item", time.Time{}, bytes.NewReader(data))
	})
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "http://cache.test/item", nil)
		r.RemoteAddr = fmt.Sprintf("192.0.2.%d:12345", i+1)
		r.Header.Set("User-Agent", fmt.Sprintf("client-%d", i))
		r.Header.Set("Range", "bytes=7-65542")
		r.Header.Set("X-Forwarded-Proto", "https")
		r.Header.Set("X-Request-Id", "trace")
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, r)
		if rec.Code != 206 || !bytes.Equal(rec.Body.Bytes(), data[7:65543]) {
			t.Fatalf("Range %d bytes=%d", rec.Code, rec.Body.Len())
		}
	}
	if len(seen) != 2 {
		t.Fatalf("origin requests=%d want HEAD+blockGET", len(seen))
	}
	if a, b := <-seen, <-seen; a != "HEAD:Razor:https" || b != "GET:Razor:https" {
		t.Fatalf("manifest/block %q %q", a, b)
	}
}

func TestPublicStaticLastSubscriberCancellationDrainsProducer(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(cancelled) })
	firstCtx, firstCancel := context.WithCancel(context.Background())
	defer firstCancel()
	secondCtx, secondCancel := context.WithCancel(context.Background())
	defer secondCancel()
	first := publicRequest(s, firstCtx, "User-Agent", "creator")
	<-entered
	follower := publicRequest(s, secondCtx, "User-Agent", "follower")
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 && st.SharedFillJoined > 0 })
	firstCancel()
	secondCancel()
	<-first
	<-follower
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("last subscriber retained producer")
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
}

func TestPublicStaticLogsOnlyHashedClientCorrelation(t *testing.T) {
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		fmt.Fprint(w, "ok")
	})
	path := filepath.Join(t.TempDir(), "access.log")
	handler := logger.AccessLogMiddleware(path)(s)
	r := httptest.NewRequest("GET", "http://cache.test/item", nil)
	r.Header.Set("X-Request-Id", strings.Repeat("sensitive-client-id", 1000)+"\r\ncontrol")
	r.Header.Set("X-Correlation-Id", "sensitive-correlation")
	r.Header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	r.Header.Set("Baggage", "private-baggage")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"sensitive-client-id", "sensitive-correlation", "private-baggage", "4bf92f3577b34da6a3ce929d0e0e4736"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("raw identifier leaked: %s", secret)
		}
	}
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(raw), &entry); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"client_x_request_id_sha256", "client_x_correlation_id_sha256", "client_traceparent_sha256"} {
		if value, ok := entry[key].(string); !ok || len(value) != 64 {
			t.Fatalf("missing bounded correlation %s: %v", key, entry)
		}
	}
	r.Header.Set("Traceparent", "invalid\ntrace")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, r)
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
	entry = nil
	if err := json.Unmarshal(lines[len(lines)-1], &entry); err != nil {
		t.Fatal(err)
	}
	if _, ok := entry["client_traceparent_sha256"]; ok {
		t.Fatal("invalid traceparent retained")
	}
}

func TestPublicStaticModeUsesSeparatePersistentNamespace(t *testing.T) {
	var calls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		calls.Add(1)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		fmt.Fprint(w, r.Header.Get("User-Agent"))
	}))
	defer origin.Close()
	path := filepath.Join(t.TempDir(), "cache")
	for _, mode := range []int{0, 1, 0} {
		cfg := config.GetDefaultConfig()
		cfg.UpstreamTargets = []string{origin.URL}
		cfg.SharedFillPublicStatic = mode
		c, err := cache.NewCache(1, []string{path}, []int64{32 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Init(); err != nil {
			t.Fatal(err)
		}
		p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo)
		if err != nil {
			t.Fatal(err)
		}
		s := server.New(cfg, c, p)
		t.Cleanup(func() { s.Shutdown() })
		got := fetch(s, "http://cache.test/item", http.Header{"User-Agent": {"original-client"}})
		want := "original-client"
		if mode == 1 {
			want = "Razor"
		}
		if got.Code != 200 || got.Body.String() != want {
			t.Fatalf("mode %d reused wrong content %d %q", mode, got.Code, got.Body.String())
		}
		if err := s.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("mode namespace isolation/reuse: origin=%d", calls.Load())
	}
}
