package server_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
	"github.com/Monsterwi/razor/server"
)

func cacheServer(t *testing.T, origin http.HandlerFunc, options ...proxy.Option) (*server.Server, *atomic.Int32) {
	t.Helper()
	calls := new(atomic.Int32)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); origin(w, r) }))
	t.Cleanup(upstream.Close)
	c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache.dat")}, []int64{32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	cfg := config.GetDefaultConfig()
	cfg.UpstreamTargets = []string{upstream.URL}
	p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo, options...)
	if err != nil {
		t.Fatal(err)
	}
	return server.New(cfg, c, p), calls
}

func fetch(s http.Handler, target string, headers http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", target, nil)
	if headers != nil {
		req.Header = headers
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestPrivateRequestBypassesPreviouslyPublicObject(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		fmt.Fprint(w, "body:"+r.Header.Get("Authorization"))
	})
	if got := fetch(s, "http://cache.test/item", nil).Body.String(); got != "body:" {
		t.Fatal(got)
	}
	got := fetch(s, "http://cache.test/item", http.Header{"Authorization": {"Bearer alice"}}).Body.String()
	if got != "body:Bearer alice" || calls.Load() != 2 {
		t.Fatalf("private request returned %q after %d origin calls", got, calls.Load())
	}
}

func TestResponseCacheAdmission(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		headers  http.Header
		reusable bool
	}{
		{"public", 200, http.Header{"Cache-Control": {"public, max-age=3600"}}, true},
		{"shared lifetime", 200, http.Header{"Cache-Control": {"s-maxage=3600"}}, true},
		{"private", 200, http.Header{"Cache-Control": {"private, max-age=3600"}}, false},
		{"no store", 200, http.Header{"Cache-Control": {"public, max-age=3600, no-store"}}, false},
		{"no cache", 200, http.Header{"Cache-Control": {"public, max-age=3600, no-cache"}}, false},
		{"no explicit sharing", 200, http.Header{"Cache-Control": {"max-age=3600"}}, false},
		{"no freshness", 200, http.Header{"Cache-Control": {"public"}}, false},
		{"expired", 200, http.Header{"Cache-Control": {"public, max-age=1"}, "Age": {"2"}}, false},
		{"old date", 200, http.Header{"Cache-Control": {"public, max-age=3600"}, "Date": {"Mon, 02 Jan 2006 15:04:05 GMT"}}, false},
		{"set cookie", 200, http.Header{"Cache-Control": {"public, max-age=3600"}, "Set-Cookie": {"user=alice"}}, false},
		{"vary", 200, http.Header{"Cache-Control": {"public, max-age=3600"}, "Vary": {"Accept-Encoding"}}, true},
		{"range", 206, http.Header{"Cache-Control": {"public, max-age=3600"}, "Content-Range": {"bytes 0-2/10"}}, false},
		{"conflicting lifetime", 200, http.Header{"Cache-Control": {"public, max-age=3600", "max-age=0"}}, false},
		{"bad age", 200, http.Header{"Cache-Control": {"public, max-age=3600"}, "Age": {"invalid"}}, false},
		{"overflow lifetime", 200, http.Header{"Cache-Control": {"public, max-age=999999999999999999999999"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var serial atomic.Int32
			s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				for k, v := range tc.headers {
					w.Header()[k] = v
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, serial.Add(1))
			})
			first := fetch(s, "http://cache.test/item", nil)
			second := fetch(s, "http://cache.test/item", nil)
			expected := int32(2)
			if tc.reusable {
				expected = 1
			}
			if calls.Load() != expected {
				t.Fatalf("origin calls=%d want %d; bodies %q %q", calls.Load(), expected, first.Body.String(), second.Body.String())
			}
		})
	}
}

func TestConnectionCannotEraseCacheAdmissionHeaders(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"Vary", "Accept-Language"},
		{"Set-Cookie", "session=private"},
		{"Age", "100"},
		{"Age", "1"},
		{"Date", "Mon, 02 Jan 2006 15:04:05 GMT"},
		{"Date", time.Now().Add(-30 * time.Second).UTC().Format(http.TimeFormat)},
		{"Content-Range", "bytes 0-2/10"},
	} {
		t.Run(tc.name+"/"+tc.value, func(t *testing.T) {
			s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "public, max-age=60")
				w.Header().Set("Connection", tc.name)
				w.Header().Set(tc.name, tc.value)
				w.Header().Add("X-Multi", "one")
				w.Header().Add("X-Multi", "two")
				fmt.Fprint(w, r.Header.Get("Accept-Language"))
			})
			for _, language := range []string{"alice", "bob"} {
				got := fetch(s, "http://cache.test/item", http.Header{"Accept-Language": {language}})
				if got.Body.String() != language {
					t.Errorf("response body=%q want %q", got.Body.String(), language)
				}
				if got.Header().Get(tc.name) != "" || got.Header().Get("Connection") != "" {
					t.Errorf("hop headers forwarded: %v", got.Header())
				}
				if len(got.Header().Values("X-Multi")) != 2 {
					t.Errorf("end-to-end headers lost: %v", got.Header())
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("Connection erased %s admission: %d origin calls", tc.name, calls.Load())
			}
		})
	}
}

func TestTruncatedOriginDoesNotCommit(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Length", "100")
		fmt.Fprint(w, "partial")
	})
	front := httptest.NewServer(s)
	defer front.Close()
	for range 2 {
		assertFailedSharedHTTP(t, front.Client(), front.URL+"/item")
	}
	if calls.Load() != 2 {
		t.Fatalf("truncated response cached: %d origin calls", calls.Load())
	}
}

type disconnectedClient struct{ header http.Header }

func (w *disconnectedClient) Header() http.Header       { return w.header }
func (w *disconnectedClient) WriteHeader(int)           {}
func (w *disconnectedClient) Write([]byte) (int, error) { return 0, fmt.Errorf("client disconnected") }

func TestClientFailureDoesNotDiscardCompletedObject(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		fmt.Fprint(w, "complete origin")
	})
	func() {
		defer func() {
			if got := recover(); got != http.ErrAbortHandler {
				t.Errorf("transfer abort = %v", got)
			}
		}()
		s.ServeHTTP(&disconnectedClient{make(http.Header)}, httptest.NewRequest("GET", "http://cache.test/item", nil))
	}()
	got := fetch(s, "http://cache.test/item", nil)
	if calls.Load() != 1 || got.Body.String() != "complete origin" {
		t.Fatalf("completed object lost after client failure: calls=%d body=%q", calls.Load(), got.Body.String())
	}
}

func TestRequestBypassAndRangeCannotPoisonOrdinaryGet(t *testing.T) {
	headers := []http.Header{{"Cookie": {"session=alice"}}, {"Cache-Control": {"no-store"}}, {"Range": {"bytes=0-2"}}}
	for _, h := range headers {
		t.Run(fmt.Sprint(h), func(t *testing.T) {
			var calls atomic.Int32
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					calls.Add(1)
				}
				w.Header().Set("Cache-Control", "public, max-age=3600")
				if r.Header.Get("Range") != "" {
					w.Header().Set("Content-Range", "bytes 0-2/8")
					w.WriteHeader(206)
					fmt.Fprint(w, "par")
					return
				}
				fmt.Fprint(w, "ordinary")
			})
			fetch(s, "http://cache.test/item", h)
			got := fetch(s, "http://cache.test/item", nil)
			if calls.Load() != 2 || got.Code != 200 || got.Body.String() != "ordinary" {
				t.Fatalf("bypass request populated cache: calls=%d response=%d %q", calls.Load(), got.Code, got.Body.String())
			}
			fetch(s, "http://cache.test/item", h)
			if calls.Load() != 3 {
				t.Fatal("bypass request reused warm object")
			}
		})
	}
}

func TestHTTPIdentityKeepsObjectsSeparate(t *testing.T) {
	var serial atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		fmt.Fprint(w, serial.Add(1))
	})
	targets := []string{"http://a.test/item?id=1", "http://a.test/item?id=2", "http://b.test/item?id=1", "https://a.test/item?id=1", "http://a.test/a%2Fb", "http://a.test/a/b"}
	for i, target := range targets {
		if got := fetch(s, target, nil).Body.String(); got != fmt.Sprint(i+1) {
			t.Fatalf("%s returned %q", target, got)
		}
	}
	for i, target := range targets {
		if got := fetch(s, target, nil).Body.String(); got != fmt.Sprint(i+1) {
			t.Fatalf("repeat %s returned %q", target, got)
		}
	}
}

func TestFreshHitUpdatesAgeAndExpiredObjectRevalidatesByRefetch(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=2")
		w.Header().Set("Age", "1")
		w.Header().Set("Date", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))
		fmt.Fprint(w, "body")
	})
	fetch(s, "http://cache.test/item", nil)
	hit := fetch(s, "http://cache.test/item", nil)
	if hit.Header().Get("Age") != "1" || calls.Load() != 1 {
		t.Fatalf("fresh hit Age=%q calls=%d", hit.Header().Get("Age"), calls.Load())
	}
	time.Sleep(1100 * time.Millisecond)
	fetch(s, "http://cache.test/item", nil)
	if calls.Load() != 2 {
		t.Fatal("future Date extended freshness")
	}
}

func TestConcurrentFillDoesNotShareAnUncommittedResponse(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var count atomic.Int32
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		if count.Add(1) == 1 {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			close(started)
			<-release
			fmt.Fprint(w, "first")
			return
		}
		fmt.Fprint(w, "second")
	})
	firstDone := make(chan struct{})
	go func() { defer close(firstDone); fetch(s, "http://cache.test/item", nil) }()
	<-started
	secondDone := make(chan string, 1)
	go func() { secondDone <- fetch(s, "http://cache.test/item", nil).Body.String() }()

	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	select {
	case <-secondDone:
		t.Fatal("uncommitted body escaped")
	default:
	}
	close(release)
	<-firstDone
	if body := <-secondDone; body != "first" || calls.Load() != 1 {
		t.Fatalf("body=%q calls=%d", body, calls.Load())
	}

}

func TestRoutingNamespaceSeparatesOrigins(t *testing.T) {
	c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache.dat")}, []int64{32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, body := range []string{"origin A", "origin B"} {
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "public, max-age=3600")
			fmt.Fprint(w, body)
		}))
		cfg := config.GetDefaultConfig()
		cfg.UpstreamTargets = []string{origin.URL}
		p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo)
		if err != nil {
			t.Fatal(err)
		}
		s := server.New(cfg, c, p)
		if got := fetch(s, "http://cache.test/item", nil).Body.String(); got != body {
			t.Errorf("route %s reused %q", body, got)
		}
		origin.Close()
	}
}

func TestTruncatedChunkedOriginAbortsDownstream(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		fmt.Fprint(rw, "HTTP/1.1 200 OK\r\nCache-Control: public, max-age=3600\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n")
		rw.Flush()
	})
	front := httptest.NewServer(s)
	defer front.Close()
	for range 2 {
		assertFailedSharedHTTP(t, front.Client(), front.URL+"/item")
	}
	if calls.Load() != 2 {
		t.Fatal("truncated response was cached")
	}
}

func TestLateTrailersPreventCaching(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		fmt.Fprint(rw, "HTTP/1.1 200 OK\r\nCache-Control: public, max-age=3600\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\nSet-Cookie: user=alice\r\n\r\n")
		rw.Flush()
	})
	front := httptest.NewServer(s)
	defer front.Close()
	for range 2 {
		assertFailedSharedHTTP(t, front.Client(), front.URL+"/item")
	}
	if calls.Load() != 2 {
		t.Fatal("unannounced late response trailers were cached")
	}
}

func TestTrustedFrontendSchemesHaveSeparateCachedRepresentations(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		fmt.Fprint(w, r.Header.Get("X-Forwarded-Proto"))
	}, proxy.WithTrustedProxies([]string{"192.0.2.0/24"}))
	for _, scheme := range []string{"http", "https", "http", "https"} {
		got := fetch(s, "http://cache.test/item", http.Header{"X-Forwarded-Proto": {scheme}}).Body.String()
		if got != scheme {
			t.Fatalf("frontend %s returned cached %q", scheme, got)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("expected two representations, origin calls=%d", calls.Load())
	}
}

func TestColdHeadPreservesMethodAndWarmHeadUsesGet(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("X-Origin-Method", r.Method)
		fmt.Fprint(w, "body")
	})
	cold := httptest.NewRecorder()
	s.ServeHTTP(cold, httptest.NewRequest("HEAD", "http://cache.test/item", nil))
	if calls.Load() != 1 || cold.Header().Get("X-Origin-Method") != "HEAD" || cold.Body.Len() != 0 {
		t.Fatalf("cold HEAD=%d %v %q", cold.Code, cold.Header(), cold.Body.String())
	}
	fetch(s, "http://cache.test/item", nil)
	head := httptest.NewRecorder()
	s.ServeHTTP(head, httptest.NewRequest("HEAD", "http://cache.test/item", nil))
	if calls.Load() != 2 || head.Header().Get("X-Origin-Method") != "GET" || head.Body.Len() != 0 {
		t.Fatalf("HEAD calls=%d method=%s body=%q", calls.Load(), head.Header().Get("X-Origin-Method"), head.Body.String())
	}
}

func TestCacheRequestsPreserveCallerHeadersAndURL(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"origin"`)
		fmt.Fprint(w, "body")
	})
	r := httptest.NewRequest("GET", "http://cache.test/item?b=2&a=1", nil)
	r.Header.Set("If-None-Match", `"client"`)
	r.Header["X-Multi"] = []string{"one", "two"}
	headers, url := r.Header.Clone(), r.URL.String()
	for range 2 {
		response := httptest.NewRecorder()
		s.ServeHTTP(response, r)
		if response.Code != 200 || response.Body.String() != "body" {
			t.Fatalf("response: %d %q", response.Code, response.Body.String())
		}
		if !reflect.DeepEqual(r.Header, headers) || r.URL.String() != url {
			t.Fatal("cache path mutated caller request metadata")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("expected MISS then HIT, origin calls=%d", calls.Load())
	}
}
