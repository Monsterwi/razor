package server_test

import (
	"bytes"
	"fmt"
	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/server"
)

func TestSharedFreshnessUsesClockAndRequestConstraints(t *testing.T) {
	now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=100, s-maxage=20, must-revalidate")
		w.Header().Set("Date", now.Add(-5*time.Second).Format(http.TimeFormat))
		w.Header().Set("Age", "2")
		fmt.Fprint(w, "body")
	})
	server.WithClock(func() time.Time { return now })(s)
	target := "http://cache.test/item"
	fetch(s, target, nil)
	now = now.Add(3 * time.Second)
	hit := fetch(s, target, nil)
	if calls.Load() != 1 || hit.Header().Get("Age") != "8" {
		t.Fatalf("Age=%q origin=%d", hit.Header().Get("Age"), calls.Load())
	}
	fetch(s, target, http.Header{"Cache-Control": {"max-age=7"}})
	if calls.Load() != 2 {
		t.Fatal("request max-age reused an older object")
	}
	now = now.Add(14 * time.Second)
	fetch(s, target, http.Header{"Cache-Control": {"min-fresh=2"}})
	if calls.Load() != 3 {
		t.Fatal("min-fresh did not force origin validation")
	}
	now = now.Add(20 * time.Second)
	fetch(s, target, nil)
	if calls.Load() != 4 {
		t.Fatal("s-maxage/must-revalidate served stale")
	}
	fetch(s, target, http.Header{"Cache-Control": {"no-cache"}})
	if calls.Load() != 5 {
		t.Fatal("no-cache did not reach origin")
	}
}

func TestVaryIsolatesEncodingLanguageAndMissingValues(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Vary", "Accept-Encoding, Accept-Language")
		fmt.Fprintf(w, "encoding=%q language=%q present=%t", r.Header.Values("Accept-Encoding"), r.Header.Values("Accept-Language"), r.Header["Accept-Language"] != nil)
	})
	headers := []http.Header{
		{"Accept-Encoding": {"gzip"}, "Accept-Language": {"en"}},
		{"Accept-Encoding": {"br"}, "Accept-Language": {"en"}},
		{"Accept-Encoding": {"gzip"}, "Accept-Language": {"fr"}},
		{"Accept-Encoding": {"gzip"}},
		{"Accept-Encoding": {"gzip"}, "Accept-Language": {""}},
	}
	bodies := make([]string, len(headers))
	for i, h := range headers {
		bodies[i] = fetch(s, "http://cache.test/item", h).Body.String()
	}
	for i, h := range headers {
		got := fetch(s, "http://cache.test/item", h)
		if got.Body.String() != bodies[i] {
			t.Fatalf("variant %v got %q want %q", h, got.Body.String(), bodies[i])
		}
	}
	if calls.Load() != int32(len(headers)) {
		t.Fatalf("origin=%d want %d", calls.Load(), len(headers))
	}
}

func TestRevalidationMerges304AndReplaces200(t *testing.T) {
	now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	version := 1
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=10")
		w.Header().Set("Date", now.Format(http.TimeFormat))
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, version))
		if r.Header.Get("If-None-Match") == fmt.Sprintf(`"v%d"`, version) {
			w.Header().Set("X-Validated", "yes")
			w.Header().Set("Content-Length", "999")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		fmt.Fprintf(w, "body%d", version)
	})
	server.WithClock(func() time.Time { return now })(s)
	target := "http://cache.test/item"
	first := fetch(s, target, nil)
	now = now.Add(11 * time.Second)
	validated := fetch(s, target, http.Header{"If-None-Match": {`"different-client-version"`}})
	if validated.Code != 200 || validated.Body.String() != first.Body.String() || validated.Header().Get("X-Validated") != "yes" || validated.Header().Get("Content-Length") != "5" {
		t.Fatalf("validated=%d %v %q", validated.Code, validated.Header(), validated.Body.String())
	}
	hit := fetch(s, target, nil)
	if hit.Body.String() != "body1" || calls.Load() != 2 {
		t.Fatalf("after 304 body=%q calls=%d", hit.Body.String(), calls.Load())
	}
	version = 2
	replacement := fetch(s, target, http.Header{"Cache-Control": {"no-cache"}, "If-None-Match": {`"v1"`}})
	if replacement.Code != 200 || replacement.Body.String() != "body2" {
		t.Fatalf("replacement=%d %q", replacement.Code, replacement.Body.String())
	}
	if got := fetch(s, target, nil).Body.String(); got != "body2" || calls.Load() != 3 {
		t.Fatalf("new hit=%q calls=%d", got, calls.Load())
	}
}

func TestConditionalGetAndHeadUseCompleteGetMetadata(t *testing.T) {
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"part,one"`)
		w.Header().Set("Last-Modified", "Mon, 19 Oct 2026 00:00:00 GMT")
		fmt.Fprint(w, "complete")
	})
	target := "http://cache.test/item"
	fetch(s, target, nil)
	for _, method := range []string{"GET", "HEAD"} {
		r := httptest.NewRequest(method, target, nil)
		r.Header.Set("If-None-Match", `"different", W/"part,one"`)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, r)
		if rec.Code != 304 || rec.Body.Len() != 0 || rec.Header().Get("Content-Length") != "8" {
			t.Fatalf("%s=%d %v %q", method, rec.Code, rec.Header(), rec.Body.String())
		}
	}
	head := httptest.NewRecorder()
	s.ServeHTTP(head, httptest.NewRequest("HEAD", target, nil))
	if head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Content-Length") != "8" {
		t.Fatalf("HEAD=%d %v %q", head.Code, head.Header(), head.Body.String())
	}
	precedence := fetch(s, target, http.Header{"If-None-Match": {`"different"`}, "If-Modified-Since": {"Tue, 20 Oct 2026 00:00:00 GMT"}})
	if precedence.Code != 200 || precedence.Body.String() != "complete" {
		t.Fatal("If-Modified-Since overrode If-None-Match")
	}
	modified := fetch(s, target, http.Header{"If-Modified-Since": {"Tue, 20 Oct 2026 00:00:00 GMT"}})
	if modified.Code != 304 || modified.Body.Len() != 0 {
		t.Fatal("Last-Modified condition missed")
	}
	if calls.Load() != 1 {
		t.Fatalf("conditional origin calls=%d", calls.Load())
	}
	if got := fetch(s, target, nil).Body.String(); got != "complete" {
		t.Fatal("HEAD replaced GET body")
	}
}

func TestVarySchemaChangesAndStarRetireOldVariants(t *testing.T) {
	var stage atomic.Int32
	stage.Store(1)
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		switch stage.Load() {
		case 1:
			w.Header().Set("Vary", "Accept-Language")
			fmt.Fprint(w, "language:"+r.Header.Get("Accept-Language"))
		case 2:
			w.Header().Set("Vary", "Accept-Encoding")
			fmt.Fprint(w, "encoding:"+r.Header.Get("Accept-Encoding"))
		default:
			w.Header().Set("Vary", "*")
			fmt.Fprint(w, "uncacheable:"+r.Header.Get("Accept-Language"))
		}
	})
	target := "http://cache.test/item"
	fetch(s, target, http.Header{"Accept-Language": {"en"}})
	fetch(s, target, http.Header{"Accept-Language": {"fr"}})
	stage.Store(2)
	fetch(s, target, http.Header{"Accept-Language": {"en"}, "Accept-Encoding": {"gzip"}, "Cache-Control": {"no-cache"}})
	got := fetch(s, target, http.Header{"Accept-Language": {"fr"}, "Accept-Encoding": {"gzip"}})
	if got.Body.String() != "encoding:gzip" || calls.Load() != 3 {
		t.Fatalf("new schema=%q calls=%d", got.Body.String(), calls.Load())
	}
	stage.Store(3)
	fetch(s, target, http.Header{"Accept-Encoding": {"gzip"}, "Cache-Control": {"no-cache"}})
	got = fetch(s, target, http.Header{"Accept-Encoding": {"gzip"}, "Accept-Language": {"fr"}})
	if got.Body.String() != "uncacheable:fr" || calls.Load() != 5 {
		t.Fatalf("Vary star retained old family=%q calls=%d", got.Body.String(), calls.Load())
	}
}

func TestVariantLookupSurvivesRestartAndExactPrefixPurge(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Vary", "Accept-Encoding,Accept-Language")
		fmt.Fprint(w, r.URL.Path+":"+r.Header.Get("Accept-Encoding")+":"+r.Header.Get("Accept-Language"))
	}))
	defer upstream.Close()
	paths := []string{filepath.Join(t.TempDir(), "cache-a"), filepath.Join(t.TempDir(), "cache-b")}
	start := func() *server.Server {
		c, err := cache.NewCache(2, paths, []int64{32 << 20, 32 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Init(); err != nil {
			t.Fatal(err)
		}
		cfg := config.GetDefaultConfig()
		cfg.UpstreamTargets = []string{upstream.URL}
		cfg.PurgeToken = "test-secret"
		p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo)
		if err != nil {
			t.Fatal(err)
		}
		return server.New(cfg, c, p)
	}
	s := start()
	headers := []http.Header{{"Accept-Encoding": {"gzip"}, "Accept-Language": {"en"}}, {"Accept-Encoding": {"br"}, "Accept-Language": {"fr"}}}
	targets := []string{"http://cache.test/folder/one", "http://cache.test/folder/two"}
	for _, target := range targets {
		for _, h := range headers {
			fetch(s, target, h)
		}
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	s = start()
	defer s.Shutdown()
	for _, target := range targets {
		for _, h := range headers {
			fetch(s, target, h)
		}
	}
	if calls.Load() != 4 {
		t.Fatalf("restart lost variants: %d", calls.Load())
	}
	if got := purge(s, targets[0], "test-secret"); got.Code != 200 {
		t.Fatal(got.Code)
	}
	for _, target := range targets {
		for _, h := range headers {
			fetch(s, target, h)
		}
	}
	if calls.Load() != 6 {
		t.Fatalf("exact PURGE variants: %d", calls.Load())
	}
	purge(s, "http://cache.test/folder/", "test-secret")
	for _, target := range targets {
		for _, h := range headers {
			fetch(s, target, h)
		}
	}
	if calls.Load() != 10 {
		t.Fatalf("prefix PURGE variants: %d", calls.Load())
	}
}

func TestInFlightRevalidationCannotOverwritePurgeOrNewerResponse(t *testing.T) {
	for _, invalidate := range []bool{false, true} {
		t.Run(fmt.Sprint("purge=", invalidate), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			s, _ := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if call <= 2 {
					w.Header().Set("ETag", `"v1"`)
				} else {
					w.Header().Set("ETag", `"v2"`)
				}
				if call == 2 {
					close(entered)
					<-release
					w.WriteHeader(304)
					return
				}
				if call == 1 {
					fmt.Fprint(w, "old")
				} else {
					fmt.Fprint(w, "new")
				}
			})
			target := "http://cache.test/item"
			fetch(s, target, nil)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- fetch(s, target, http.Header{"Cache-Control": {"no-cache"}}) }()
			<-entered
			if invalidate {
				purge(s, target, "test-secret")
			}
			if got := fetch(s, target, http.Header{"Cache-Control": {"no-cache"}, "X-Validation-Request": {"independent"}}).Body.String(); got != "new" {
				t.Fatal(got)
			}
			close(release)
			if got := <-done; got.Body.String() != "new" {
				t.Fatalf("stale validation body=%q", got.Body.String())
			}
			before := calls.Load()
			got := fetch(s, target, nil)
			if got.Body.String() != "new" || calls.Load() != before {
				t.Fatalf("old validation damaged newer body: %q calls %d -> %d", got.Body.String(), before, calls.Load())
			}
		})
	}
}

func TestUnsafe304MustRefetchWithoutReusingBody(t *testing.T) {
	for _, headers := range []http.Header{
		{"Connection": {"ETag"}},
		{"Content-Encoding": {"gzip"}},
		{"Vary": {"*"}},
		{"Set-Cookie": {"private=true"}},
	} {
		t.Run(fmt.Sprint(headers), func(t *testing.T) {
			var calls atomic.Int32
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("ETag", `"v1"`)
				if call == 2 {
					for key, values := range headers {
						w.Header()[key] = values
					}
					w.WriteHeader(304)
					return
				}
				if call == 1 {
					fmt.Fprint(w, "old")
				} else {
					fmt.Fprint(w, "new")
				}
			})
			target := "http://cache.test/item"
			fetch(s, target, nil)
			got := fetch(s, target, http.Header{"Cache-Control": {"no-cache"}})
			if got.Body.String() != "new" || calls.Load() != 3 {
				t.Fatalf("unsafe 304 reused %q calls=%d", got.Body.String(), calls.Load())
			}
		})
	}
}

func TestUnsafeEmptyFamilyFencesEarlierOriginResponse(t *testing.T) {
	for _, policy := range []string{"vary-star", "no-store"} {
		t.Run(policy, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var serial atomic.Int32
			s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				n := serial.Add(1)
				if n == 1 {
					close(entered)
					<-release
					w.Header().Set("Cache-Control", "public,max-age=3600")
					fmt.Fprint(w, "old")
					return
				}
				if policy == "vary-star" {
					w.Header().Set("Cache-Control", "public,max-age=3600")
					w.Header().Set("Vary", "*")
				} else {
					w.Header().Set("Cache-Control", "no-store")
				}
				fmt.Fprint(w, "private")
			})
			target := "http://cache.test/item"
			done := make(chan struct{})
			go func() { defer close(done); fetch(s, target, nil) }()
			<-entered
			fetch(s, target, http.Header{"X-Policy-Request": {"independent"}})
			close(release)
			<-done
			if got := fetch(s, target, nil).Body.String(); got != "private" || calls.Load() != 3 {
				t.Fatalf("late public response escaped policy fence: %q calls=%d", got, calls.Load())
			}
		})
	}
}

func TestUnsupportedVaryInputsBypassAndVariantFanoutIsBounded(t *testing.T) {
	for _, field := range []string{"X-Forwarded-For", "User-Agent", "*"} {
		t.Run(field, func(t *testing.T) {
			s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("Vary", field)
				fmt.Fprint(w, "body")
			})
			fetch(s, "http://cache.test/item", nil)
			fetch(s, "http://cache.test/item", nil)
			if calls.Load() != 2 {
				t.Fatal("unsupported or rewritten Vary input was reused")
			}
		})
	}
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		fmt.Fprint(w, r.Header.Get("Accept-Language"))
	})
	for i := 0; i < 17; i++ {
		fetch(s, "http://cache.test/item", http.Header{"Accept-Language": {fmt.Sprint(i)}})
	}
	fetch(s, "http://cache.test/item", http.Header{"Accept-Language": {"0"}})
	fetch(s, "http://cache.test/item", http.Header{"Accept-Language": {"16"}})
	if calls.Load() != 18 {
		t.Fatalf("variant cap evicted healthy objects or admitted excess: %d", calls.Load())
	}
}

func TestOriginDelayContributesToAgeAndResponseNoCacheAlwaysValidates(t *testing.T) {
	var nanos atomic.Int64
	nanos.Store(time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, nanos.Load()) }
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=13")
		w.Header().Set("Date", now().Format(http.TimeFormat))
		w.Header().Set("Age", "7")
		nanos.Add(int64(3 * time.Second))
		fmt.Fprint(w, "body")
	})
	server.WithClock(now)(s)
	target := "http://cache.test/item"
	fetch(s, target, nil)
	nanos.Add(int64(2 * time.Second))
	got := fetch(s, target, nil)
	if got.Header().Get("Age") != "12" || calls.Load() != 1 {
		t.Fatalf("delay age=%q calls=%d", got.Header().Get("Age"), calls.Load())
	}
	nanos.Add(int64(time.Second))
	fetch(s, target, nil)
	if calls.Load() != 2 {
		t.Fatal("origin delay extended freshness")
	}
	var validations atomic.Int32
	constrained, originCalls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600,no-cache")
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			validations.Add(1)
			w.WriteHeader(304)
			return
		}
		fmt.Fprint(w, "validated body")
	})
	for range 3 {
		if got := fetch(constrained, target, nil).Body.String(); got != "validated body" {
			t.Fatal(got)
		}
	}
	if originCalls.Load() != 3 || validations.Load() != 2 {
		t.Fatalf("response no-cache calls=%d validations=%d", originCalls.Load(), validations.Load())
	}
}

func Test304CannotStrengthenOrContradictStoredValidator(t *testing.T) {
	for _, tc := range []struct {
		name     string
		old, new http.Header
	}{
		{"weak to strong", http.Header{"Etag": {`W/"v1"`}}, http.Header{"Etag": {`"v1"`}}},
		{"new etag after date validation", http.Header{"Last-Modified": {"Mon, 19 Oct 2026 00:00:00 GMT"}}, http.Header{"Etag": {`"unidentified"`}}},
		{"contradictory modified date", http.Header{"Last-Modified": {"Mon, 19 Oct 2026 00:00:00 GMT"}}, http.Header{"Last-Modified": {"Tue, 20 Oct 2026 00:00:00 GMT"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if call == 2 {
					for k, v := range tc.new {
						w.Header()[k] = v
					}
					w.WriteHeader(304)
					return
				}
				if call == 1 {
					for k, v := range tc.old {
						w.Header()[k] = v
					}
					fmt.Fprint(w, "old bytes")
				} else {
					fmt.Fprint(w, "new bytes")
				}
			})
			target := "http://cache.test/item"
			fetch(s, target, nil)
			got := fetch(s, target, http.Header{"Cache-Control": {"no-cache"}})
			if got.Body.String() != "new bytes" || calls.Load() != 3 {
				t.Fatalf("unidentified 304 reused %q calls=%d", got.Body.String(), calls.Load())
			}
		})
	}
}

func TestUnconditionalRetryCannotReturnAnotherUnusable304(t *testing.T) {
	var calls atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if calls.Add(1) == 1 {
			w.Header().Set("ETag", `"original"`)
			fmt.Fprint(w, "body")
			return
		}
		w.Header().Set("ETag", `"wrong"`)
		w.WriteHeader(304)
	})
	target := "http://cache.test/item"
	fetch(s, target, nil)
	got := fetch(s, target, http.Header{"Cache-Control": {"no-cache"}})
	if got.Code != http.StatusBadGateway || calls.Load() != 3 {
		t.Fatalf("unusable retry=%d calls=%d", got.Code, calls.Load())
	}
}

func TestUnsafeResponseFencesAlreadyReservedWriter(t *testing.T) {
	for _, policy := range []string{"vary-star", "no-store"} {
		t.Run(policy, func(t *testing.T) {
			firstEntered, unsafeEntered := make(chan struct{}), make(chan struct{})
			firstHeaders, unsafeResponse, firstBody := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var serial atomic.Int32
			s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				switch serial.Add(1) {
				case 1:
					close(firstEntered)
					<-firstHeaders
					w.Header().Set("Cache-Control", "public,max-age=3600")
					w.Header().Set("Content-Length", "3")
					fmt.Fprint(w, "o")
					w.(http.Flusher).Flush()
					<-firstBody
					fmt.Fprint(w, "ld")
					return
				case 2:
					close(unsafeEntered)
					<-unsafeResponse
				}
				if policy == "vary-star" {
					w.Header().Set("Cache-Control", "public,max-age=3600")
					w.Header().Set("Vary", "*")
				} else {
					w.Header().Set("Cache-Control", "no-store")
				}
				fmt.Fprint(w, "new")
			})
			front := httptest.NewServer(s)
			defer front.Close()
			target := front.URL + "/item"
			firstDone := make(chan struct{})
			go func() {
				defer close(firstDone)
				assertFailedSharedHTTP(t, front.Client(), target)
			}()
			<-firstEntered
			unsafeDone := make(chan *httptest.ResponseRecorder, 1)
			go func() { unsafeDone <- fetch(s, target, http.Header{"X-Policy-Request": {"independent"}}) }()
			<-unsafeEntered
			close(firstHeaders)
			waitFillStats(t, s, func(st server.Stats) bool { return st.CacheWriters == 2 && st.Origin.Bytes > 0 })
			close(unsafeResponse)
			unsafe := <-unsafeDone
			close(firstBody)
			<-firstDone
			got := fetch(s, target, nil)
			if unsafe.Body.String() != "new" || got.Body.String() != "new" || calls.Load() != 3 {
				t.Fatalf("unsafe=%q next=%q origin calls=%d", unsafe.Body.String(), got.Body.String(), calls.Load())
			}
		})
	}
}

func TestUnsafeResponseRetiresValidCommittedHeadAfterStorageWrap(t *testing.T) {
	for _, policy := range []string{"vary-star", "no-store"} {
		t.Run(policy, func(t *testing.T) {
			entered, resume := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if n == 1 {
					fmt.Fprint(w, "old")
					return
				}
				if n == 2 {
					close(entered)
					<-resume
				}
				if policy == "vary-star" {
					w.Header().Set("Vary", "*")
				} else {
					w.Header().Set("Cache-Control", "no-store")
				}
				fmt.Fprint(w, "new")
			}))
			defer origin.Close()
			c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache")}, []int64{8 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Init(); err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			padding := bytes.Repeat([]byte("p"), 1<<20)
			fill := func() {
				w, err := c.OpenWrite([]byte("padding"), 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = w.Write(padding); err != nil {
					t.Fatal(err)
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for range 6 {
				fill()
			}
			cfg := config.GetDefaultConfig()
			cfg.UpstreamTargets = []string{origin.URL}
			p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			s := server.New(cfg, c, p)
			target := "http://cache.test/item"
			first := fetch(s, target, nil)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- fetch(s, target, http.Header{"Cache-Control": {"no-cache"}}) }()
			<-entered
			// Wrap at the beginning while the committed head remains in the tail.
			for range 2 {
				fill()
			}
			close(resume)
			unsafe := <-done
			next := fetch(s, target, nil)
			if first.Body.String() != "old" || unsafe.Body.String() != "new" || next.Body.String() != "new" || calls.Load() != 3 {
				t.Fatalf("first=%q unsafe=%q next=%q origin calls=%d", first.Body.String(), unsafe.Body.String(), next.Body.String(), calls.Load())
			}
		})
	}
}

func Test304RefreshPreservesRecoveredPreviousPhaseFragments(t *testing.T) {
	body := bytes.Repeat([]byte("body"), (1<<20)/4+1)
	var calls, validations atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			validations.Add(1)
			w.Header().Set("X-Refreshed", "yes")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Write(body)
	}))
	defer origin.Close()
	path := filepath.Join(t.TempDir(), "cache")
	start := func() (*server.Server, *cache.Cache) {
		c, err := cache.NewCache(1, []string{path}, []int64{8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Init(); err != nil {
			t.Fatal(err)
		}
		cfg := config.GetDefaultConfig()
		cfg.UpstreamTargets = []string{origin.URL}
		p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo)
		if err != nil {
			t.Fatal(err)
		}
		return server.New(cfg, c, p), c
	}
	s, c := start()
	padding := bytes.Repeat([]byte("p"), 1<<20)
	fill := func() {
		w, err := c.OpenWrite([]byte("padding"), 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write(padding); err != nil {
			t.Fatal(err)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for range 12 {
		fill()
	}
	target := "http://cache.test/item"
	if got := fetch(s, target, nil); !bytes.Equal(got.Body.Bytes(), body) {
		t.Fatal("initial response body mismatch")
	}
	for range 2 {
		fill()
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	s, c = start()
	defer s.Shutdown()
	refreshed := fetch(s, target, http.Header{"Cache-Control": {"no-cache"}})
	if refreshed.Code != 200 || refreshed.Header().Get("X-Refreshed") != "yes" || !bytes.Equal(refreshed.Body.Bytes(), body) || calls.Load() != 2 || validations.Load() != 1 {
		t.Fatalf("recovered 304 status=%d refreshed=%q bytes=%d calls=%d validations=%d", refreshed.Code, refreshed.Header().Get("X-Refreshed"), refreshed.Body.Len(), calls.Load(), validations.Load())
	}
	if got := fetch(s, target, nil); !bytes.Equal(got.Body.Bytes(), body) || got.Header().Get("X-Refreshed") != "yes" || calls.Load() != 2 {
		t.Fatal("refreshed previous-phase body was not retained")
	}
	// The first append after recovery must not clear the recovered current phase.
	r, err := c.OpenReadCommitted([]byte("padding"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, padding) {
		t.Fatalf("304 append damaged recovered current-phase object: bytes=%d err=%v", len(got), err)
	}
}

// A parsed lookup policy must not freeze age while a request is descheduled.
func TestCachedPolicyAdvancesBetweenLookupAndResponse(t *testing.T) {
	for _, expiry := range []bool{true, false} {
		name := "age"
		if expiry {
			name = "expiry"
		}
		t.Run(name, func(t *testing.T) {
			base := time.Now().UTC().Truncate(time.Second)
			lifetime := 60
			if expiry {
				lifetime = 1
			}
			s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", fmt.Sprintf("public,max-age=%d", lifetime))
				w.Header().Set("Date", base.Format(http.TimeFormat))
				fmt.Fprint(w, "body")
			})
			server.WithClock(func() time.Time { return base })(s)
			fetch(s, "http://cache.test/clock", nil)
			var ticks atomic.Int32
			server.WithClock(func() time.Time {
				n := ticks.Add(1)
				if (expiry && n >= 2) || (!expiry && n >= 3) {
					return base.Add(3 * time.Second)
				}
				return base
			})(s)
			response := fetch(s, "http://cache.test/clock", nil)
			if response.Code != 200 || response.Body.String() != "body" {
				t.Fatal("clock test response failed")
			}
			if expiry && calls.Load() != 2 {
				t.Fatal("served an object that expired after lookup")
			}
			if !expiry && (calls.Load() != 1 || response.Header().Get("Age") != "3") {
				t.Fatalf("response age frozen: %v origin=%d", response.Header(), calls.Load())
			}
		})
	}
}
