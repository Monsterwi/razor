package server_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
	"github.com/Monsterwi/razor/server"
)

// The source stays reachable after health removal so an accidental bypass is observable.
func TestRangeFreshCachedIntervalSurvivesAllOriginsUnhealthy(t *testing.T) {
	data := bytes.Repeat([]byte("offline-range-"), 180000)
	var unavailable atomic.Bool
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if unavailable.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		calls.Add(1)
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer upstream.Close()
	c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache")}, []int64{32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Init(); err != nil {
		t.Fatal(err)
	}
	p, err := proxy.NewHTTPProxy([]string{upstream.URL}, "round-robin")
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(config.GetDefaultConfig(), c, p)
	t.Cleanup(func() { s.Shutdown() })
	front := httptest.NewServer(s)
	defer front.Close()
	read := func() (int, []byte) {
		req, _ := http.NewRequest(http.MethodGet, front.URL+"/item", nil)
		req.Header.Set("Range", "bytes=100-2097151")
		resp, err := front.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}
	if status, body := read(); status != 206 || !bytes.Equal(body, data[100:2097152]) {
		t.Fatalf("warm status=%d bytes=%d", status, len(body))
	}
	unavailable.Store(true)
	if err := p.StartHealthChecks(proxy.HealthConfig{Interval: time.Millisecond, Timeout: time.Second, Failures: 1, Successes: 1, Path: "/health"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for p.ReadAlive(upstream.URL) {
		if time.Now().After(deadline) {
			t.Fatal("origin not removed")
		}
		time.Sleep(time.Millisecond)
	}
	before := calls.Load()
	if status, body := read(); status != 206 || !bytes.Equal(body, data[100:2097152]) {
		t.Fatalf("offline fresh interval status=%d bytes=%d", status, len(body))
	}
	if calls.Load() != before {
		t.Fatal("offline HIT contacted origin")
	}
}

func TestRangeOfflineMissesNeverContactUnhealthySource(t *testing.T) {
	for _, mode := range []string{"missing-block", "stale-manifest", "stale-block", "if-range-mismatch", "purged"} {
		t.Run(mode, func(t *testing.T) {
			data := bytes.Repeat([]byte("x"), 2<<20)
			var unavailable atomic.Bool
			var calls atomic.Int64
			var clock atomic.Int64
			clock.Store(time.Now().Unix())
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					if unavailable.Load() {
						w.WriteHeader(503)
					}
					return
				}
				calls.Add(1)
				w.Header().Set("ETag", `"one"`)
				w.Header().Set("Date", time.Unix(clock.Load(), 0).UTC().Format(http.TimeFormat))
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if mode == "stale-manifest" || (mode == "stale-block" && r.Method == http.MethodGet) {
					w.Header().Set("Cache-Control", "public,max-age=1")
				}
				http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
			}))
			defer upstream.Close()
			c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache")}, []int64{32 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Init(); err != nil {
				t.Fatal(err)
			}
			p, err := proxy.NewHTTPProxy([]string{upstream.URL}, "round-robin")
			if err != nil {
				t.Fatal(err)
			}
			cfg := config.GetDefaultConfig()
			cfg.PurgeToken = "offline-test"
			s := server.New(cfg, c, p, server.WithClock(func() time.Time { return time.Unix(clock.Load(), 0) }))
			t.Cleanup(func() { s.Shutdown() })
			front := httptest.NewServer(s)
			defer front.Close()
			read := func(bounds, ifRange string) int {
				req, _ := http.NewRequest(http.MethodGet, front.URL+"/item", nil)
				req.Header.Set("Range", bounds)
				if ifRange != "" {
					req.Header.Set("If-Range", ifRange)
				}
				resp, err := front.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if _, err := io.Copy(io.Discard, resp.Body); err != nil {
					t.Fatal(err)
				}
				return resp.StatusCode
			}
			if status := read("bytes=0-100", ""); status != 206 {
				t.Fatalf("warm status=%d", status)
			}
			unavailable.Store(true)
			if err := p.StartHealthChecks(proxy.HealthConfig{Interval: time.Millisecond, Timeout: time.Second, Failures: 1, Successes: 1, Path: "/health"}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for p.ReadAlive(upstream.URL) {
				if time.Now().After(deadline) {
					t.Fatal("origin not removed")
				}
				time.Sleep(time.Millisecond)
			}
			bounds, ifRange := "bytes=0-100", ""
			switch mode {
			case "missing-block":
				bounds = "bytes=1048576-1048676"
			case "stale-manifest", "stale-block":
				clock.Add(3)
			case "if-range-mismatch":
				ifRange = `"different"`
			case "purged":
				req, _ := http.NewRequest("PURGE", front.URL+"/item", nil)
				req.Header.Set("Authorization", "Bearer offline-test")
				resp, err := front.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != 200 {
					t.Fatalf("PURGE status=%d", resp.StatusCode)
				}
			}
			before := calls.Load()
			if status := read(bounds, ifRange); status != 502 {
				t.Fatalf("offline %s status=%d, want502", mode, status)
			}
			if calls.Load() != before {
				t.Fatal("offline MISS contacted unhealthy source")
			}
		})
	}
}

func TestRangeCorruptCacheFailsSafelyOfflineAfterRestart(t *testing.T) {
	for _, record := range []string{"manifest", "block"} {
		t.Run(record, func(t *testing.T) {
			data := bytes.Repeat([]byte("offline-corruption-payload-"), 50000)
			var unavailable atomic.Bool
			var calls atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					if unavailable.Load() {
						w.WriteHeader(503)
					}
					return
				}
				calls.Add(1)
				w.Header().Set("ETag", `"one"`)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
			}))
			defer upstream.Close()
			path := filepath.Join(t.TempDir(), "cache")
			s, _ := newRangeDiskServer(t, path, 32<<20, []string{upstream.URL}, 64<<20)
			if got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-100"}}); got.Code != 206 {
				t.Fatal(got.Code)
			}
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
			if err := s.Shutdown(); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			needle := []byte("offline-corruption-payload-")
			if record == "manifest" {
				needle = []byte(`"Version":3,"ChunkBytes":1048576`)
			}
			buf := make([]byte, 64<<10)
			found := false
			for offset := int64(0); ; offset += int64(len(buf)) {
				n, readErr := file.ReadAt(buf, offset)
				if i := bytes.Index(buf[:n], needle); i >= 0 {
					if _, err := file.WriteAt([]byte("X"), offset+int64(i)); err != nil {
						t.Fatal(err)
					}
					found = true
					break
				}
				if readErr != nil {
					if readErr != io.EOF {
						t.Fatal(readErr)
					}
					break
				}
			}
			file.Close()
			if !found {
				t.Fatalf("cached %s payload not found", record)
			}
			c, err := cache.NewCache(1, []string{path}, []int64{32 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Init(); err != nil {
				t.Fatal(err)
			}
			p, err := proxy.NewHTTPProxy([]string{upstream.URL}, "round-robin")
			if err != nil {
				t.Fatal(err)
			}
			next := server.New(config.GetDefaultConfig(), c, p)
			t.Cleanup(func() { next.Shutdown() })
			unavailable.Store(true)
			if err := p.StartHealthChecks(proxy.HealthConfig{Interval: time.Millisecond, Timeout: time.Second, Failures: 1, Successes: 1, Path: "/health"}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for p.ReadAlive(upstream.URL) {
				if time.Now().After(deadline) {
					t.Fatal("origin not removed")
				}
				time.Sleep(time.Millisecond)
			}
			before := calls.Load()
			got := fetch(next, "http://cache.test/item", http.Header{"Range": {"bytes=0-100"}})
			if got.Code != 502 {
				t.Fatalf("offline corrupt %s status=%d", record, got.Code)
			}
			if calls.Load() != before {
				t.Fatal("corrupt cache contacted unhealthy source")
			}
		})
	}
}

func TestRangePartialUnhealthyOriginDoesNotMaskHealthyReplica(t *testing.T) {
	var unavailable atomic.Bool
	var aCalls atomic.Int64
	aData, bData := bytes.Repeat([]byte("a"), 2<<20), bytes.Repeat([]byte("b"), 2<<20)
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if unavailable.Load() {
				w.WriteHeader(503)
			}
			return
		}
		aCalls.Add(1)
		w.Header().Set("ETag", `"same"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(aData))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			return
		}
		w.Header().Set("ETag", `"same"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(bData))
	}))
	defer b.Close()
	c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache")}, []int64{32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Init(); err != nil {
		t.Fatal(err)
	}
	// Round-robin starts at index one, so warm only A's first block.
	p, err := proxy.NewHTTPProxy([]string{b.URL, a.URL}, "round-robin")
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(config.GetDefaultConfig(), c, p)
	t.Cleanup(func() { s.Shutdown() })
	got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-100"}})
	if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), aData[:101]) {
		t.Fatalf("warm status=%d", got.Code)
	}
	unavailable.Store(true)
	if err := p.StartHealthChecks(proxy.HealthConfig{Interval: time.Millisecond, Timeout: time.Second, Failures: 1, Successes: 1, Path: "/health"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for p.ReadAlive(a.URL) {
		if time.Now().After(deadline) {
			t.Fatal("origin not removed")
		}
		time.Sleep(time.Millisecond)
	}
	before := aCalls.Load()
	got = fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-2097151"}})
	if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), bData) {
		t.Fatalf("healthy replica status=%d bytes=%d", got.Code, got.Body.Len())
	}
	if aCalls.Load() != before {
		t.Fatal("contacted unhealthy A")
	}
}

func TestRangeOfflineLookupCannotReenableReconfiguredOrigin(t *testing.T) {
	var oldCalls, newCalls atomic.Int64
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oldCalls.Add(1)
		w.Header().Set("ETag", `"same"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, strings.NewReader("old-source"))
	}))
	defer old.Close()
	newer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(503)
			return
		}
		newCalls.Add(1)
		w.Header().Set("ETag", `"same"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, strings.NewReader("new-source"))
	}))
	defer newer.Close()
	s, _ := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{old.URL}, 64<<20)
	if got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-2"}}); got.Code != 206 || got.Body.String() != "old" {
		t.Fatalf("warm status=%d", got.Code)
	}
	cfg := config.GetDefaultConfig()
	cfg.UpstreamTargets, cfg.UpstreamAlgo = []string{newer.URL}, "round-robin"
	cfg.MaxCacheObjectBytes = 64 << 20
	cfg.HealthCheckInterval, cfg.HealthCheckFailures, cfg.HealthCheckSuccesses = 1, 1, 1
	cfg.HealthCheckPath = "/health"
	if err := s.Reload(cfg); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := fetch(s, "http://cache.test/probe", http.Header{"Authorization": {"Bearer test-only"}})
		if got.Code == 502 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new origin not removed")
		}
		time.Sleep(time.Millisecond)
	}
	beforeOld, beforeNew := oldCalls.Load(), newCalls.Load()
	got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-2"}})
	if got.Code != 502 {
		t.Fatalf("removed origin reused: status=%d body=%q", got.Code, got.Body.String())
	}
	if oldCalls.Load() != beforeOld || newCalls.Load() != beforeNew {
		t.Fatal("offline miss contacted removed or unhealthy source")
	}
}
