package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/logger"
	"github.com/Monsterwi/razor/server"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHundredEquivalentColdRequestsShareOneCompleteFill(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Header().Set("Cache-Control", "public,max-age=3600")
		fmt.Fprint(w, "complete shared body")
	})
	const clients = 100
	responses := make(chan *httptest.ResponseRecorder, clients)
	for i := 0; i < clients; i++ {
		go func(i int) {
			query := "b=2&a=1"
			req := httptest.NewRequest("GET", "http://cache.test/item?"+query, nil)
			req.RemoteAddr = fmt.Sprintf("192.0.2.1:%d", 10000+i)
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			responses <- rec
		}(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Stats().Requests < clients && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Stats().Requests != clients {
		t.Fatalf("only %d clients admitted", s.Stats().Requests)
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == clients })
	once.Do(func() { close(release) })
	for range clients {
		select {
		case rec := <-responses:
			if rec.Code != 200 || rec.Body.String() != "complete shared body" {
				t.Errorf("response=%d %q", rec.Code, rec.Body.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("shared fill did not finish")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("actual origin requests=%d, want 1", calls.Load())
	}
}

func waitFillStats(t *testing.T, s *server.Server, ready func(server.Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ready(s.Stats()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("fill condition not reached: %+v", s.Stats())
}
func startFill(s *server.Server, ctx context.Context, headers http.Header) <-chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		r := httptest.NewRequest("GET", "http://cache.test/item", nil).WithContext(ctx)
		if headers != nil {
			r.Header = headers
		}
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, r)
		done <- rec
	}()
	return done
}
func TestSharedFillCreatorCancellationKeepsOtherSubscribers(t *testing.T) {
	entered, release, cancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			close(cancelled)
			return
		}
		w.Header().Set("Cache-Control", "public,max-age=3600")
		fmt.Fprint(w, "complete")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startFill(s, ctx, nil)
	<-entered
	second := startFill(s, context.Background(), nil)
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	cancel()
	<-first
	select {
	case <-cancelled:
		t.Fatal("creator cancelled the remaining subscription")
	default:
	}
	close(release)
	got := <-second
	if got.Code != 200 || got.Body.String() != "complete" || calls.Load() != 1 {
		t.Fatalf("response=%d %q origin=%d", got.Code, got.Body.String(), calls.Load())
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
}
func TestSharedFillAllSubscribersCancelOrigin(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(cancelled) })
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	first := startFill(s, firstCtx, nil)
	<-entered
	second := startFill(s, secondCtx, nil)
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	cancelFirst()
	cancelSecond()
	<-first
	<-second
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("origin retained after last subscriber left")
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
	if calls.Load() != 1 {
		t.Fatalf("origin=%d", calls.Load())
	}
}
func TestPrivateFillIsExclusiveAndFollowersDoNotRetry(t *testing.T) {
	for _, policy := range []string{"private,max-age=3600", "no-store"} {
		t.Run(policy, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				w.Header().Set("Cache-Control", policy)
				fmt.Fprint(w, "creator secret")
			})
			first := startFill(s, context.Background(), nil)
			<-entered
			followers := make([]<-chan *httptest.ResponseRecorder, 99)
			for i := range followers {
				followers[i] = startFill(s, context.Background(), nil)
			}
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 100 })
			close(release)
			got := <-first
			if got.Code != 200 || got.Body.String() != "creator secret" {
				t.Fatalf("creator=%d %q", got.Code, got.Body.String())
			}
			for _, done := range followers {
				got := <-done
				if got.Code != 503 || got.Body.String() == "creator secret" {
					t.Fatalf("follower=%d %q", got.Code, got.Body.String())
				}
			}
			waitFillStats(t, s, func(st server.Stats) bool {
				return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0
			})
			if calls.Load() != 1 || s.Stats().SharedFillUnshareable != 1 {
				t.Fatalf("origin=%d stats=%+v", calls.Load(), s.Stats())
			}
		})
	}
}

func TestPurgePreventsJoiningOldFill(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	s, _ := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) {
		serial := calls.Add(1)
		if serial == 1 {
			close(entered)
			<-release
		}
		w.Header().Set("Cache-Control", "public,max-age=3600")
		fmt.Fprint(w, serial)
	})
	first := startFill(s, context.Background(), nil)
	<-entered
	follower := startFill(s, context.Background(), nil)
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	if got := purge(s, "http://cache.test/item", "test-secret"); got.Code != 200 {
		t.Fatal(got.Code)
	}
	next := fetch(s, "http://cache.test/item", nil)
	if next.Code != 200 || next.Body.String() != "2" {
		t.Fatalf("post-PURGE=%d %q", next.Code, next.Body.String())
	}
	close(release)
	<-first
	if got := <-follower; got.Code == 200 {
		t.Fatalf("old task published %q", got.Body.String())
	}
	if got := fetch(s, "http://cache.test/item", nil); got.Body.String() != "2" || calls.Load() != 2 {
		t.Fatalf("cache=%q origin=%d", got.Body.String(), calls.Load())
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 })
}

func TestFullRequestIdentityAndAuthenticatedBypass(t *testing.T) {
	for _, field := range []string{"X-Client-Variant", "Accept-Language", "Authorization", "Cookie"} {
		t.Run(field, func(t *testing.T) {
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				entered <- struct{}{}
				<-release
				w.Header().Set("Cache-Control", "private")
				w.Header().Set("Vary", field)
				fmt.Fprint(w, r.Header.Get(field))
			})
			first := startFill(s, context.Background(), http.Header{field: {"alice"}})
			second := startFill(s, context.Background(), http.Header{field: {"bob"}})
			for range 2 {
				select {
				case <-entered:
				case <-time.After(time.Second):
					close(release)
					t.Fatal("distinct requests shared an origin")
				}
			}
			close(release)
			if got := <-first; got.Code != 200 || got.Body.String() != "alice" {
				t.Fatalf("alice=%d %q", got.Code, got.Body.String())
			}
			if got := <-second; got.Code != 200 || got.Body.String() != "bob" {
				t.Fatalf("bob=%d %q", got.Code, got.Body.String())
			}
			if calls.Load() != 2 || s.Stats().SharedFillJoined != 0 {
				t.Fatalf("origin=%d stats=%+v", calls.Load(), s.Stats())
			}
		})
	}
}

func TestSharedFillBudgetsRejectWithoutOriginRetry(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(entered)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer origin.Close()
	cfg := config.GetDefaultConfig()
	cfg.SharedFillMaxTasks = 1
	cfg.SharedFillMaxWaiters = 2
	cfg.SharedFillTimeout = 1
	s := lifecycleServer(t, origin.URL, cfg)
	first := startFill(s, context.Background(), nil)
	<-entered
	second := startFill(s, context.Background(), nil)
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	for _, target := range []string{"http://cache.test/item", "http://cache.test/other"} {
		if got := fetch(s, target, nil); got.Code != 503 {
			t.Fatalf("budget rejected with status=%d", got.Code)
		}
	}
	for _, done := range []<-chan *httptest.ResponseRecorder{first, second} {
		if got := <-done; got.Code != 504 {
			t.Fatalf("deadline status=%d", got.Code)
		}
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("deadline retained origin")
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
	if calls.Load() != 1 || s.Stats().SharedFillFailed != 1 {
		t.Fatalf("origin=%d stats=%+v", calls.Load(), s.Stats())
	}
}

func TestCancelledCreatorCannotTransferPrivateBody(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Header().Set("Cache-Control", "private")
		fmt.Fprint(w, "secret")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startFill(s, ctx, nil)
	<-entered
	second := startFill(s, context.Background(), nil)
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	cancel()
	<-first
	close(release)
	if got := <-second; got.Code != 503 || got.Body.String() == "secret" {
		t.Fatalf("private follower=%d %q", got.Code, got.Body.String())
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.Origin.InFlight == 0 && st.CacheWriters == 0
	})
	if calls.Load() != 1 {
		t.Fatalf("origin=%d", calls.Load())
	}
}

func TestFailedFillWakesHundredSubscribersWithoutRetry(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Content-Length", "100")
		fmt.Fprint(w, "truncated")
	})
	front := httptest.NewServer(s)
	defer front.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	done := make(chan struct{}, 100)
	start := func() {
		go func() {
			assertFailedSharedHTTP(t, client, front.URL+"/item")
			done <- struct{}{}
		}()
	}
	start()
	<-entered
	for range 99 {
		start()
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 100 })
	close(release)
	for range 100 {
		<-done
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
	if calls.Load() != 1 || s.Stats().SharedFillFailed != 1 {
		t.Fatalf("origin=%d stats=%+v", calls.Load(), s.Stats())
	}
}

func TestSharedFillLogsFinalCacheOutcome(t *testing.T) {
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/private" {
			w.Header().Set("Cache-Control", "private")
			fmt.Fprint(w, "private")
			return
		}
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"one"`)
		if r.Header.Get("If-None-Match") == `"one"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		fmt.Fprint(w, "body")
	})
	path := filepath.Join(t.TempDir(), "access.log")
	logged := logger.AccessLogMiddleware(path)(s)
	fetchWhole(logged, "http://cache.test/item", nil)
	fetchWhole(logged, "http://cache.test/item", nil)
	fetchWhole(logged, "http://cache.test/item", http.Header{"Cache-Control": {"no-cache"}})
	fetchWhole(logged, "http://cache.test/private", nil)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	expected := []string{"MISS", "HIT", "REVALIDATE", "BYPASS"}
	if len(lines) != len(expected) {
		t.Fatalf("access logs=%s", data)
	}
	for i, line := range lines {
		var entry struct {
			Status string `json:"cache_status"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Status != expected[i] {
			t.Fatalf("request %d: got %s want %s", i, entry.Status, expected[i])
		}
	}
	st := s.Stats()
	if st.Misses != 1 || st.Hits != 1 || st.Bypass != 1 {
		t.Fatalf("cache outcome counters overlap: %+v", st)
	}
}
