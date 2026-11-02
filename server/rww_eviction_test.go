package server_test

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/server"
)

type pacedHash struct {
	hash.Hash
	pause time.Duration
}

func (h pacedHash) Write(p []byte) (int, error) {
	n, err := h.Hash.Write(p)
	if h.pause > 0 {
		time.Sleep(h.pause)
	}
	return n, err
}

func TestConcurrentPublicGETSurvivesRollingDiskWindow(t *testing.T) {
	const total int64 = 100 << 20
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"100m"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, io.NewSectionReader(repeatedRangeSource{total}, 0, total))
	}))
	defer origin.Close()
	cfg := config.GetDefaultConfig()
	cfg.SharedFillPublicStatic = 1
	cfg.RangeBlockBytes = 1 << 20
	cfg.MaxCacheObjectBytes = 128 << 20
	cfg.PurgeToken = "test"
	s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 32<<20, origin.URL)
	endpoint := httptest.NewServer(s)
	defer endpoint.Close()
	type result struct {
		status int
		n      int64
		digest string
		err    error
	}
	for round := 0; round < 2; round++ {
		start := make(chan struct{})
		done := make(chan result, 2)
		for worker := 0; worker < 2; worker++ {
			go func(worker int) {
				<-start
				req, _ := http.NewRequest("GET", endpoint.URL+"/large", nil)
				req.Header.Set("Accept-Encoding", "identity")
				resp, err := endpoint.Client().Do(req)
				if err != nil {
					done <- result{err: err}
					return
				}
				defer resp.Body.Close()
				h := sha256.New()
				pause := time.Duration(worker) * 50 * time.Microsecond
				n, err := io.CopyBuffer(pacedHash{h, pause}, resp.Body, make([]byte, 64<<10))
				done <- result{resp.StatusCode, n, fmt.Sprintf("%x", h.Sum(nil)), err}
			}(worker)
		}
		close(start)
		for worker := 0; worker < 2; worker++ {
			got := <-done
			if got.err != nil || got.status != 200 || got.n != total || got.digest != "015bf0bc9dbc0b010bd9e62cfec4682bfaaef866653e0ce387130dbf574f76a2" {
				t.Fatalf("round=%d HTTP200 body incomplete: status=%d bytes=%d SHA=%s error=%v stats=%+v", round, got.status, got.n, got.digest, got.err, s.Stats())
			}
		}
	}
}

type blockSendBarrier struct {
	http.ResponseWriter
	once            sync.Once
	entered, resume chan struct{}
}

func (w *blockSendBarrier) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *blockSendBarrier) Write(p []byte) (int, error) {
	w.once.Do(func() { w.entered <- struct{}{}; <-w.resume })
	return w.ResponseWriter.Write(p)
}

func TestPreparedRangeBlockEvictedBeforeSendRecovers(t *testing.T) {
	for _, scenario := range []struct {
		name                string
		warmBlocks, readers int
		change              string
	}{
		{"cached-hit", 2, 1, ""}, {"newly-committed-miss-shared", 1, 2, ""}, {"concurrent-miss-shared", 1, 2, ""},
		{"purged-parent", 2, 1, "purge"}, {"observed-new-version", 2, 1, "version"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			const total int64 = 100 << 20
			var newer atomic.Bool
			var mu sync.Mutex
			active, peak, gets := 0, 0, 0
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tag := `"100m"`
				if newer.Load() {
					tag = `"new"`
				}
				w.Header().Set("ETag", tag)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if start, end, ok := originInterval(r); r.Method == "GET" && ok && start <= 1<<20 && end >= (2<<20)-1 {
					mu.Lock()
					active++
					gets++
					peak = max(peak, active)
					mu.Unlock()
					defer func() { mu.Lock(); active--; mu.Unlock() }()
					time.Sleep(10 * time.Millisecond)
				}
				http.ServeContent(w, r, "body", time.Time{}, io.NewSectionReader(repeatedRangeSource{total}, 0, total))
			}))
			defer origin.Close()
			cfg := config.GetDefaultConfig()
			cfg.SharedFillPublicStatic = 1
			cfg.RangeBlockBytes = 1 << 20
			cfg.MaxCacheObjectBytes = 128 << 20
			cfg.PurgeToken = "test"
			s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 32<<20, origin.URL)
			warm := fetch(s, "http://cache.test/large", http.Header{"Range": {fmt.Sprintf("bytes=0-%d", scenario.warmBlocks*(1<<20)-1)}, "Accept-Encoding": {"identity"}})
			if warm.Code != 206 || warm.Body.Len() != scenario.warmBlocks*(1<<20) {
				t.Fatal("warm Range failed")
			}
			entered, resume := make(chan struct{}, scenario.readers), make(chan struct{})
			var release sync.Once
			var requests atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) <= int32(scenario.readers) {
					w = &blockSendBarrier{ResponseWriter: w, entered: entered, resume: resume}
				}
				s.ServeHTTP(w, r)
			}))
			defer func() { release.Do(func() { close(resume) }); endpoint.Close() }()
			type result struct {
				status int
				n      int64
				digest string
				err    error
			}
			request := func() result {
				req, _ := http.NewRequest("GET", endpoint.URL+"/large", nil)
				req.Host = "cache.test"
				req.Header.Set("Accept-Encoding", "identity")
				resp, err := endpoint.Client().Do(req)
				if err != nil {
					return result{err: err}
				}
				defer resp.Body.Close()
				h := sha256.New()
				n, err := io.Copy(h, resp.Body)
				return result{resp.StatusCode, n, fmt.Sprintf("%x", h.Sum(nil)), err}
			}
			held := make(chan result, scenario.readers)
			for n := 0; n < scenario.readers; n++ {
				go func() { held <- request() }()
			}
			for n := 0; n < scenario.readers; n++ {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("reader did not reach first-block barrier")
				}
			}
			// Hold consumption until the prefetch MISS has actually committed;
			// the fast reader then evicts that prepared allocation.
			if scenario.name != "concurrent-miss-shared" {
				waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
			}
			fast := request()
			if fast.err != nil || fast.status != 200 || fast.n != total || fast.digest != "015bf0bc9dbc0b010bd9e62cfec4682bfaaef866653e0ce387130dbf574f76a2" {
				t.Fatalf("fast body=%+v", fast)
			}
			if scenario.change == "purge" {
				req := httptest.NewRequest("PURGE", "http://cache.test/large", nil)
				req.Header.Set("Authorization", "Bearer test")
				response := httptest.NewRecorder()
				s.ServeHTTP(response, req)
				if response.Code != 200 {
					t.Fatalf("purge=%d", response.Code)
				}
			}
			if scenario.change == "version" {
				newer.Store(true)
				changed := fetch(s, "http://cache.test/large", http.Header{"Range": {"bytes=0-0"}, "Accept-Encoding": {"identity"}, "Cache-Control": {"no-cache"}})
				if changed.Code != 206 {
					t.Fatalf("new version=%d", changed.Code)
				}
			}
			mu.Lock()
			before := gets
			mu.Unlock()
			release.Do(func() { close(resume) })
			for n := 0; n < scenario.readers; n++ {
				got := <-held
				if scenario.change != "" {
					if got.err == nil || got.status != 200 || got.n != 1<<20 {
						t.Fatalf("old stream must abort: %+v", got)
					}
				} else if got.err != nil || got.status != 200 || got.n != total || got.digest != "015bf0bc9dbc0b010bd9e62cfec4682bfaaef866653e0ce387130dbf574f76a2" {
					t.Fatalf("held body=%+v", got)
				}
			}
			waitFillStats(t, s, func(st server.Stats) bool {
				return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.RangeReaders == 0 && st.BufferReserved == 0
			})
			mu.Lock()
			defer mu.Unlock()
			if peak != 1 {
				t.Fatalf("duplicate active block producer: %d", peak)
			}
			if scenario.change != "" && gets != before {
				t.Fatalf("invalidated old block recovered: before=%d after=%d", before, gets)
			}
			if scenario.change == "" && gets < 2 {
				t.Fatalf("did not exercise block recovery: GETs=%d", gets)
			}
		})
	}
}
