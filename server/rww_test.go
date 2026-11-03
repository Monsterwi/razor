package server_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
	"github.com/Monsterwi/razor/server"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// originBoundaryWriter gates or fails at an absolute object offset, independent
// of whether the cache requested one chunk or a merged run.
type originBoundaryWriter struct {
	http.ResponseWriter
	boundary, offset     int64
	initialized, crossed bool
	cross                func() error
}

func (w *originBoundaryWriter) Write(p []byte) (int, error) {
	if !w.initialized {
		_, _ = fmt.Sscanf(w.Header().Get("Content-Range"), "bytes %d-", &w.offset)
		w.initialized = true
	}
	written := 0
	if !w.crossed {
		before := min(int64(len(p)), max(w.boundary-w.offset, int64(0)))
		if before > 0 {
			n, err := w.ResponseWriter.Write(p[:before])
			written += n
			w.offset += int64(n)
			if err != nil {
				return written, err
			}
			if int64(n) != before {
				return written, io.ErrShortWrite
			}
			p = p[before:]
		}
		if len(p) == 0 {
			return written, nil
		}
		w.crossed = true
		if flush, ok := w.ResponseWriter.(http.Flusher); ok {
			flush.Flush()
		}
		if err := w.cross(); err != nil {
			return written, err
		}
	}
	n, err := w.ResponseWriter.Write(p)
	w.offset += int64(n)
	return written + n, err
}
func originInterval(r *http.Request) (int64, int64, bool) {
	var start, end int64
	n, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
	return start, end, err == nil && n == 2
}

func TestRangeCommittedBlockArrivesBeforeSecondBlock(t *testing.T) { committedBlockBarrier(t, false) }
func TestPublicGETCommittedBlockArrivesBeforeSecondBlock(t *testing.T) {
	committedBlockBarrier(t, true)
}
func committedBlockBarrier(t *testing.T, full bool) {
	data := bytes.Repeat([]byte("a"), 2<<20)
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	origin := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"one"`)
		out := &originBoundaryWriter{ResponseWriter: w, boundary: 1 << 20, cross: func() error {
			close(blocked)
			select {
			case <-release:
				return nil
			case <-r.Context().Done():
				return r.Context().Err()
			}
		}}
		http.ServeContent(out, r, "body", time.Time{}, bytes.NewReader(data))
	}
	var s http.Handler
	if full {
		s, _ = publicCacheServer(t, origin)
	} else {
		s, _ = cacheServer(t, origin)
	}
	endpoint := httptest.NewServer(s)
	defer func() { once.Do(func() { close(release) }); endpoint.Close() }()
	first := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest("GET", endpoint.URL+"/object", nil)
		req.Header.Set("Accept-Encoding", "identity")
		if !full {
			req.Header.Set("Range", "bytes=0-2097151")
		}
		resp, err := endpoint.Client().Do(req)
		if err != nil {
			first <- err
			return
		}
		defer resp.Body.Close()
		b := make([]byte, 1<<20)
		_, err = io.ReadFull(resp.Body, b)
		if err == nil && (resp.StatusCode != map[bool]int{true: 200, false: 206}[full] || !bytes.Equal(b, data[:1<<20])) {
			err = fmt.Errorf("first block status=%d", resp.StatusCode)
		}
		first <- err
		_, _ = io.Copy(io.Discard, resp.Body)
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("second block bytes never reached origin gate")
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first committed block withheld behind second block")
	}
	once.Do(func() { close(release) })
}

func assertIncompleteRange(t *testing.T, s http.Handler, value string, total int) {
	t.Helper()
	endpoint := httptest.NewServer(s)
	defer endpoint.Close()
	req, _ := http.NewRequest("GET", endpoint.URL+"/item", nil)
	req.Host = "cache.test"
	req.Header.Set("Range", value)
	resp, err := endpoint.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 206 || err == nil || len(data) == total {
		t.Fatalf("expected incomplete206, status=%d bytes=%d err=%v", resp.StatusCode, len(data), err)
	}
}

func blockServer(t *testing.T, cfg *config.Config, path string, diskBytes int64, origin string) *server.Server {
	t.Helper()
	cfg.UpstreamTargets = []string{origin}
	cfg.UpstreamAlgo = "round-robin"
	c, err := cache.NewCache(1, []string{path}, []int64{diskBytes})
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
	t.Cleanup(func() {
		if err := s.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func TestConfiguredRangeBlocksAndNonAlignedTail(t *testing.T) {
	for _, size := range []int{1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := bytes.Repeat([]byte("0123456789abcdef"), (3*size+128)/16)
			data = data[:3*size+17]
			var originBytes atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"layout"`)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				http.ServeContent(&countRangeBytes{ResponseWriter: w, bytes: &originBytes}, r, "body", time.Time{}, bytes.NewReader(data))
			}))
			defer origin.Close()
			cfg := config.GetDefaultConfig()
			cfg.RangeBlockBytes = size
			cfg.SharedFillPublicStatic = 1
			s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 64<<20, origin.URL)
			for _, interval := range [][2]int{{size - 7, 2*size + 9}, {3 * size, 3*size + 16}, {2, 8}} {
				got := fetch(s, "http://cache.test/item", http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", interval[0], interval[1])}, "Accept-Encoding": {"identity"}})
				if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data[interval[0]:interval[1]+1]) {
					t.Fatalf("range %v status=%d bytes=%d", interval, got.Code, got.Body.Len())
				}
			}
			if originBytes.Load() != int64(len(data)) {
				t.Fatalf("origin aligned union=%d want=%d", originBytes.Load(), len(data))
			}
			got := fetch(s, "http://cache.test/item", http.Header{"Accept-Encoding": {"identity"}})
			if got.Code != 200 || !bytes.Equal(got.Body.Bytes(), data) {
				t.Fatalf("full GET=%d bytes=%d", got.Code, got.Body.Len())
			}
			if len(data) > max(size, 1<<20) && originBytes.Load() != int64(len(data)) {
				t.Fatalf("GET did not reuse Range blocks: %d", originBytes.Load())
			}
			waitFillStats(t, s, func(st server.Stats) bool {
				return st.RangeReaders == 0 && st.SharedFillTasks == 0 && st.BufferReserved == 0
			})
		})
	}
}

func TestPublicGETAndOverlappingRangeShareBlockProducers(t *testing.T) {
	data := bytes.Repeat([]byte("z"), 3<<20)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var mu sync.Mutex
	counts := map[string]int{}
	var heads atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"shared"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "HEAD" {
			heads.Add(1)
		} else {
			mu.Lock()
			counts[r.Header.Get("Range")]++
			mu.Unlock()

			if r.Header.Get("User-Agent") != "Razor" || r.Header.Get("X-Real-IP") != "" {
				t.Error("detached block lost canonical identity")
			}
		}
		out := &originBoundaryWriter{ResponseWriter: w, boundary: 1 << 20, cross: func() error {
			if start, _, ok := originInterval(r); ok && start > 1<<20 {
				return nil
			}
			close(entered)
			select {
			case <-release:
				return nil
			case <-r.Context().Done():
				return r.Context().Err()
			}
		}}
		http.ServeContent(out, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	cfg := config.GetDefaultConfig()
	cfg.SharedFillPublicStatic = 1
	s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 64<<20, origin.URL)
	endpoint := httptest.NewServer(s)
	defer func() { once.Do(func() { close(release) }); endpoint.Close() }()
	type result struct {
		body   []byte
		status int
		err    error
	}
	full, part := make(chan result, 1), make(chan result, 1)
	request := func(value, ua string, out chan result) {
		query := "b=2&a=1"
		req, _ := http.NewRequest("GET", endpoint.URL+"/item?"+query, nil)
		req.Host = "cache.test"
		req.Header.Set("Accept-Encoding", "identity")
		req.Header.Set("User-Agent", ua)
		if value != "" {
			req.Header.Set("Range", value)
		}
		resp, err := endpoint.Client().Do(req)
		if err != nil {
			out <- result{err: err}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		out <- result{body, resp.StatusCode, err}
	}
	go request("", "full-client", full)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second block bytes did not reach origin gate")
	}
	go request("bytes=1048576-3145727", "range-client", part)
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters >= 2 && st.SharedFillJoined > 0 })
	once.Do(func() { close(release) })
	for _, tc := range []struct {
		result <-chan result
		want   []byte
		status int
	}{{full, data, 200}, {part, data[1<<20:], 206}} {
		got := <-tc.result
		if got.err != nil || got.status != tc.status || !bytes.Equal(got.body, tc.want) {
			t.Fatalf("GET/Range status=%d bytes=%d err=%v", got.status, len(got.body), got.err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if heads.Load() != 0 || len(counts) != 1 || counts[""] != 1 {
		t.Fatalf("cold GET/Range did not share one ordinary GET: HEAD=%d requests=%v", heads.Load(), counts)
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.RangeReaders == 0 && st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.BufferReserved == 0
	})
}

func TestPublicGET100MiBThroughMinimumBudgetAndDiskWindow(t *testing.T) {
	for _, blockSize := range []int{1 << 20} {
		t.Run(fmt.Sprint(blockSize), func(t *testing.T) {
			const total int64 = 100 << 20
			var originBytes atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"100m"`)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				http.ServeContent(&countRangeBytes{ResponseWriter: w, bytes: &originBytes}, r, "body", time.Time{}, io.NewSectionReader(repeatedRangeSource{total}, 0, total))
			}))
			defer origin.Close()
			cfg := config.GetDefaultConfig()
			cfg.SharedFillPublicStatic = 1
			cfg.RangeBlockBytes = blockSize
			cfg.MaxCacheObjectBytes = 128 << 20
			cfg.CacheWriteBufferBytes = cfg.CacheWriterReservation()
			s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 32<<20, origin.URL)
			endpoint := httptest.NewServer(s)
			defer endpoint.Close()
			req, _ := http.NewRequest("GET", endpoint.URL+"/large", nil)
			req.Header.Set("Accept-Encoding", "identity")
			resp, err := endpoint.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			h := sha256.New()
			n, err := io.Copy(h, resp.Body)
			if err != nil || resp.StatusCode != 200 || n != total || fmt.Sprintf("%x", h.Sum(nil)) != "015bf0bc9dbc0b010bd9e62cfec4682bfaaef866653e0ce387130dbf574f76a2" {
				t.Fatalf("100MiB status=%d bytes=%d SHA=%x err=%v", resp.StatusCode, n, h.Sum(nil), err)
			}
			if originBytes.Load() != total {
				t.Fatalf("origin body bytes=%d", originBytes.Load())
			}
			waitFillStats(t, s, func(st server.Stats) bool {
				return st.RangeReaders == 0 && st.RangeReaderQueued == 0 && st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.BufferReserved == 0
			})
		})
	}
}

func TestRangeOnlyFetchesMissingAlignedUnion(t *testing.T) {
	const total int64 = 100 << 20
	var originBytes atomic.Int64
	var heads atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"union"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "HEAD" {
			heads.Add(1)
		}
		http.ServeContent(&countRangeBytes{ResponseWriter: w, bytes: &originBytes}, r, "body", time.Time{}, io.NewSectionReader(repeatedRangeSource{total}, 0, total))
	}))
	defer origin.Close()
	cfg := config.GetDefaultConfig()
	cfg.MaxCacheObjectBytes = 128 << 20
	s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 64<<20, origin.URL)
	for _, tc := range []struct {
		value  string
		want   int64
		origin int64
	}{{"bytes=0-16777215", 16 << 20, 16 << 20}, {"bytes=8388608-25165823", 16 << 20, 24 << 20}} {
		got := fetch(s, "http://cache.test/large", http.Header{"Range": {tc.value}})
		if got.Code != 206 || int64(got.Body.Len()) != tc.want || !bytes.Equal(got.Body.Bytes(), bytes.Repeat([]byte("q"), int(tc.want))) {
			t.Fatalf("union response=%d bytes=%d", got.Code, got.Body.Len())
		}
		if originBytes.Load() != tc.origin {
			t.Fatalf("union origin=%d want=%d", originBytes.Load(), tc.origin)
		}
	}
	st := s.Stats()
	if heads.Load() != 1 || st.Hits != 0 || st.Misses != 2 || st.RangeBlockHits != 8 || st.RangeBlockMisses != 24 || st.RangeHitBytes != 8<<20 || st.RangeMissBytes != 24<<20 {
		t.Fatalf("HEAD=%d stats=%+v", heads.Load(), st)
	}
}

func TestBlockLayoutRestartAndReloadIsolation(t *testing.T) {
	data := bytes.Repeat([]byte("layout"), 1<<20)
	var bodyBytes atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"layout"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(&countRangeBytes{ResponseWriter: w, bytes: &bodyBytes}, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	path := filepath.Join(t.TempDir(), "cache")
	for _, tc := range []struct {
		size  int
		total int64
	}{{1 << 20, 1 << 20}, {1 << 20, 1 << 20}} {
		cfg := config.GetDefaultConfig()
		cfg.RangeBlockBytes = tc.size
		s := blockServer(t, cfg, path, 64<<20, origin.URL)
		got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=7-19"}})
		waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
		if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data[7:20]) || bodyBytes.Load() != tc.total {
			t.Fatalf("size=%d status=%d bytes=%d origin=%d", tc.size, got.Code, got.Body.Len(), bodyBytes.Load())
		}
		candidate := *cfg
		candidate.RangeBlockBytes = 64 << 10
		if err := s.Reload(&candidate); err == nil || !strings.Contains(err.Error(), "range_block_bytes") {
			t.Fatalf("layout reload accepted: %v", err)
		}
		if err := s.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCommittedBlockStreamAbortsOnLaterOriginFailure(t *testing.T) {
	for _, full := range []bool{false, true} {
		for _, failure := range []string{"truncated", "etag", "200"} {
			t.Run(fmt.Sprintf("full=%t/%s", full, failure), func(t *testing.T) {
				data := bytes.Repeat([]byte("v"), 3<<20)
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", `"one"`)
					w.Header().Set("Cache-Control", "public,max-age=3600")
					if start, _, ok := originInterval(r); ok && start == 2<<20 {
						switch failure {
						case "etag":
							w.Header().Set("ETag", `"two"`)
						case "200":
							w.Header().Set("Content-Length", fmt.Sprint(len(data)))
							w.WriteHeader(200)
							_, _ = w.Write(data)
							return
						}
					}
					var out http.ResponseWriter = w
					if failure == "truncated" {
						out = &originBoundaryWriter{ResponseWriter: w, boundary: (1 << 20) + 3, cross: func() error { return io.ErrUnexpectedEOF }}
					}

					http.ServeContent(out, r, "body", time.Time{}, bytes.NewReader(data))
				}))
				defer origin.Close()
				cfg := config.GetDefaultConfig()
				cfg.SharedFillPublicStatic = 1
				s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 32<<20, origin.URL)
				if full && failure != "truncated" {
					warm := fetch(s, "http://cache.test/item", http.Header{"Accept-Encoding": {"identity"}, "Range": {"bytes=0-2097151"}})
					if warm.Code != 206 || !bytes.Equal(warm.Body.Bytes(), data[:2<<20]) {
						t.Fatalf("warm prefix status=%d bytes=%d", warm.Code, warm.Body.Len())
					}
				}
				endpoint := httptest.NewServer(s)
				defer endpoint.Close()
				req, _ := http.NewRequest("GET", endpoint.URL+"/item", nil)
				req.Host = "cache.test"
				req.Header.Set("Accept-Encoding", "identity")
				if !full {
					req.Header.Set("Range", "bytes=0-3145727")
				}
				resp, err := endpoint.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				wantStatus := 206
				if full {
					wantStatus = 200
				}
				minBytes, maxBytes := 2<<20, 2<<20
				if failure == "truncated" {
					minBytes, maxBytes = 1<<20, (1<<20)+3
				}
				if resp.StatusCode != wantStatus || err == nil || len(body) < minBytes || len(body) > maxBytes || !bytes.Equal(body, data[:len(body)]) {
					t.Fatalf("late failure status=%d bytes=%d err=%v", resp.StatusCode, len(body), err)
				}
				waitFillStats(t, s, func(st server.Stats) bool {
					return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.RangeReaders == 0 && st.BufferReserved == 0 && st.Origin.InFlight == 0
				})
			})
		}
	}
}

func TestCommittedBlockClientCancellationStopsPrefetch(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	data := bytes.Repeat([]byte("p"), 4<<20)
	var third atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"prefetch"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if start, end, ok := originInterval(r); ok && (start >= 2<<20 || end >= 2<<20) {
			third.Add(1)
		}
		out := &originBoundaryWriter{ResponseWriter: w, boundary: 1 << 20, cross: func() error {
			close(entered)
			<-r.Context().Done()
			close(cancelled)
			return r.Context().Err()
		}}
		http.ServeContent(out, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	cfg := config.GetDefaultConfig()
	cfg.SharedFillPublicStatic = 1
	s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 32<<20, origin.URL)
	endpoint := httptest.NewServer(s)
	defer endpoint.Close()
	req, _ := http.NewRequest("GET", endpoint.URL+"/item", nil)
	req.Header.Set("Range", "bytes=0-2097151")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := endpoint.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.CopyN(io.Discard, resp.Body, 1<<20); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("second chunk never reached source gate")
	}
	resp.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled client retained merged fill")
	}
	if third.Load() != 0 {
		t.Fatalf("fetched outside requested interval: %d", third.Load())
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.RangeReaders == 0 && st.BufferReserved == 0 && st.Origin.InFlight == 0
	})
}

func TestPublicSmallObjectSharesPartialPath(t *testing.T) {
	data := bytes.Repeat([]byte("s"), 256<<10)
	var heads, whole, ranges atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			heads.Add(1)
		} else if r.Header.Get("Range") == "" {
			whole.Add(1)
		} else {
			ranges.Add(1)
		}
		w.Header().Set("ETag", `"small"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	cfg := config.GetDefaultConfig()
	cfg.SharedFillPublicStatic = 1
	cfg.RangeBlockBytes = 1 << 20
	s := blockServer(t, cfg, filepath.Join(t.TempDir(), "cache"), 32<<20, origin.URL)
	for range 2 {
		got := fetch(s, "http://cache.test/small", http.Header{"Accept-Encoding": {"identity"}})
		if got.Code != 200 || !bytes.Equal(got.Body.Bytes(), data) {
			t.Fatalf("small status=%d bytes=%d", got.Code, got.Body.Len())
		}
	}
	part := fetch(s, "http://cache.test/small", http.Header{"Accept-Encoding": {"identity"}, "Range": {"bytes=17-99"}})
	if part.Code != 206 || !bytes.Equal(part.Body.Bytes(), data[17:100]) {
		t.Fatalf("small Range status=%d bytes=%d", part.Code, part.Body.Len())
	}
	if heads.Load() != 0 || whole.Load() != 1 || ranges.Load() != 0 {
		t.Fatalf("small classification HEAD=%d whole=%d range=%d", heads.Load(), whole.Load(), ranges.Load())
	}
}

func TestSingleBlockRequestIgnoresUnrelatedSmallStripe(t *testing.T) {
	data := bytes.Repeat([]byte("h"), 8<<20)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"heterogeneous"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	dir := t.TempDir()
	c, err := cache.NewCache(2, []string{filepath.Join(dir, "large"), filepath.Join(dir, "small")}, []int64{64 << 20, 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	cfg := config.GetDefaultConfig()
	cfg.RangeBlockBytes = 1 << 20
	cfg.UpstreamTargets = []string{origin.URL}
	p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo)
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(cfg, c, p)
	defer s.Shutdown()
	for i := 0; i < 1; i++ {
		got := fetch(s, fmt.Sprintf("http://cache.test/item-%d", i), http.Header{"Range": {"bytes=7-19"}})
		if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data[7:20]) {
			t.Fatalf("request=%d single mapped block status=%d bytes=%d stats=%+v", i, got.Code, got.Body.Len(), s.Stats())
		}
	}
}

func TestRangeRawEncodingVariantsKeepIndependentBlocks(t *testing.T) {
	data := bytes.Repeat([]byte("e"), 1<<20)
	var heads, gets atomic.Int32
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			heads.Add(1)
		} else {
			gets.Add(1)
		}
		w.Header().Set("ETag", `"encoding"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	})
	// A prior absent-AE fill must not become identity's block or manifest.
	if got := fetch(s, "http://cache.test/encoding", http.Header{"Range": {"bytes=0-100"}}); got.Code != 206 {
		t.Fatal(got.Code)
	}
	done := make(chan *httptest.ResponseRecorder, 2)
	for _, encoding := range []string{"", "identity"} {
		go func(encoding string) {
			h := http.Header{"Range": {"bytes=0-100"}}
			if encoding != "" {
				h.Set("Accept-Encoding", encoding)
			}
			done <- fetch(s, "http://cache.test/encoding", h)
		}(encoding)
	}
	for range 2 {
		got := <-done
		if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data[:101]) {
			t.Fatalf("encoding variant status=%d bytes=%d", got.Code, got.Body.Len())
		}
	}
	if heads.Load() != 2 || gets.Load() != 2 {
		t.Fatalf("raw AE variants aliased: HEAD=%d GET=%d, want2 each", heads.Load(), gets.Load())
	}
}

func TestUnknownHeadersDoNotCreateConcurrentBlockProducers(t *testing.T) {
	data := bytes.Repeat([]byte("u"), 2<<20)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var blockGets atomic.Int32
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"unknown"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Header.Get("Range") == "bytes=1048576-2097151" {
			if blockGets.Add(1) == 1 {
				close(entered)
			}
			<-release
		}
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	})
	headers := func(value, interval string) http.Header {
		return http.Header{"Range": {interval}, "Accept-Encoding": {"identity"}, "X-Unknown": {value}}
	}
	if got := fetch(s, "http://cache.test/item", headers("", "bytes=0-100")); got.Code != 206 {
		t.Fatal(got.Code)
	}
	first := make(chan *httptest.ResponseRecorder, 1)
	second := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- fetch(s, "http://cache.test/item", headers("a", "bytes=1048576-1048676")) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first block producer not entered")
	}
	go func() { second <- fetch(s, "http://cache.test/item", headers("b", "bytes=1048576-1048676")) }()
	select {
	case got := <-second:
		if got.Code < 500 || got.Code > 599 {
			t.Fatalf("conflicting task status=%d", got.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("conflicting task retained/started another origin: GET=%d", blockGets.Load())
	}
	if blockGets.Load() != 1 {
		t.Fatalf("concurrent unknown-header block producers=%d", blockGets.Load())
	}
	once.Do(func() { close(release) })
	if got := <-first; got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data[:101]) {
		t.Fatalf("owner status=%d bytes=%d", got.Code, got.Body.Len())
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
	if got := fetch(s, "http://cache.test/item", headers("b", "bytes=1048576-1048676")); got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data[:101]) || blockGets.Load() != 1 {
		t.Fatalf("committed owner block unusable: status=%d GET=%d", got.Code, blockGets.Load())
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.RangeReaders == 0 && st.BufferReserved == 0
	})
}

func TestUnknownHeadersCannotBypassManifestOwnership(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint(full), func(t *testing.T) {
			data := bytes.Repeat([]byte("m"), 2<<20)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var heads, gets atomic.Int32
			s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"manifest"`)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if r.Method == "HEAD" {
					if heads.Add(1) == 1 {
						close(entered)
					}
					<-release
				} else {
					if gets.Add(1) == 1 && full {
						close(entered)
						<-release
					}
				}
				http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
			})
			headers := func(value string) http.Header {
				h := http.Header{"Accept-Encoding": {"identity"}, "X-Unknown": {value}}
				if !full {
					h.Set("Range", "bytes=0-100")
				}
				return h
			}
			first, second := make(chan *httptest.ResponseRecorder, 1), make(chan *httptest.ResponseRecorder, 1)
			go func() { first <- fetch(s, "http://cache.test/item", headers("a")) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("first HEAD producer not entered")
			}
			go func() { second <- fetch(s, "http://cache.test/item", headers("b")) }()
			select {
			case got := <-second:
				if got.Code < 500 || got.Code > 599 {
					t.Fatalf("manifest ownership bypassed status=%d", got.Code)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("concurrent HEAD retained: HEAD=%d GET=%d", heads.Load(), gets.Load())
			}
			wantHeads, wantGets := int32(1), int32(0)
			if full {
				wantHeads, wantGets = 0, 1
			}
			if heads.Load() != wantHeads || gets.Load() != wantGets {
				t.Fatalf("ownership conflict reached origin HEAD=%d GET=%d", heads.Load(), gets.Load())
			}
			once.Do(func() { close(release) })
			got := <-first
			want := data[:101]
			status := 206
			if full {
				want = data
				status = 200
			}
			if got.Code != status || !bytes.Equal(got.Body.Bytes(), want) {
				t.Fatalf("manifest owner status=%d bytes=%d", got.Code, got.Body.Len())
			}
			waitFillStats(t, s, func(st server.Stats) bool {
				return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.RangeReaders == 0 && st.BufferReserved == 0
			})
		})
	}
}
