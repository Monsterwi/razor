package server_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
	"github.com/Monsterwi/razor/server"
	"io"
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

func TestRangeOverlapsReuseCommittedBlocks(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), (3<<20)/16)
	var originBytes atomic.Int64
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"one"`)
		rec := &countRangeBytes{ResponseWriter: w, bytes: &originBytes}
		http.ServeContent(rec, r, "body", time.Time{}, bytes.NewReader(data))
	})
	for _, spec := range []struct {
		header     string
		start, end int
		query      string
	}{{"bytes=0-1572863", 0, 1572864, "b=2&a=1"}, {"bytes=1048576-2097151", 1048576, 2097152, "b=2&a=1"}} {
		got := fetch(s, "http://cache.test/item?"+spec.query, http.Header{"Range": {spec.header}})
		if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data[spec.start:spec.end]) {
			t.Fatalf("range %s status=%d length=%d", spec.header, got.Code, got.Body.Len())
		}
	}
	if n := originBytes.Load(); n != 2<<20 {
		t.Fatalf("actual origin bytes=%d, want 2097152 (two reused aligned blocks)", n)
	}
}

type countRangeBytes struct {
	http.ResponseWriter
	bytes *atomic.Int64
}

func (w *countRangeBytes) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes.Add(int64(n))
	return n, err
}

func TestRangeVersionChangeRejectsMixedBodyAndRecovers(t *testing.T) {
	old := bytes.Repeat([]byte("a"), 3<<20)
	newer := bytes.Repeat([]byte("b"), 3<<20)
	var version atomic.Int32
	version.Store(1)
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && gets.Add(1) == 2 {
			version.Store(2)
		}
		data := old
		tag := `"one"`
		if version.Load() == 2 {
			data = newer
			tag = `"two"`
		}
		w.Header().Set("ETag", tag)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	defer front.Close()
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/item", nil)
	req.Host = "cache.test"
	req.Header.Set("Range", "bytes=0-3145727")
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 206 || resp.Header.Get("ETag") != `"one"` || err == nil || len(body) != 2<<20 || !bytes.Equal(body, old[:2<<20]) {
		t.Fatalf("version conflict must abort after only old bytes: status=%d bytes=%d error=%v", resp.StatusCode, len(body), err)
	}
	if gets.Load() != 2 {
		t.Fatalf("version change retried current request: GET=%d", gets.Load())
	}
	got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-3145727"}})
	if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), newer) {
		t.Fatalf("new version recovery status=%d length=%d", got.Code, got.Body.Len())
	}
}

func TestRangeBoundariesAndIfRange(t *testing.T) {
	data := []byte("0123456789abcdef")
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	})
	for _, tc := range []struct {
		value, ifRange, body, contentRange string
		status                             int
	}{
		{"bytes=2-5", "", "2345", "bytes 2-5/16", 206},
		{"bytes=10-", "", "abcdef", "bytes 10-15/16", 206},
		{"bytes=-3", "", "def", "bytes 13-15/16", 206},
		{"bytes=12-100", "", "cdef", "bytes 12-15/16", 206},
		{"bytes=-100", "", "0123456789abcdef", "bytes 0-15/16", 206},
		{"bytes=99-", "", "", "bytes */16", 416},
		{"bytes=1-2", `"one"`, "12", "bytes 1-2/16", 206},
		{"bytes=1-2", `"other"`, string(data), "", 200},
		{"bytes=1-2", `W/"one"`, string(data), "", 200},
	} {
		t.Run(tc.value+tc.ifRange, func(t *testing.T) {
			headers := http.Header{"Range": {tc.value}}
			if tc.ifRange != "" {
				headers.Set("If-Range", tc.ifRange)
			}
			got := fetch(s, "http://cache.test/item", headers)
			if got.Code != tc.status || got.Body.String() != tc.body || got.Header().Get("Content-Range") != tc.contentRange {
				t.Fatalf("got=%d %q Content-Range=%q", got.Code, got.Body.String(), got.Header().Get("Content-Range"))
			}
		})
	}
}

func TestRangeUnsupportedRepresentationBypasses(t *testing.T) {
	for _, tc := range []struct {
		name, etag, encoding, rangeValue string
		ignore                           bool
	}{
		{name: "weak", etag: `W/"one"`}, {name: "missing"}, {name: "encoding", etag: `"one"`, encoding: "gzip"}, {name: "multiple", etag: `"one"`, rangeValue: "bytes=0-1,4-5"}, {name: "ignored", etag: `"one"`, ignore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gets atomic.Int32
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					gets.Add(1)
				}
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if tc.etag != "" {
					w.Header().Set("ETag", tc.etag)
				}
				if tc.encoding != "" {
					w.Header().Set("Content-Encoding", tc.encoding)
				}
				if tc.ignore {
					w.Header().Set("Content-Length", "6")
					if r.Method != "HEAD" {
						fmt.Fprint(w, "abcdef")
					}
					return
				}
				http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader([]byte("abcdef")))
			})
			value := tc.rangeValue
			if value == "" {
				value = "bytes=1-2"
			}
			for i := 0; i < 2; i++ {
				got := fetch(s, "http://cache.test/item", http.Header{"Range": {value}})
				if got.Code != 206 && !(tc.ignore && got.Code == 200) {
					t.Fatalf("status=%d", got.Code)
				}
				if tc.rangeValue == "" && !tc.ignore && got.Body.String() != "bc" {
					t.Fatalf("body=%q", got.Body.String())
				}
			}
			if gets.Load() < 2 {
				t.Fatal("unsupported response was cached")
			}
		})
	}
}

func TestRangeCreatorCancellationKeepsBlockSubscriber(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	data := bytes.Repeat([]byte("x"), 1<<20)
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "GET" {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startFill(s, ctx, http.Header{"Range": {"bytes=0-100"}})
	<-entered
	second := startFill(s, context.Background(), http.Header{"Range": {"bytes=100-200"}})
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	cancel()
	<-first
	once.Do(func() { close(release) })
	got := <-second
	if got.Code != 206 || got.Body.String() != strings.Repeat("x", 101) {
		t.Fatalf("remaining subscriber=%d %q", got.Code, got.Body.String())
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
	if calls.Load() != 2 {
		t.Fatalf("HEAD+shared GET requests=%d", calls.Load())
	}
}

func TestRangeLastSubscriberCancellationStopsOrigin(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Content-Length", "1048576")
		if r.Method == "GET" {
			close(entered)
			<-r.Context().Done()
			close(cancelled)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startFill(s, ctx, http.Header{"Range": {"bytes=0-100"}})
	<-entered
	cancel()
	<-first
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("last Range subscriber retained origin")
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
}

func TestRangePurgeFencesAllBlocksAndKeepsAdjacentURL(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var blocked atomic.Bool
	data := bytes.Repeat([]byte("a"), 2<<20)
	var adjacentBytes atomic.Int64
	var firstBlockGets atomic.Int32
	s, _ := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if start, _, ok := originInterval(r); r.URL.Path == "/item" && ok && start == 0 {
			firstBlockGets.Add(1)
		}
		var out http.ResponseWriter = w
		if r.URL.Path == "/item" {
			out = &originBoundaryWriter{ResponseWriter: w, boundary: 1 << 20, cross: func() error {
				if blocked.CompareAndSwap(false, true) {
					close(entered)
					select {
					case <-release:
					case <-r.Context().Done():
						return r.Context().Err()
					}
				}
				return nil
			}}
		}
		if r.URL.Path == "/item-adjacent" {
			out = &countRangeBytes{ResponseWriter: w, bytes: &adjacentBytes}
		}
		http.ServeContent(out, r, "body", time.Time{}, bytes.NewReader(data))
	})
	got := fetch(s, "http://cache.test/item-adjacent", http.Header{"Range": {"bytes=0-100"}})
	if got.Code != 206 {
		t.Fatal(got.Code)
	}
	endpoint := httptest.NewServer(s)
	defer func() { once.Do(func() { close(release) }); endpoint.Close() }()
	first := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		req, _ := http.NewRequest("GET", endpoint.URL+"/item?b=2&a=1", nil)
		req.Host = "cache.test"
		req.Header.Set("Range", "bytes=0-2097151")
		resp, err := endpoint.Client().Do(req)
		if err != nil {
			first <- err
			return
		}
		defer resp.Body.Close()
		prefix := make([]byte, 1<<20)
		if _, err := io.ReadFull(resp.Body, prefix); err != nil || resp.StatusCode != 206 || !bytes.Equal(prefix, data[:len(prefix)]) {
			first <- fmt.Errorf("pre-PURGE prefix: status=%d err=%v", resp.StatusCode, err)
			return
		}
		close(started)
		body, err := io.ReadAll(resp.Body)
		if err != nil || !bytes.Equal(body, data[len(prefix):]) {
			err = fmt.Errorf("started pre-PURGE response did not complete: status=%d bytes=%d err=%v", resp.StatusCode, len(prefix)+len(body), err)
		}
		first <- err
	}()
	<-entered
	// The origin gate alone does not prove the downstream response started.
	select {
	case <-started:
	case err := <-first:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("pre-PURGE response prefix did not arrive")
	}
	if got = purge(s, "http://cache.test/item?b=2&a=1", "test-secret"); got.Code != 200 {
		t.Fatal(got.Code)
	}
	once.Do(func() { close(release) })
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if got = fetch(s, "http://cache.test/item?b=2&a=1", http.Header{"Range": {"bytes=0-2097151"}}); got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data) || firstBlockGets.Load() != 2 {
		t.Fatalf("post purge=%d bytes=%d first-block-fetches=%d", got.Code, got.Body.Len(), firstBlockGets.Load())
	}
	if got = fetch(s, "http://cache.test/item-adjacent", http.Header{"Range": {"bytes=0-100"}}); got.Code != 206 || adjacentBytes.Load() != 1<<20 {
		t.Fatalf("adjacent was evicted: status=%d originbytes=%d", got.Code, adjacentBytes.Load())
	}
}

func newRangeDiskServer(t *testing.T, path string, size int64, targets []string, maxObject int) (*server.Server, *cache.Cache) {
	t.Helper()
	c, err := cache.NewCache(1, []string{path}, []int64{size})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	cfg := config.GetDefaultConfig()
	cfg.UpstreamTargets = targets
	cfg.MaxCacheObjectBytes = maxObject
	cfg.UpstreamAlgo = "round-robin"
	p, err := proxy.NewHTTPProxy(targets, cfg.UpstreamAlgo)
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(cfg, c, p)
	t.Cleanup(func() { s.Shutdown() })
	return s, c
}

func TestRangeCleanRestartReusesCompleteBlocksAndRefetchesInterruptedBlock(t *testing.T) {
	data := bytes.Repeat([]byte("r"), 2<<20)
	var broken atomic.Bool
	broken.Store(true)
	var originBytes atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		var out http.ResponseWriter = &countRangeBytes{ResponseWriter: w, bytes: &originBytes}
		if broken.Load() {
			out = &originBoundaryWriter{ResponseWriter: out, boundary: (1 << 20) + 3, cross: func() error { return io.ErrUnexpectedEOF }}
		}
		http.ServeContent(out, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer upstream.Close()
	path := filepath.Join(t.TempDir(), "cache")
	s, _ := newRangeDiskServer(t, path, 32<<20, []string{upstream.URL}, 64<<20)
	assertIncompleteRange(t, s, "bytes=0-2097151", 2<<20)
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	broken.Store(false)
	next, _ := newRangeDiskServer(t, path, 32<<20, []string{upstream.URL}, 64<<20)
	got := fetch(next, "http://cache.test/item", http.Header{"Range": {"bytes=0-2097151"}})
	if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data) || originBytes.Load() != (2<<20)+3 {
		t.Fatalf("restart status=%d length=%d total origin=%d", got.Code, got.Body.Len(), originBytes.Load())
	}
}

func TestRangeResponseLargerThanDiskStreamsThroughWindow(t *testing.T) {
	data := bytes.Repeat([]byte("window"), (40<<20)/6+1)
	data = data[:40<<20]
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"window"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer upstream.Close()
	s, _ := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{upstream.URL}, 64<<20)
	endpoint := httptest.NewServer(s)
	defer endpoint.Close()
	req, _ := http.NewRequest("GET", endpoint.URL+"/item", nil)
	req.Header.Set("Range", "bytes=0-41943039")
	resp, err := endpoint.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data) {
		t.Fatalf("window response status=%d bytes=%d err=%v", resp.StatusCode, len(body), err)
	}
}

func TestRangePinsFullOriginIdentityWithEqualETags(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := byte('a')
		if strings.HasPrefix(r.URL.Path, "/b/") {
			value = 'b'
		}
		if (value == 'a' && r.URL.Query().Get("replica") != "a") || (value == 'b' && r.URL.Query().Get("replica") != "b") {
			t.Error("configured query lost")
		}
		w.Header().Set("ETag", `"same-on-both"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(bytes.Repeat([]byte{value}, 2<<20)))
	}))
	defer upstream.Close()
	s, _ := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{upstream.URL + "/a?replica=a", upstream.URL + "/b?replica=b"}, 64<<20)
	for i := 0; i < 4; i++ {
		got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-2097151"}})
		if got.Code != 206 || got.Body.Len() != 2<<20 {
			t.Fatalf("status=%d bytes=%d", got.Code, got.Body.Len())
		}
		value := got.Body.Bytes()[0]
		if value != 'a' && value != 'b' {
			t.Fatal(value)
		}
		if !bytes.Equal(got.Body.Bytes(), bytes.Repeat([]byte{value}, 2<<20)) {
			t.Fatal("equal ETag caused cross-origin assembly")
		}
	}
}

func TestRangeCorruptDiskBlockIsMissAfterRestart(t *testing.T) {
	data := bytes.Repeat([]byte("range-corruption-payload-"), 50000)
	var originBytes atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(&countRangeBytes{ResponseWriter: w, bytes: &originBytes}, r, "body", time.Time{}, bytes.NewReader(data))
	}))
	defer upstream.Close()
	path := filepath.Join(t.TempDir(), "cache")
	s, _ := newRangeDiskServer(t, path, 32<<20, []string{upstream.URL}, 64<<20)
	got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-1048575"}})
	if got.Code != 206 {
		t.Fatal(got.Code)
	}
	if err := s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Inject an actual disk fault by finding known origin bytes, without relying on storage offsets.
	buf := make([]byte, 64<<10)
	offset := int64(0)
	found := false
	for {
		n, readErr := file.ReadAt(buf, offset)
		if n > 0 {
			if i := bytes.Index(buf[:n], []byte("range-corruption-payload-")); i >= 0 {
				if _, err = file.WriteAt([]byte("X"), offset+int64(i)); err != nil {
					t.Fatal(err)
				}
				found = true
				break
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				t.Fatal(readErr)
			}
			break
		}
		offset += int64(n)
	}
	file.Close()
	if !found {
		t.Fatal("cached body not found in task fixture")
	}
	next, _ := newRangeDiskServer(t, path, 32<<20, []string{upstream.URL}, 64<<20)
	got = fetch(next, "http://cache.test/item", http.Header{"Range": {"bytes=0-1048575"}})
	if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data[:1<<20]) || originBytes.Load() != 2<<20 {
		t.Fatalf("corrupt recovery status=%d bytes=%d origin=%d", got.Code, got.Body.Len(), originBytes.Load())
	}
}

type repeatedRangeSource struct{ size int64 }

func (source repeatedRangeSource) ReadAt(p []byte, off int64) (int, error) {
	if off >= source.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), source.size-off))
	for i := 0; i < n; i++ {
		p[i] = 'q'
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestRangeMoreThan64BlocksThroughRealHTTP(t *testing.T) {
	const size int64 = 65*(1<<20) + 17
	var originBytes atomic.Int64
	source := repeatedRangeSource{size: size}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"large"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(&countRangeBytes{ResponseWriter: w, bytes: &originBytes}, r, "body", time.Time{}, io.NewSectionReader(source, 0, size))
	}))
	defer upstream.Close()
	s, _ := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 160<<20, []string{upstream.URL}, 128<<20)
	frontend := httptest.NewServer(s)
	defer frontend.Close()
	get := func(value string) *http.Response {
		req, _ := http.NewRequest("GET", frontend.URL+"/large", nil)
		req.Header.Set("Range", value)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	resp := get("bytes=0-")
	if resp.StatusCode != 206 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	buf := make([]byte, 32<<10)
	var total int64
	for {
		n, err := resp.Body.Read(buf)
		total += int64(n)
		for _, v := range buf[:n] {
			if v != 'q' {
				t.Fatal("incorrect streamed byte")
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	resp.Body.Close()
	if total != size || originBytes.Load() != size {
		t.Fatalf("stream=%d origin=%d want=%d", total, originBytes.Load(), size)
	}
	resp = get("bytes=-17")
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 206 || string(body) != strings.Repeat("q", 17) || originBytes.Load() != size {
		t.Fatalf("last block=%d %q err=%v origin=%d", resp.StatusCode, body, err, originBytes.Load())
	}
}

func TestRangeRejectsInvalidBlockMetadataAndRevalidates(t *testing.T) {
	for _, kind := range []string{"412", "416", "etag", "total", "content-range", "length", "encoding", "private"} {
		t.Run(kind, func(t *testing.T) {
			var invalid atomic.Bool
			invalid.Store(true)
			data := bytes.Repeat([]byte("v"), 1<<20)
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"one"`)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if r.Method == "GET" && invalid.CompareAndSwap(true, false) {
					switch kind {
					case "412":
						w.WriteHeader(412)
						return
					case "416":
						w.Header().Set("Content-Range", "bytes */1048576")
						w.WriteHeader(416)
						return
					case "etag":
						w.Header().Set("ETag", `"another"`)
					case "encoding":
						w.Header().Set("Content-Encoding", "gzip")
					case "private":
						w.Header().Set("Cache-Control", "private")
					}
					contentRange := "bytes 0-1048575/1048576"
					if kind == "total" {
						contentRange = "bytes 0-1048575/2097152"
					}
					if kind == "content-range" {
						contentRange = "bytes 1-1048576/1048576"
					}
					w.Header().Set("Content-Range", contentRange)
					length := len(data)
					if kind == "length" {
						length--
					}
					w.Header().Set("Content-Length", fmt.Sprint(length))
					w.WriteHeader(206)
					w.Write(data[:length])
					return
				}
				http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
			})
			got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-1048575"}})
			if kind == "private" {
				if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data) {
					t.Fatalf("private fallback=%d", got.Code)
				}
			} else if got.Code != 502 {
				t.Fatalf("invalid metadata completed: %d bytes=%d", got.Code, got.Body.Len())
			}
			got = fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-1048575"}})
			if got.Code != 206 || !bytes.Equal(got.Body.Bytes(), data) {
				t.Fatalf("recovered=%d bytes=%d", got.Code, got.Body.Len())
			}
		})
	}
}

func TestRangeReloadKeepsPinnedOldProducerAndSeparatesNewRuntime(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	oldData := bytes.Repeat([]byte("a"), 1<<20)
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"same"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "GET" {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(oldData))
	}))
	defer old.Close()
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"same"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(bytes.Repeat([]byte("b"), 1<<20)))
	}))
	defer next.Close()
	s, _ := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{old.URL}, 64<<20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startFill(s, ctx, http.Header{"Range": {"bytes=0-100"}})
	<-entered
	follower := startFill(s, context.Background(), http.Header{"Range": {"bytes=100-200"}})
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	cfg := config.GetDefaultConfig()
	cfg.UpstreamTargets = []string{next.URL}
	cfg.UpstreamAlgo = "round-robin"
	if err := s.Reload(cfg); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-first
	got := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=100-200"}})
	if got.Code != 206 || got.Body.String() != strings.Repeat("b", 101) {
		t.Fatalf("new runtime=%d %q", got.Code, got.Body.String())
	}
	once.Do(func() { close(release) })
	got = <-follower
	if got.Code != 206 || got.Body.String() != strings.Repeat("a", 101) {
		t.Fatalf("old runtime=%d %q", got.Code, got.Body.String())
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.Origin.InFlight == 0
	})
}

func TestRangeOriginBufferHoldsWriterBudget(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "GET" {
			close(entered)
			<-release
		}
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(bytes.Repeat([]byte("x"), 1<<20)))
	})
	first := startFill(s, context.Background(), http.Header{"Range": {"bytes=0-100"}})
	<-entered
	if s.Stats().CacheWriters != 1 {
		t.Errorf("block origin holds %d writer reservations, want 1 before buffering", s.Stats().CacheWriters)
	}
	once.Do(func() { close(release) })
	if got := <-first; got.Code != 206 {
		t.Fatal(got.Code)
	}
}

func TestRangeHonorsShorterBlockFreshnessThanManifest(t *testing.T) {
	now := time.Now().UTC()
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Date", now.Format(http.TimeFormat))
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "GET" {
			gets.Add(1)
			w.Header().Set("Cache-Control", "public,max-age=1")
		}
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader([]byte("0123456789")))
	})
	server.WithClock(func() time.Time { return now })(s)
	first := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-2"}})
	if first.Code != 206 || first.Body.String() != "012" {
		t.Fatal(first.Code)
	}
	if first.Header().Get("Cache-Control") != "no-cache" {
		t.Errorf("assembled response advertises unchecked freshness: %q", first.Header().Get("Cache-Control"))
	}
	now = now.Add(2 * time.Second)
	second := fetch(s, "http://cache.test/item", http.Header{"Range": {"bytes=0-2"}})
	if second.Code != 206 || second.Body.String() != "012" || gets.Load() != 2 {
		t.Fatalf("short-lived block status=%d body=%q GETs=%d", second.Code, second.Body.String(), gets.Load())
	}
}
