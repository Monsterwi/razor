package server_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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

func TestRangeRepairsMissingCoverageEvenWhenRefillFails(t *testing.T) {
	const chunk = 1 << 20
	data := partialRangeData(3 * chunk)
	var broken atomic.Bool
	var mu sync.Mutex
	var ranges []string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"repair"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == http.MethodGet {
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()
			if broken.Swap(false) {
				w.Header().Set("Content-Length", "1048576")
				w.Header().Set("Content-Range", "bytes 1048576-2097151/3145728")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data[chunk : chunk+10000])
				return
			}
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache")}, []int64{32 << 20})
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
	s := server.New(cfg, c, p)
	t.Cleanup(func() { _ = s.Shutdown() })
	front := httptest.NewServer(s)
	defer front.Close()
	request := func() (*http.Response, error) {
		req, _ := http.NewRequest(http.MethodGet, front.URL+"/repair", nil)
		req.Host = "cache.test"
		req.Header.Set("Accept-Encoding", "identity")
		req.Header.Set("Range", "bytes=0-3145727")
		return front.Client().Do(req)
	}
	check := func() {
		t.Helper()
		resp, err := request()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data) || resp.Header.Get("Content-Range") != "bytes 0-3145727/3145728" {
			t.Fatalf("range status=%d bytes=%d err=%v", resp.StatusCode, len(body), err)
		}
	}
	check()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 })
	identity := httptest.NewRequest(http.MethodGet, "http://cache.test/repair", nil)
	root := []byte(p.CacheNamespace() + ":" + p.CacheKey(identity))
	const variant = `{"Encoding":["identity"],"Language":null}`
	m, _, err := c.ReadPartialObject(cache.PartialObjectKey(root, origin.URL, variant), origin.URL, variant)
	if err != nil {
		t.Fatal(err)
	}
	key := m.ChunkKey(root, 1)
	stripe := c.KeyToStripe(key)
	if _, err = stripe.DirMgr.Delete(key); err != nil {
		t.Fatal(err)
	}
	stripe.RamCache.Delete(key)
	broken.Store(true)
	resp, err := request()
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err == nil || len(body) >= len(data) || !bytes.Equal(body, data[:len(body)]) {
		t.Fatalf("failed refill accepted or corrupted bytes: length=%d err=%v", len(body), err)
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 })
	coverage, err := c.PartialCoverage(root, m, 0)
	if err != nil || coverage.Bits != 5 {
		t.Fatalf("failed read left unavailable chunk advertised: bits=%b err=%v", coverage.Bits, err)
	}
	check()
	check()
	mu.Lock()
	defer mu.Unlock()
	want := []string{"bytes=0-2097151", "bytes=2097152-3145727", "bytes=1048576-2097151", "bytes=1048576-2097151"}
	if !reflect.DeepEqual(ranges, want) {
		t.Fatalf("refill fetched valid chunks or duplicated work: got=%v want=%v", ranges, want)
	}
}

func TestRangeCorruptChunkRefillsWithoutFetchingOtherHits(t *testing.T) {
	const chunk = 1 << 20
	data := make([]byte, 3*chunk)
	copy(data, bytes.Repeat([]byte("a"), chunk))
	marker := []byte("middle-chunk-corruption-marker-")
	copy(data[chunk:], bytes.Repeat(marker, chunk/len(marker)+1))
	copy(data[2*chunk:], bytes.Repeat([]byte("z"), chunk))
	var mu sync.Mutex
	var ranges []string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"crc-repair"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == http.MethodGet {
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	s, c := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{origin.URL}, 64<<20)
	front := httptest.NewServer(s)
	defer front.Close()
	check := func() {
		t.Helper()
		resp := rangeHTTP(t, front.Client(), front.URL+"/crc-repair", 0, len(data)-1)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data) {
			t.Fatalf("corrupt content escaped: status=%d length=%d err=%v", resp.StatusCode, len(body), err)
		}
	}
	check()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 })
	corruptPartialMarker(t, c, marker)
	check()
	check()
	mu.Lock()
	defer mu.Unlock()
	want := []string{"bytes=0-2097151", "bytes=2097152-3145727", "bytes=1048576-2097151"}
	if !reflect.DeepEqual(ranges, want) {
		t.Fatalf("corrupt chunk did not become an isolated miss: ranges=%v want=%v", ranges, want)
	}
}

func TestRangeMissingSegmentUsesBoundedMissWithoutReconstructingChunks(t *testing.T) {
	data := partialRangeData(2 << 20)
	var gets atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"missing-segment"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	s, c := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{origin.URL}, 64<<20)
	front := httptest.NewServer(s)
	defer front.Close()
	check := func() {
		t.Helper()
		resp := rangeHTTP(t, front.Client(), front.URL+"/missing-segment", 0, len(data)-1)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data) {
			t.Fatalf("missing segment range: status=%d bytes=%d err=%v", resp.StatusCode, len(body), err)
		}
	}
	check()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 })
	stripe := c.Stripes[0]
	keys, err := stripe.DirMgr.DirTrie.KeysForPrefixLimit(nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	removed := false
	for _, key := range keys {
		if !strings.Contains(key, "\nrange:segment:") {
			continue
		}
		if _, err = stripe.DirMgr.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
		stripe.RamCache.Delete([]byte(key))
		removed = true
	}
	if !removed {
		t.Fatal("no segment metadata to evict")
	}
	check()
	check()
	if gets.Load() != 2 {
		t.Fatalf("missing segment unexpectedly scanned chunks or failed to retain refill: origin GET=%d", gets.Load())
	}
}

func corruptPartialMarker(t *testing.T, c *cache.Cache, marker []byte) {
	t.Helper()
	stripe := c.Stripes[0]
	buf := make([]byte, 64<<10)
	found := false
	for offset := int64(0); ; offset += int64(len(buf)) {
		n, err := stripe.Fd.ReadAt(buf, offset)
		if i := bytes.Index(buf[:n], marker); i >= 0 {
			if _, err = stripe.Fd.WriteAt([]byte("X"), offset+int64(i)); err != nil {
				t.Fatal(err)
			}
			found = true
			break
		}
		if err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
			break
		}
	}
	if !found {
		t.Fatal("cached middle chunk was not present in disk fixture")
	}
	keys, err := stripe.DirMgr.DirTrie.KeysForPrefixLimit(nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		stripe.RamCache.Delete([]byte(key))
	}
}

func TestRangeCompletedSlotCorruptionRefillsAfterResponseStarted(t *testing.T) {
	const chunk = 1 << 20
	marker := []byte("completed-slot-corruption-marker-")
	data := append(bytes.Repeat([]byte("a"), chunk), bytes.Repeat(marker, chunk/len(marker)+1)...)
	data = data[:2*chunk]
	var mu sync.Mutex
	var ranges []string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"completed-crc"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == http.MethodGet {
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(origin.Close)
	s, c := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{origin.URL}, 64<<20)
	entered, resume := make(chan struct{}, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(&blockSendBarrier{ResponseWriter: w, entered: entered, resume: resume}, r)
	}))
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	result := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, front.URL+"/completed-crc", nil)
		req.Header.Set("Range", "bytes=0-2097151")
		resp, err := front.Client().Do(req)
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil && (resp.StatusCode != 206 || !bytes.Equal(body, data)) {
				err = fmt.Errorf("completed stream changed: status=%d bytes=%d", resp.StatusCode, len(body))
			}
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first chunk did not reach client write gate")
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
	corruptPartialMarker(t, c, marker)
	unblock()
	if err := <-result; err != nil {
		t.Fatalf("available same-version chunk was not recovered: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"bytes=0-2097151", "bytes=1048576-2097151"}
	if !reflect.DeepEqual(ranges, want) {
		t.Fatalf("completed-slot refill duplicated valid content: ranges=%v want=%v", ranges, want)
	}
}
