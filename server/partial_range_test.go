package server_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Monsterwi/razor/server"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func partialRangeData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*17 + i/(1<<20)*31) % 251)
	}
	return data
}

func TestRangeCombinesAdjacentMissingChunksWithOriginalClientBounds(t *testing.T) {
	data := partialRangeData(3<<20 + 17)
	var mu sync.Mutex
	var ranges []string
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"merged"`)
		if r.Method == http.MethodGet {
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	defer front.Close()
	const start, end = 17, (1 << 20) + 4321
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/merged", nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data[start:end+1]) ||
		resp.ContentLength != end-start+1 || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)) {
		t.Fatalf("client bounds changed: status=%d bytes=%d headers=%v err=%v", resp.StatusCode, len(body), resp.Header, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"bytes=0-2097151"}; !reflect.DeepEqual(ranges, want) {
		t.Fatalf("origin ranges=%v want=%v", ranges, want)
	}
}

func rangeHTTP(t *testing.T, client *http.Client, target string, start, end int) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestRangeRunsReuseOutOfOrderCoverageWithoutFetchingAcrossHits(t *testing.T) {
	const chunk = 1 << 20
	data := partialRangeData(6 * chunk)
	var mu sync.Mutex
	var ranges []string
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"holes"`)
		if r.Method == http.MethodGet {
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	defer front.Close()
	for _, interval := range [][2]int{{4 * chunk, 5*chunk - 1}, {2 * chunk, 3*chunk - 1}, {0, len(data) - 1}, {chunk + 17, 5*chunk + 31}} {
		resp := rangeHTTP(t, front.Client(), front.URL+"/holes", interval[0], interval[1])
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data[interval[0]:interval[1]+1]) {
			t.Fatalf("interval=%v status=%d bytes=%d err=%v", interval, resp.StatusCode, len(body), err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"bytes=4194304-5242879", "bytes=2097152-3145727", "bytes=0-2097151", "bytes=3145728-4194303", "bytes=5242880-6291455"}
	if !reflect.DeepEqual(ranges, want) {
		t.Fatalf("hit blocks were fetched again: ranges=%v want=%v", ranges, want)
	}
}

func TestRangeRunJoinsOverlappingInFlightIntervalWithoutDuplicateBytes(t *testing.T) {
	const chunk = 1 << 20
	data := partialRangeData(5 * chunk)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var mu sync.Mutex
	var ranges []string
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"overlap"`)
		if r.Method == http.MethodGet {
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()
		}
		if r.Header.Get("Range") == "bytes=2097152-4194303" {
			w.Header().Set("Content-Range", "bytes 2097152-4194303/5242880")
			w.Header().Set("Content-Length", "2097152")
			w.WriteHeader(206)
			_, _ = w.Write(data[2*chunk : 2*chunk+(32<<10)])
			w.(http.Flusher).Flush()
			select {
			case <-release:
				_, _ = w.Write(data[2*chunk+(32<<10) : 4*chunk])
			case <-r.Context().Done():
			}
			return
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 5 * time.Second}
	first := rangeHTTP(t, client, front.URL+"/overlap", 2*chunk, 4*chunk-1)
	defer first.Body.Close()
	prefix := make([]byte, 4096)
	if _, err := io.ReadFull(first.Body, prefix); err != nil || !bytes.Equal(prefix, data[2*chunk:2*chunk+len(prefix)]) {
		t.Fatalf("creator prefix: %v", err)
	}
	second := rangeHTTP(t, client, front.URL+"/overlap", chunk, 5*chunk-1)
	defer second.Body.Close()
	beforeGate := make([]byte, chunk+4096)
	if _, err := io.ReadFull(second.Body, beforeGate); err != nil || !bytes.Equal(beforeGate, data[chunk:2*chunk+4096]) {
		t.Fatalf("cross-run RWW prefix: %v", err)
	}
	mu.Lock()
	gotRanges := append([]string(nil), ranges...)
	mu.Unlock()
	if want := []string{"bytes=2097152-4194303", "bytes=1048576-2097151"}; !reflect.DeepEqual(gotRanges, want) {
		t.Fatalf("overlapping ownership duplicated origin: %v", gotRanges)
	}
	unblock()
	for i, resp := range []*http.Response{first, second} {
		body, err := io.ReadAll(resp.Body)
		start, end := 2*chunk+4096, 4*chunk
		if i == 1 {
			end = 5 * chunk
		}
		if err != nil || !bytes.Equal(body, data[start:end]) {
			t.Fatalf("subscriber %d final bytes=%d err=%v", i, len(body), err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"bytes=2097152-4194303", "bytes=1048576-2097151", "bytes=4194304-5242879"}; !reflect.DeepEqual(ranges, want) {
		t.Fatalf("final origin ranges=%v want=%v", ranges, want)
	}
}

func TestTruncatedRangeRunKeepsCompletedChunkAndRefillsOnlyIncompleteTail(t *testing.T) {
	const chunk = 1 << 20
	data := partialRangeData(2 * chunk)
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"truncated-run"`)
		if r.Method == http.MethodGet && gets.Add(1) == 1 {
			w.Header().Set("Content-Range", "bytes 0-2097151/2097152")
			w.Header().Set("Content-Length", "2097152")
			w.WriteHeader(206)
			_, _ = w.Write(data[:chunk+17000])
			return
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	defer front.Close()
	resp := rangeHTTP(t, front.Client(), front.URL+"/truncated-run", 0, len(data)-1)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err == nil || resp.StatusCode != 206 || len(body) < chunk || len(body) >= len(data) || !bytes.Equal(body, data[:len(body)]) {
		t.Fatalf("bad run terminal: status=%d length=%d err=%v", resp.StatusCode, len(body), err)
	}
	for _, interval := range [][2]int{{0, chunk - 1}, {chunk, 2*chunk - 1}} {
		resp = rangeHTTP(t, front.Client(), front.URL+"/truncated-run", interval[0], interval[1])
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || !bytes.Equal(body, data[interval[0]:interval[1]+1]) {
			t.Fatalf("retained/refilled chunk=%v length=%d err=%v", interval, len(body), err)
		}
		want := int32(1)
		if interval[0] != 0 {
			want = 2
		}
		if gets.Load() != want {
			t.Fatalf("completed chunk lost or incomplete chunk reused: gets=%d want=%d", gets.Load(), want)
		}
	}
}

type offsetRangeSource int64

func (s offsetRangeSource) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= int64(s) {
		return 0, io.EOF
	}
	n := min(int64(len(p)), int64(s)-offset)
	for i := int64(0); i < n; i++ {
		p[i] = byte((offset + i) % 251)
	}
	if n != int64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

func TestRangeSegmentBoundaryShortTailAndMaximumLength(t *testing.T) {
	const chunk int64 = 1 << 20
	for _, tc := range []struct {
		name, value string
		total       int64
		start, end  int64
		origin      []string
	}{
		{"segment", "bytes=67108857-67108873", 65*chunk + 17, 64*chunk - 7, 64*chunk + 9, []string{"bytes=66060288-67108863", "bytes=67108864-68157439"}},
		{"suffix tail", "bytes=-17", 65*chunk + 17, 65 * chunk, 65*chunk + 16, []string{"bytes=68157440-68157456"}},
		{"open tail", "bytes=68157445-", 65*chunk + 17, 65*chunk + 5, 65*chunk + 16, []string{"bytes=68157440-68157456"}},
		{"clamped tail", "bytes=68157445-9223372036854775807", 65*chunk + 17, 65*chunk + 5, 65*chunk + 16, []string{"bytes=68157440-68157456"}},
		{"maximum total bypasses admission", "bytes=-17", math.MaxInt64, math.MaxInt64 - 17, math.MaxInt64 - 1, []string{"bytes=-17"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := offsetRangeSource(tc.total)
			var mu sync.Mutex
			var ranges []string
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("ETag", `"offsets"`)
				if r.Method == http.MethodGet {
					mu.Lock()
					ranges = append(ranges, r.Header.Get("Range"))
					mu.Unlock()
				}
				http.ServeContent(w, r, "data", time.Time{}, io.NewSectionReader(source, 0, tc.total))
			}))
			defer origin.Close()
			s, _ := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{origin.URL}, 128<<20)
			front := httptest.NewServer(s)
			defer front.Close()
			req, _ := http.NewRequest(http.MethodGet, front.URL+"/offsets", nil)
			req.Header.Set("Range", tc.value)
			resp, err := front.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			want := make([]byte, tc.end-tc.start+1)
			_, _ = source.ReadAt(want, tc.start)
			if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, want) || resp.ContentLength != int64(len(want)) || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", tc.start, tc.end, tc.total) {
				t.Fatalf("status=%d bytes=%d headers=%v err=%v", resp.StatusCode, len(body), resp.Header, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(ranges, tc.origin) {
				t.Fatalf("origin ranges=%v want=%v", ranges, tc.origin)
			}
		})
	}
}

func TestRangeSmallAndEmptyObjectsDoNotCreateNonexistentChunks(t *testing.T) {
	for _, size := range []int{0, 17} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := partialRangeData(size)
			var gets atomic.Int32
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("ETag", `"small-boundary"`)
				if r.Method == http.MethodGet {
					gets.Add(1)
				}
				http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
			})
			front := httptest.NewServer(s)
			defer front.Close()
			for _, value := range []string{"bytes=0-", "bytes=-9223372036854775807", "bytes=17-", "bytes=9223372036854775808-"} {
				req, _ := http.NewRequest(http.MethodGet, front.URL+"/small", nil)
				req.Header.Set("Range", value)
				resp, err := front.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				valid := size > 0 && (value == "bytes=0-" || value == "bytes=-9223372036854775807")
				if valid {
					if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data) {
						t.Fatalf("small range=%s status=%d body=%v err=%v", value, resp.StatusCode, body, err)
					}
				} else if err != nil || resp.StatusCode != 416 || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes */%d", size) || len(body) != 0 {
					t.Fatalf("unsatisfiable range=%s status=%d headers=%v err=%v", value, resp.StatusCode, resp.Header, err)
				}
			}
			want := int32(0)
			if size > 0 {
				want = 1
			}
			if gets.Load() != want {
				t.Fatalf("nonexistent chunks fetched: gets=%d want=%d", gets.Load(), want)
			}
		})
	}
}

func TestCancelledRangeRunFinishesCurrentChunkAndKeepsPrefix(t *testing.T) {
	const chunk = 1 << 20
	data := partialRangeData(2 * chunk)
	finished := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"cancelled-run"`)
		w.Header().Set("Content-Length", "2097152")
		if r.Method == http.MethodHead {
			return
		}
		gets.Add(1)
		w.Header().Set("Content-Range", "bytes 0-2097151/2097152")
		w.WriteHeader(206)
		_, _ = w.Write(data[:chunk+(32<<10)])
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = w.Write(data[chunk+(32<<10):])
		case <-r.Context().Done():
		}
		close(finished)
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, front.URL+"/cancelled-run", nil)
	req.Header.Set("Range", "bytes=0-2097151")
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	prefix := make([]byte, chunk+4096)
	if _, err := io.ReadFull(resp.Body, prefix); err != nil || !bytes.Equal(prefix, data[:len(prefix)]) {
		t.Fatalf("complete prefix: %v", err)
	}
	cancel()
	resp.Body.Close()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 0 && st.SharedFillTasks == 1 })
	unblock()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("current chunk did not finish after the last reader left")
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
	resp = rangeHTTP(t, front.Client(), front.URL+"/cancelled-run", 0, len(data)-1)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || !bytes.Equal(body, data) || gets.Load() != 1 {
		t.Fatalf("cancel discarded committed prefix: bytes=%d gets=%d err=%v", len(body), gets.Load(), err)
	}
}

func TestConcurrentRunsPublishDifferentBitsInSameSegment(t *testing.T) {
	const chunk = 1 << 20
	data := partialRangeData(4 * chunk)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"parallel-bits"`)
		if r.Method == http.MethodGet {
			gets.Add(1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 5 * time.Second}
	results := make(chan error, 2)
	for _, start := range []int{0, 2 * chunk} {
		go func() {
			req, _ := http.NewRequest(http.MethodGet, front.URL+"/parallel-bits", nil)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+2*chunk-1))
			resp, err := client.Do(req)
			if err == nil {
				var body []byte
				body, err = io.ReadAll(resp.Body)
				resp.Body.Close()
				if err == nil && (resp.StatusCode != 206 || !bytes.Equal(body, data[start:start+2*chunk])) {
					err = fmt.Errorf("parallel response status=%d length=%d", resp.StatusCode, len(body))
				}
			}
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("independent same-segment runs did not start")
		}
	}
	unblock()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	for index := range 4 {
		resp := rangeHTTP(t, client, front.URL+"/parallel-bits", index*chunk, (index+1)*chunk-1)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || !bytes.Equal(body, data[index*chunk:(index+1)*chunk]) {
			t.Fatalf("published bit %d lost data: %v", index, err)
		}
	}
	if gets.Load() != 2 {
		t.Fatalf("concurrent segment updates lost completed chunks: origin GET=%d", gets.Load())
	}
}

func TestCompletedRunPrefixCanRefillWhileFollowingChunkIsInFlight(t *testing.T) {
	const chunk, total = 1 << 20, 64 << 20
	gate := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(gate) })
	var original, recovery atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"rolling-run"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == http.MethodGet && r.Header.Get("Range") == "bytes=0-2097151" {
			original.Add(1)
			w = &originBoundaryWriter{ResponseWriter: w, boundary: chunk, cross: func() error {
				select {
				case <-gate:
					return nil
				case <-r.Context().Done():
					return r.Context().Err()
				}
			}}
		}
		if r.Method == http.MethodGet && r.Header.Get("Range") == "bytes=0-1048575" {
			recovery.Add(1)
		}
		http.ServeContent(w, r, "data", time.Time{}, io.NewSectionReader(offsetRangeSource(total), 0, total))
	}))
	t.Cleanup(origin.Close)
	s, _ := newRangeDiskServer(t, filepath.Join(t.TempDir(), "cache"), 32<<20, []string{origin.URL}, 128<<20)
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 10 * time.Second}
	held := rangeHTTP(t, client, front.URL+"/rolling-run", 0, 2*chunk-1)
	defer held.Body.Close()
	prefix := make([]byte, chunk)
	if _, err := io.ReadFull(held.Body, prefix); err != nil {
		t.Fatal(err)
	}
	// Roll the disk while the original run still owns its unfinished second chunk.
	fast := rangeHTTP(t, client, front.URL+"/rolling-run", 2*chunk, total-1)
	n, err := io.Copy(io.Discard, fast.Body)
	fast.Body.Close()
	if err != nil || n != total-2*chunk {
		t.Fatalf("rolling request bytes=%d err=%v", n, err)
	}
	refilled := rangeHTTP(t, client, front.URL+"/rolling-run", 0, chunk-1)
	body, err := io.ReadAll(refilled.Body)
	refilled.Body.Close()
	if err != nil || !bytes.Equal(body, prefix) || recovery.Load() != 1 || original.Load() != 1 {
		t.Fatalf("completed prefix did not refill independently: bytes=%d recovery=%d original=%d err=%v", len(body), recovery.Load(), original.Load(), err)
	}
	unblock()
	body, err = io.ReadAll(held.Body)
	expected := make([]byte, chunk)
	offsetRangeSource(total).ReadAt(expected, chunk)
	if err != nil || !bytes.Equal(body, expected) || original.Load() != 1 {
		t.Fatalf("following chunk lost its owner: bytes=%d origin=%d err=%v", len(body), original.Load(), err)
	}
}
