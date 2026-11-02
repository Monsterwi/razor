package server_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/server"
)

func TestColdGETStreamsOnceAndFeedsRangeCache(t *testing.T) {
	data := bytes.Repeat([]byte("cold-get-content"), (2<<20)/16+9)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var gets, heads atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"cold-one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "HEAD" {
			heads.Add(1)
		}
		if r.Method == "GET" {
			gets.Add(1)
		}
		if r.Header.Get("Range") != "" || r.Method == "HEAD" {
			http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		_, _ = w.Write(data[:6<<10])
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = w.Write(data[6<<10:])
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Get(front.URL + "/cold")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	prefix := make([]byte, 4096)
	if _, err = io.ReadFull(resp.Body, prefix); err != nil || !bytes.Equal(prefix, data[:len(prefix)]) {
		t.Fatalf("intra-chunk prefix err=%v", err)
	}
	if gets.Load() != 1 || heads.Load() != 0 {
		t.Fatalf("cold origin GET=%d HEAD=%d", gets.Load(), heads.Load())
	}
	unblock()
	rest, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 || resp.ContentLength != int64(len(data)) || !bytes.Equal(rest, data[len(prefix):]) {
		t.Fatalf("GET status=%d bytes=%d err=%v", resp.StatusCode, len(rest), err)
	}
	resp.Body.Close()
	for _, interval := range [][2]int{{17, 65535}, {(1 << 20) + 9, (1 << 20) + 65551}, {len(data) - 15, len(data) - 1}} {
		req, _ := http.NewRequest("GET", front.URL+"/cold", nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", interval[0], interval[1]))
		got, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(got.Body)
		got.Body.Close()
		if err != nil || got.StatusCode != 206 || !bytes.Equal(body, data[interval[0]:interval[1]+1]) {
			t.Fatalf("Range status=%d bytes=%d err=%v", got.StatusCode, len(body), err)
		}
	}
	if gets.Load() != 1 || heads.Load() != 0 {
		t.Fatalf("GET chunks not reused: GET=%d HEAD=%d", gets.Load(), heads.Load())
	}
}

func TestRangeJoinsColdGETBeforeOriginHeaders(t *testing.T) {
	data := bytes.Repeat([]byte("interleaved"), (2<<20)/11+1)
	headers, body := make(chan struct{}), make(chan struct{})
	releaseHeaders, releaseBody := sync.OnceFunc(func() { close(headers) }), sync.OnceFunc(func() { close(body) })
	entered := make(chan struct{}, 4)
	var gets, heads atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			heads.Add(1)
		} else {
			gets.Add(1)
		}
		entered <- struct{}{}
		<-headers
		w.Header().Set("ETag", `"cross-one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Header.Get("Range") != "" || r.Method == "HEAD" {
			http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		_, _ = w.Write(data[:32<<10])
		w.(http.Flusher).Flush()
		select {
		case <-body:
			_, _ = w.Write(data[32<<10:])
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(releaseBody)
	t.Cleanup(releaseHeaders)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	replies := make(chan *http.Response, 2)
	failures := make(chan error, 2)
	go func() {
		r, e := client.Get(front.URL + "/cross")
		if e != nil {
			failures <- e
			return
		}
		replies <- r
	}()
	<-entered
	go func() {
		q, _ := http.NewRequest("GET", front.URL+"/cross", nil)
		q.Header.Set("Range", "bytes=17-65535")
		r, e := client.Do(q)
		if e != nil {
			failures <- e
			return
		}
		replies <- r
	}()
	// The second request must subscribe while neither response has origin headers.
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters >= 2 })
	releaseHeaders()
	responses := make([]*http.Response, 0, 2)
	for range 2 {
		select {
		case e := <-failures:
			t.Fatal(e)
		case r := <-replies:
			responses = append(responses, r)
		}
	}
	for _, r := range responses {
		defer r.Body.Close()
		start := 0
		if r.StatusCode == 206 {
			start = 17
		} else if r.StatusCode != 200 {
			t.Fatalf("status=%d", r.StatusCode)
		}
		got := make([]byte, 8192)
		if _, e := io.ReadFull(r.Body, got); e != nil || !bytes.Equal(got, data[start:start+len(got)]) {
			t.Fatalf("prefix %v", e)
		}
	}
	if gets.Load() != 1 || heads.Load() != 0 {
		t.Fatalf("inflight duplicate GET=%d HEAD=%d", gets.Load(), heads.Load())
	}
	releaseBody()
	for _, r := range responses {
		start, end := 8192, len(data)
		if r.StatusCode == 206 {
			start, end = 17+8192, 65536
		}
		got, e := io.ReadAll(r.Body)
		if e != nil || !bytes.Equal(got, data[start:end]) {
			t.Fatalf("body status=%d length=%d err=%v", r.StatusCode, len(got), e)
		}
	}
}

func TestGETReusesInFlightRangeAndOnlyFetchesRemainingChunks(t *testing.T) {
	data := bytes.Repeat([]byte("range-first"), (3<<20)/11+4)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	var mu sync.Mutex
	var ranges []string
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"range-first"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "HEAD" {
			http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
			return
		}
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		if r.Header.Get("Range") == "bytes=1048576-2097151" {
			w.Header().Set("Content-Length", "1048576")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 1048576-2097151/%d", len(data)))
			w.WriteHeader(206)
			_, _ = w.Write(data[1<<20 : (1<<20)+(32<<10)])
			w.(http.Flusher).Flush()
			select {
			case <-gate:
				_, _ = w.Write(data[(1<<20)+(32<<10) : 2<<20])
			case <-r.Context().Done():
			}
			return
		}
		http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(release)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	request, _ := http.NewRequest("GET", front.URL+"/range-first", nil)
	request.Header.Set("Range", "bytes=1048593-1114111")
	first, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	prefix := make([]byte, 8192)
	if _, err = io.ReadFull(first.Body, prefix); err != nil || !bytes.Equal(prefix, data[(1<<20)+17:(1<<20)+17+8192]) {
		t.Fatalf("range prefix %v", err)
	}
	full, err := client.Get(front.URL + "/range-first")
	if err != nil {
		t.Fatal(err)
	}
	defer full.Body.Close()
	leading := make([]byte, (1<<20)+8192)
	if _, err = io.ReadFull(full.Body, leading); err != nil || !bytes.Equal(leading, data[:len(leading)]) {
		t.Fatalf("GET prefix %v", err)
	}
	mu.Lock()
	seen := append([]string(nil), ranges...)
	mu.Unlock()
	if len(seen) != 2 || seen[0] != "bytes=1048576-2097151" || seen[1] != "bytes=0-1048575" {
		t.Fatalf("duplicate inflight fetch: %v", seen)
	}
	release()
	rest, err := io.ReadAll(full.Body)
	if err != nil || full.StatusCode != 200 || !bytes.Equal(rest, data[len(leading):]) {
		t.Fatalf("full body status=%d len=%d err=%v", full.StatusCode, len(rest), err)
	}
	rest, err = io.ReadAll(first.Body)
	if err != nil || !bytes.Equal(rest, data[(1<<20)+17+8192:1114112]) {
		t.Fatalf("range tail len=%d err=%v", len(rest), err)
	}
	mu.Lock()
	seen = append([]string(nil), ranges...)
	mu.Unlock()
	wantLast := fmt.Sprintf("bytes=2097152-%d", len(data)-1)
	if len(seen) != 3 || seen[2] != wantLast {
		t.Fatalf("missing ranges=%v", seen)
	}
	again, err := client.Get(front.URL + "/range-first")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(again.Body)
	again.Body.Close()
	mu.Lock()
	count := len(ranges)
	mu.Unlock()
	if err != nil || !bytes.Equal(got, data) || count != 3 {
		t.Fatalf("hot GET bytes=%d calls=%d err=%v", len(got), count, err)
	}
}

func TestCompletedNewRunCountsAsMissAfterSlowResponse(t *testing.T) {
	data := bytes.Repeat([]byte("m"), 2<<20)
	entered, resume := make(chan struct{}, 1), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"metrics"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(&blockSendBarrier{ResponseWriter: w, entered: entered, resume: resume}, r)
	}))
	t.Cleanup(front.Close)
	t.Cleanup(release)
	client := &http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequest("GET", front.URL+"/metrics", nil)
	req.Header.Set("Range", "bytes=0-2097151")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	<-entered
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 })
	release()
	got, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("response bytes=%d err=%v", len(got), err)
	}
	st := s.Stats()
	if st.RangeBlockHits != 0 || st.RangeBlockMisses != 2 {
		t.Fatalf("completed new run counted as hit: hits=%d misses=%d", st.RangeBlockHits, st.RangeBlockMisses)
	}
}
