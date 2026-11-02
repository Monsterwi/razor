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

func TestColdGETCancellationKeepsFutureRangeAndCommittedChunks(t *testing.T) {
	data := bytes.Repeat([]byte("c"), (3<<20)+17)
	advance := make(chan struct{})
	release := sync.OnceFunc(func() { close(advance) })
	cancelled := make(chan struct{}, 1)
	var gets, heads atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"cancel"`)
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
		case <-advance:
			_, _ = w.Write(data[6<<10 : (2<<20)+(6<<10)])
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			cancelled <- struct{}{}
			return
		}
		<-r.Context().Done()
		cancelled <- struct{}{}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(release)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	leader, err := client.Get(front.URL + "/cancel")
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Body.Close()
	prefix := make([]byte, 4096)
	if _, err = io.ReadFull(leader.Body, prefix); err != nil {
		t.Fatal(err)
	}
	type reply struct {
		status int
		body   []byte
		err    error
	}
	done := make(chan reply, 1)
	go func() {
		req, _ := http.NewRequest("GET", front.URL+"/cancel", nil)
		req.Header.Set("Range", "bytes=1048593-1081343")
		resp, e := client.Do(req)
		if e != nil {
			done <- reply{err: e}
			return
		}
		defer resp.Body.Close()
		b, e := io.ReadAll(resp.Body)
		done <- reply{resp.StatusCode, b, e}
	}()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 2 })
	leader.Body.Close()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 1 })
	release()
	result := <-done
	if result.err != nil || result.status != 206 || !bytes.Equal(result.body, data[1048593:1081344]) {
		t.Fatalf("future Range after leader cancel: status=%d bytes=%d err=%v", result.status, len(result.body), result.err)
	}
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("unneeded ordinary GET did not cancel")
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 })
	for _, interval := range []string{"bytes=0-1023", "bytes=1048576-1049599"} {
		req, _ := http.NewRequest("GET", front.URL+"/cancel", nil)
		req.Header.Set("Range", interval)
		resp, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		got, e := io.ReadAll(resp.Body)
		resp.Body.Close()
		if e != nil || resp.StatusCode != 206 || !bytes.Equal(got, data[:1024]) {
			t.Fatalf("committed prefix status=%d bytes=%d err=%v", resp.StatusCode, len(got), e)
		}
	}
	if gets.Load() != 1 || heads.Load() != 0 {
		t.Fatalf("completed chunks not reused GET=%d HEAD=%d", gets.Load(), heads.Load())
	}
}

func TestColdGETUnsupportedResponseUsesItsSingleOriginResponse(t *testing.T) {
	for _, mode := range []string{"missing-etag", "weak-etag", "unknown-length", "private", "empty"} {
		t.Run(mode, func(t *testing.T) {
			data := []byte("same response body")
			if mode == "empty" {
				data = nil
			}
			var gets, heads atomic.Int32
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "HEAD" {
					heads.Add(1)
				} else {
					gets.Add(1)
				}
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("ETag", `"v1"`)
				switch mode {
				case "missing-etag":
					w.Header().Del("ETag")
				case "weak-etag":
					w.Header().Set("ETag", `W/"v1"`)
				case "private":
					w.Header().Set("Cache-Control", "private")
				}
				if mode == "unknown-length" {
					w.(http.Flusher).Flush()
				} else {
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				}
				_, _ = w.Write(data)
			})
			front := httptest.NewServer(s)
			t.Cleanup(front.Close)
			client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
			resp, e := client.Get(front.URL + "/fallback")
			if e != nil {
				t.Fatal(e)
			}
			got, e := io.ReadAll(resp.Body)
			resp.Body.Close()
			if e != nil || resp.StatusCode != 200 || !bytes.Equal(got, data) {
				t.Fatalf("status=%d bytes=%d err=%v", resp.StatusCode, len(got), e)
			}
			if gets.Load() != 1 || heads.Load() != 0 {
				t.Fatalf("fallback refetched GET=%d HEAD=%d", gets.Load(), heads.Load())
			}
		})
	}
}

func TestGETAfterNoCompletedColdChunksUsesOneOrdinaryGET(t *testing.T) {
	data := bytes.Repeat([]byte("retry"), (2<<20)/5+1)
	var mu sync.Mutex
	var requests []string
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.Header.Get("Range"))
		first := len(requests) == 1
		mu.Unlock()
		w.Header().Set("ETag", `"no-complete"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if first {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			_, _ = w.Write(data[:17])
			return
		}
		http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	first, e := client.Get(front.URL + "/no-complete")
	if e == nil {
		_, _ = io.Copy(io.Discard, first.Body)
		first.Body.Close()
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 })
	resp, e := client.Get(front.URL + "/no-complete")
	if e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(resp.Body)
	resp.Body.Close()
	if e != nil || resp.StatusCode != 200 || !bytes.Equal(got, data) {
		t.Fatalf("status=%d bytes=%d err=%v", resp.StatusCode, len(got), e)
	}
	mu.Lock()
	seen := append([]string(nil), requests...)
	mu.Unlock()
	if len(seen) != 2 || seen[0] != "GET " || seen[1] != "GET " {
		t.Fatalf("no reusable chunks should issue ordinary GET: %v", seen)
	}
}

func TestGETJoinsColdRangeDiscoveryBeforeHEADCompletes(t *testing.T) {
	data := bytes.Repeat([]byte("d"), 2<<20)
	entered, headers, body := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	releaseHeaders, releaseBody := sync.OnceFunc(func() { close(headers) }), sync.OnceFunc(func() { close(body) })
	var heads, ordinary, originBytes atomic.Int64
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"discovery"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "HEAD" {
			heads.Add(1)
			entered <- struct{}{}
			<-headers
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			return
		}
		var start, end int
		if _, e := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); e != nil {
			ordinary.Add(1)
			start, end = 0, len(data)-1
		} else {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		}
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		if r.Header.Get("Range") != "" {
			w.WriteHeader(206)
		}
		originBytes.Add(int64(end - start + 1))
		_, _ = w.Write(data[start : start+(6<<10)])
		w.(http.Flusher).Flush()
		select {
		case <-body:
			_, _ = w.Write(data[start+(6<<10) : end+1])
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(releaseBody)
	t.Cleanup(releaseHeaders)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	type reply struct {
		resp *http.Response
		err  error
	}
	replies := make(chan reply, 2)
	go func() {
		q, _ := http.NewRequest("GET", front.URL+"/discovery", nil)
		q.Header.Set("Range", "bytes=17-65535")
		r, e := client.Do(q)
		replies <- reply{r, e}
	}()
	<-entered
	go func() { r, e := client.Get(front.URL + "/discovery"); replies <- reply{r, e} }()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillJoined >= 1 })
	releaseHeaders()
	responses := make([]*http.Response, 0, 2)
	for range 2 {
		result := <-replies
		if result.err != nil {
			t.Fatal(result.err)
		}
		responses = append(responses, result.resp)
		defer result.resp.Body.Close()
	}
	for _, r := range responses {
		prefix := make([]byte, 4096)
		if _, e := io.ReadFull(r.Body, prefix); e != nil || !bytes.Equal(prefix, data[:4096]) {
			t.Fatalf("prefix status=%d err=%v", r.StatusCode, e)
		}
	}
	releaseBody()
	for _, r := range responses {
		want := len(data) - 4096
		if r.StatusCode == 206 {
			want = 65536 - 17 - 4096
		} else if r.StatusCode != 200 {
			t.Fatalf("status=%d", r.StatusCode)
		}
		got, e := io.ReadAll(r.Body)
		if e != nil || len(got) != want || !bytes.Equal(got, data[:want]) {
			t.Fatalf("status=%d bytes=%d want=%d err=%v", r.StatusCode, len(got), want, e)
		}
	}
	if heads.Load() != 1 || ordinary.Load() != 0 || originBytes.Load() != int64(len(data)) {
		t.Fatalf("discovery duplicate HEAD=%d ordinary=%d bytes=%d", heads.Load(), ordinary.Load(), originBytes.Load())
	}
}

func TestPartialGETCountsOneCacheOutcomePerResponse(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("small shared chunks")} {
		t.Run(fmt.Sprint(len(data)), func(t *testing.T) {
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"count"`)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				_, _ = w.Write(data)
			})
			for range 2 {
				got := fetch(s, "http://cache.test/count", nil)
				if got.Code != 200 || !bytes.Equal(got.Body.Bytes(), data) {
					t.Fatal("incorrect GET")
				}
			}
			st := s.Stats()
			if st.Requests != 2 || st.Hits != 1 || st.Misses != 1 {
				t.Fatalf("request outcome counted more than once: requests=%d hit=%d miss=%d", st.Requests, st.Hits, st.Misses)
			}
		})
	}
}
