package server_test

import (
	"bytes"
	"github.com/Monsterwi/razor/server"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type versionWriteBarrier struct {
	http.ResponseWriter
	once            sync.Once
	entered, resume chan struct{}
}

func (w *versionWriteBarrier) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *versionWriteBarrier) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.resume })
	return w.ResponseWriter.Write(p)
}

func TestKnownObjectReplacementKeepsOldHTTPStream(t *testing.T) {
	objectReplacementStream(t, true)
}
func TestUnchangedObjectRefreshKeepsOldHTTPStream(t *testing.T) {
	objectReplacementStream(t, false)
}
func objectReplacementStream(t *testing.T, changedVersion bool) {
	old, newer := bytes.Repeat([]byte("a"), 2<<20), bytes.Repeat([]byte("b"), 2<<20)
	var version atomic.Bool
	var gets atomic.Int32
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		body, tag := old, `"v1"`
		if version.Load() {
			body, tag = newer, `"v2"`
		}
		w.Header().Set("ETag", tag)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(body))
	})
	headers := http.Header{"Range": {"bytes=0-2097151"}, "Accept-Encoding": {"identity"}}
	warm := fetch(s, "http://cache.test/item", headers)
	if warm.Code != 206 || !bytes.Equal(warm.Body.Bytes(), old) {
		t.Fatal("warm failed")
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
	entered, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Gate") == "1" {
			w = &versionWriteBarrier{ResponseWriter: w, entered: entered, resume: resume}
		}
		s.ServeHTTP(w, r)
	}))
	defer func() { release.Do(func() { close(resume) }); endpoint.Close() }()
	type result struct {
		status int
		etag   string
		body   []byte
		err    error
	}
	done := make(chan result, 1)
	go func() {
		req, _ := http.NewRequest("GET", endpoint.URL+"/item", nil)
		req.Host = "cache.test"
		req.Header = headers.Clone()
		req.Header.Set("X-Test-Gate", "1")
		resp, err := endpoint.Client().Do(req)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		done <- result{resp.StatusCode, resp.Header.Get("ETag"), body, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old first block did not reach send barrier")
	}
	version.Store(changedVersion)
	revalidate := headers.Clone()
	revalidate.Set("Cache-Control", "no-cache")
	changed := fetch(s, "http://cache.test/item", revalidate)
	expected := old
	if changedVersion {
		expected = newer
	}
	if changed.Code != 206 || !bytes.Equal(changed.Body.Bytes(), expected) {
		t.Fatalf("replacement status=%d bytes=%d", changed.Code, changed.Body.Len())
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
	beforeResume := gets.Load()
	release.Do(func() { close(resume) })
	select {
	case got := <-done:
		if got.err != nil || got.status != 206 || got.etag != `"v1"` || !bytes.Equal(got.body, old) {
			t.Fatalf("metadata refresh interrupted obtainable old bytes: status=%d bytes=%d error=%v", got.status, len(got.body), got.err)
		}
		if gets.Load() != beforeResume {
			t.Fatalf("old cached stream retried origin: GETs=%d before=%d", gets.Load(), beforeResume)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old stream did not terminate")
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.RangeReaders == 0 && st.CacheWriters == 0 && st.BufferReserved == 0
	})
}
