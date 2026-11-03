package server_test

import (
	"bytes"
	"context"
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
	objectReplacementStream(t, true, false, false)
}
func TestUnchangedObjectRefreshKeepsOldHTTPStream(t *testing.T) {
	objectReplacementStream(t, false, false, false)
}
func TestUnchangedRefreshInProgressKeepsOldHTTPStream(t *testing.T) {
	objectReplacementStream(t, false, true, false)
}
func TestUnchangedRefreshWaitHonorsClientCancellation(t *testing.T) {
	objectReplacementStream(t, false, true, true)
}
func objectReplacementStream(t *testing.T, changedVersion, activeRefresh, cancelOld bool) {
	old, newer := bytes.Repeat([]byte("a"), 2<<20), bytes.Repeat([]byte("b"), 2<<20)
	var version, holdRefresh atomic.Bool
	refreshTail := make(chan struct{})
	finishRefresh := sync.OnceFunc(func() { close(refreshTail) })
	defer finishRefresh()
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
		if holdRefresh.Load() && r.Header.Get("Range") == "bytes=1048576-2097151" {
			w.Header().Set("Content-Length", "1048576")
			w.Header().Set("Content-Range", "bytes 1048576-2097151/2097152")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[1<<20 : (1<<20)+(16<<10)])
			w.(http.Flusher).Flush()
			select {
			case <-refreshTail:
				_, _ = w.Write(body[(1<<20)+(16<<10):])
			case <-r.Context().Done():
			}
			return
		}
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
	firstRead := make(chan struct{})
	oldContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		req, _ := http.NewRequestWithContext(oldContext, "GET", endpoint.URL+"/item", nil)
		req.Host = "cache.test"
		req.Header = headers.Clone()
		req.Header.Set("X-Test-Gate", "1")
		resp, err := endpoint.Client().Do(req)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer resp.Body.Close()
		prefix := make([]byte, 1<<20)
		n, err := io.ReadFull(resp.Body, prefix)
		close(firstRead)
		if err != nil {
			done <- result{resp.StatusCode, resp.Header.Get("ETag"), prefix[:n], err}
			return
		}
		body, err := io.ReadAll(resp.Body)
		done <- result{resp.StatusCode, resp.Header.Get("ETag"), append(prefix, body...), err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old first block did not reach send barrier")
	}
	version.Store(changedVersion)
	revalidate := headers.Clone()
	revalidate.Set("Cache-Control", "no-cache")
	if activeRefresh {
		holdRefresh.Store(true)
		revalidate.Set("Range", "bytes=1048576-1048576")
	}
	request, _ := http.NewRequest(http.MethodGet, endpoint.URL+"/item", nil)
	request.Host, request.Header = "cache.test", revalidate
	changed, err := endpoint.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	changedBody, err := io.ReadAll(changed.Body)
	changed.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	expected := old
	if changedVersion {
		expected = newer
	}
	if activeRefresh {
		expected = expected[1<<20 : (1<<20)+1]
	}
	if changed.StatusCode != 206 || !bytes.Equal(changedBody, expected) {
		t.Fatalf("replacement status=%d bytes=%d", changed.StatusCode, len(changedBody))
	}
	if !activeRefresh {
		waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
	}
	beforeResume := gets.Load()
	release.Do(func() { close(resume) })
	if activeRefresh {
		select {
		case <-firstRead:
		case <-time.After(time.Second):
			t.Fatal("old prefix did not resume")
		}
		select {
		case got := <-done:
			t.Fatalf("old stream did not wait for same-version replacement: bytes=%d err=%v", len(got.body), got.err)
		case <-time.After(50 * time.Millisecond):
		}
		if cancelOld {
			cancel()
			select {
			case got := <-done:
				if got.err == nil {
					t.Fatal("cancelled old response completed while refresh blocked")
				}
			case <-time.After(time.Second):
				t.Fatal("old reader ignored cancellation while waiting for refresh")
			}
			waitFillStats(t, s, func(st server.Stats) bool {
				return st.InFlight == 0 && st.SharedFillTasks == 1 && st.SharedFillWaiters == 0
			})
			finishRefresh()
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
			if gets.Load() != beforeResume {
				t.Fatalf("waiter retried origin: %d != %d", gets.Load(), beforeResume)
			}
			return
		}
		finishRefresh()
	}
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
