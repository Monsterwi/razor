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

func TestRangeStreamsWithinIncompleteChunkAndReusesCompletedChunk(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), (1<<20)/16)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		if r.Method == http.MethodHead {
			return
		}
		gets.Add(1)
		if r.Header.Get("Range") != "bytes=0-1048575" {
			t.Errorf("origin Range=%q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", "bytes 0-1048575/1048576")
		w.WriteHeader(206)
		_, _ = w.Write(data[:32<<10])
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = w.Write(data[32<<10:])
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 3 * time.Second}
	request := func(start, end int) *http.Response {
		t.Helper()
		req, _ := http.NewRequest("GET", front.URL+"/chunk", nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("no intra-chunk response: %v", err)
		}
		if resp.StatusCode != 206 || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/1048576", start, end) || resp.ContentLength != int64(end-start+1) {
			t.Fatalf("response status=%d headers=%v", resp.StatusCode, resp.Header)
		}
		return resp
	}
	first := request(17, 65535)
	defer first.Body.Close()
	prefix := make([]byte, 4096)
	if _, err := io.ReadFull(first.Body, prefix); err != nil || !bytes.Equal(prefix, data[17:17+4096]) {
		t.Fatalf("first prefix err=%v", err)
	}
	second := request(31, 65567)
	defer second.Body.Close()
	if _, err := io.ReadFull(second.Body, prefix); err != nil || !bytes.Equal(prefix, data[31:31+4096]) {
		t.Fatalf("second prefix err=%v", err)
	}
	if gets.Load() != 1 {
		t.Fatalf("inflight duplicate fills=%d", gets.Load())
	}
	unblock()
	for i, resp := range []*http.Response{first, second} {
		got, err := io.ReadAll(resp.Body)
		start, end := 17+4096, 65536
		if i == 1 {
			start, end = 31+4096, 65568
		}
		if err != nil || !bytes.Equal(got, data[start:end]) {
			t.Fatalf("subscriber %d body=%d err=%v", i, len(got), err)
		}
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 })
	third := request(80000, 80015)
	defer third.Body.Close()
	got, err := io.ReadAll(third.Body)
	if err != nil || !bytes.Equal(got, data[80000:80016]) || gets.Load() != 1 {
		t.Fatalf("reuse got=%q err=%v origin=%d", got, err, gets.Load())
	}
}

func TestSmallRangeCompletesOnlyAfterAlignedChunkIsValidated(t *testing.T) {
	data := bytes.Repeat([]byte("s"), 1<<20)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"small-range"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Content-Length", "1048576")
		if r.Method == "HEAD" {
			return
		}
		gets.Add(1)
		w.Header().Set("Content-Range", "bytes 0-1048575/1048576")
		w.WriteHeader(206)
		_, _ = w.Write(data[:32<<10])
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = w.Write(data[32<<10:])
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 3 * time.Second}
	req, _ := http.NewRequest("GET", front.URL+"/small-range", nil)
	req.Header.Set("Range", "bytes=7-8198")
	req.Close = true
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	prefix := make([]byte, 8191)
	if _, err = io.ReadFull(resp.Body, prefix); err != nil || !bytes.Equal(prefix, data[:8191]) {
		t.Fatalf("RWW prefix: %v", err)
	}
	last := make(chan error, 1)
	go func() {
		body, err := io.ReadAll(resp.Body)
		if err == nil && !bytes.Equal(body, []byte("s")) {
			err = fmt.Errorf("last byte %q", body)
		}
		last <- err
	}()
	select {
	case err := <-last:
		t.Fatalf("response completed before aligned chunk validated: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	if err = <-last; err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	req, _ = http.NewRequest("GET", front.URL+"/small-range", nil)
	req.Header.Set("Range", "bytes=70000-70015")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(got, data[:16]) || gets.Load() != 1 {
		t.Fatalf("completed Range reuse body=%q err=%v gets=%d", got, err, gets.Load())
	}
}

func TestIncompleteOrInvalidChunkIsNeverReused(t *testing.T) {
	for _, failure := range []string{"truncated", "content-range", "version-200"} {
		t.Run(failure, func(t *testing.T) {
			data := bytes.Repeat([]byte("checked"), (1<<20)/7+1)
			data = data[:1<<20]
			var gets atomic.Int32
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"one"`)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("Content-Length", "1048576")
				if r.Method == "HEAD" {
					return
				}
				attempt := gets.Add(1)
				w.Header().Set("Content-Range", "bytes 0-1048575/1048576")
				if attempt == 1 {
					switch failure {
					case "content-range":
						w.Header().Set("Content-Range", "bytes 1-1048576/1048577")
					case "version-200":
						w.Header().Set("ETag", `"new"`)
						w.Header().Del("Content-Range")
						w.WriteHeader(200)
						_, _ = w.Write(data)
						return
					}
					w.WriteHeader(206)
					_, _ = w.Write(data[:10000])
					return
				}
				w.WriteHeader(206)
				_, _ = w.Write(data)
			})
			front := httptest.NewServer(s)
			defer front.Close()
			client := &http.Client{Timeout: 3 * time.Second}
			get := func() (*http.Response, error) {
				req, _ := http.NewRequest("GET", front.URL+"/failed-chunk", nil)
				req.Header.Set("Range", "bytes=7-32774")
				return client.Do(req)
			}
			resp, err := get()
			if err == nil {
				body, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 206 && readErr == nil {
					t.Fatalf("invalid chunk succeeded with %d bytes", len(body))
				}
			}
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 })
			if gets.Load() != 1 {
				t.Fatalf("failed request retried %d origin GETs", gets.Load())
			}
			resp, err = get()
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != 206 || !bytes.Equal(got, data[7:32775]) || gets.Load() != 2 {
				t.Fatalf("recovery status=%d len=%d err=%v gets=%d", resp.StatusCode, len(got), err, gets.Load())
			}
		})
	}
}
