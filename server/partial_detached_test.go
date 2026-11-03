package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/server"
)

func TestSingleByteCompletesBeforeIndependentChunkFill(t *testing.T) {
	data := partialRangeData(1 << 20)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"independent-chunk"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		if r.Method == http.MethodHead {
			return
		}
		gets.Add(1)
		if got := r.Header.Get("Range"); got != "bytes=0-1048575" {
			t.Errorf("origin Range=%q", got)
		}
		w.Header().Set("Content-Range", "bytes 0-1048575/1048576")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[:16<<10])
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = w.Write(data[16<<10:])
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableCompression: true, DisableKeepAlives: true}}
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/independent", nil)
	req.Header.Set("Range", "bytes=7-7")
	req.Close = true
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, data[7:8]) {
		t.Fatalf("single byte waited for aligned tail: status=%d body=%v err=%v", resp.StatusCode, body, err)
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 1 && st.SharedFillWaiters == 0 && st.CacheWriters == 1
	})
	follower := rangeHTTP(t, client, front.URL+"/independent", 0, len(data)-1)
	prefix := make([]byte, 4096)
	if _, err := io.ReadFull(follower.Body, prefix); err != nil || !bytes.Equal(prefix, data[:len(prefix)]) {
		t.Fatalf("new subscriber did not join independent fill: %v", err)
	}
	follower.Body.Close()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 0 })
	unblock()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
	hot := rangeHTTP(t, client, front.URL+"/independent", 0, len(data)-1)
	body, err = io.ReadAll(hot.Body)
	hot.Body.Close()
	if err != nil || !bytes.Equal(body, data) || gets.Load() != 1 {
		t.Fatalf("independently finished chunk not reused: bytes=%d gets=%d err=%v", len(body), gets.Load(), err)
	}
}

func TestDisconnectedPartialFillStopsAfterCurrentChunk(t *testing.T) {
	const chunk = 1 << 20
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("full=%t", full), func(t *testing.T) {
			data := partialRangeData(3 * chunk)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			cancelled := make(chan struct{})
			var gets atomic.Int32
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("ETag", `"stop-at-chunk"`)
				if r.Method != http.MethodGet || gets.Add(1) != 1 {
					http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
					return
				}
				length := len(data)
				if value := r.Header.Get("Range"); value != "" {
					if value != "bytes=0-2097151" {
						t.Errorf("merged Range=%q", value)
					}
					length = 2 * chunk
					w.Header().Set("Content-Range", "bytes 0-2097151/3145728")
				}
				w.Header().Set("Content-Length", fmt.Sprint(length))
				if !full {
					w.WriteHeader(http.StatusPartialContent)
				}
				_, _ = w.Write(data[:16<<10])
				w.(http.Flusher).Flush()
				select {
				case <-release:
					_, _ = w.Write(data[16<<10 : chunk])
					w.(http.Flusher).Flush()
				case <-r.Context().Done():
					close(cancelled)
					return
				}
				<-r.Context().Done()
				close(cancelled)
			})
			front := httptest.NewServer(s)
			t.Cleanup(front.Close)
			t.Cleanup(unblock)
			client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableCompression: true}}
			req, _ := http.NewRequest(http.MethodGet, front.URL+"/current", nil)
			if !full {
				req.Header.Set("Range", "bytes=0-3145727")
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			prefix := make([]byte, 4096)
			if _, err = io.ReadFull(resp.Body, prefix); err != nil || !bytes.Equal(prefix, data[:len(prefix)]) {
				t.Fatalf("prefix bytes=%d err=%v", len(prefix), err)
			}
			resp.Body.Close()
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 0 && st.SharedFillTasks == 1 })
			unblock()
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("unneeded next chunk retained the origin response")
			}
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
			if st := s.Stats(); st.Origin.Bytes != chunk || gets.Load() != 1 {
				t.Fatalf("read beyond current chunk: bytes=%d gets=%d", st.Origin.Bytes, gets.Load())
			}
			hot := rangeHTTP(t, client, front.URL+"/current", 0, chunk-1)
			body, err := io.ReadAll(hot.Body)
			hot.Body.Close()
			if err != nil || !bytes.Equal(body, data[:chunk]) || gets.Load() != 1 {
				t.Fatalf("current chunk did not finish after disconnect: bytes=%d gets=%d err=%v", len(body), gets.Load(), err)
			}
		})
	}
}

func TestIndependentChunkFillRemainsBoundedByTimeoutAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown=%t", shutdown), func(t *testing.T) {
			cancelled := make(chan struct{})
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("ETag", `"bounded-fill"`)
				w.Header().Set("Content-Length", "1048576")
				if r.Method == http.MethodHead {
					return
				}
				w.Header().Set("Content-Range", "bytes 0-1048575/1048576")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("prefix"))
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(cancelled)
			}))
			t.Cleanup(origin.Close)
			cfg := config.GetDefaultConfig()
			cfg.SharedFillTimeout = 1
			s := lifecycleServer(t, origin.URL, cfg)
			front := httptest.NewServer(s)
			t.Cleanup(front.Close)
			client := &http.Client{Timeout: 3 * time.Second}
			resp := rangeHTTP(t, client, front.URL+"/bounded", 0, 0)
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusPartialContent || string(body) != "p" {
				t.Fatalf("early response=%q status=%d err=%v", body, resp.StatusCode, err)
			}
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 0 && st.SharedFillTasks == 1 })
			if shutdown {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := s.ShutdownContext(ctx); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-cancelled:
			case <-time.After(2 * time.Second):
				t.Fatal("independent fill ignored its task lifetime")
			}
			waitFillStats(t, s, func(st server.Stats) bool {
				return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.BufferReserved == 0 && st.Origin.InFlight == 0
			})
		})
	}
}

func TestEarlyRangeSuccessDoesNotPublishFailedAlignedTail(t *testing.T) {
	data := partialRangeData(1 << 20)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"failed-tail"`)
		if r.Method != http.MethodGet || gets.Add(1) != 1 {
			http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
			return
		}
		w.Header().Set("Content-Length", "1048576")
		w.Header().Set("Content-Range", "bytes 0-1048575/1048576")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[:16<<10])
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	early := rangeHTTP(t, client, front.URL+"/failed-tail", 7, 7)
	body, err := io.ReadAll(early.Body)
	early.Body.Close()
	if err != nil || !bytes.Equal(body, data[7:8]) {
		t.Fatalf("early response=%v err=%v", body, err)
	}
	waiting := rangeHTTP(t, client, front.URL+"/failed-tail", 0, 65535)
	prefix := make([]byte, 16<<10)
	if _, err := io.ReadFull(waiting.Body, prefix); err != nil || !bytes.Equal(prefix, data[:len(prefix)]) {
		t.Fatalf("waiting response prefix: %v", err)
	}
	unblock()
	if _, err := io.ReadAll(waiting.Body); err == nil {
		t.Fatal("subscriber missing required bytes accepted truncated source")
	}
	waiting.Body.Close()
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
	if gets.Load() != 1 {
		t.Fatalf("failed fill retried in current request: %d", gets.Load())
	}
	refilled := rangeHTTP(t, client, front.URL+"/failed-tail", 0, len(data)-1)
	body, err = io.ReadAll(refilled.Body)
	refilled.Body.Close()
	if err != nil || !bytes.Equal(body, data) || gets.Load() != 2 {
		t.Fatalf("failed tail reused: bytes=%d gets=%d err=%v", len(body), gets.Load(), err)
	}
}

func TestIndependentOldChunkCannotPublishOverPurgeOrNewVersion(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(fmt.Sprintf("purge=%t", purge), func(t *testing.T) {
			oldData, newData := bytes.Repeat([]byte("a"), 1<<20), bytes.Repeat([]byte("b"), 1<<20)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			var newer atomic.Bool
			var gets atomic.Int32
			s, _ := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) {
				data, tag := oldData, `"old"`
				if newer.Load() {
					data, tag = newData, `"new"`
				}
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("ETag", tag)
				if r.Method != http.MethodGet || gets.Add(1) != 1 {
					http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
					return
				}
				w.Header().Set("Content-Length", "1048576")
				w.Header().Set("Content-Range", "bytes 0-1048575/1048576")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data[:16<<10])
				w.(http.Flusher).Flush()
				select {
				case <-release:
					_, _ = w.Write(data[16<<10:])
				case <-r.Context().Done():
				}
			})
			front := httptest.NewServer(s)
			t.Cleanup(front.Close)
			t.Cleanup(unblock)
			client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableCompression: true}}
			early := rangeHTTP(t, client, front.URL+"/old", 0, 0)
			body, err := io.ReadAll(early.Body)
			early.Body.Close()
			if err != nil || string(body) != "a" {
				t.Fatalf("early old response=%q err=%v", body, err)
			}
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 1 && st.SharedFillWaiters == 0 })
			if purge {
				req, _ := http.NewRequest("PURGE", front.URL+"/old", nil)
				req.Header.Set("Authorization", "Bearer test-secret")
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("PURGE status=%d", resp.StatusCode)
				}
			}
			newer.Store(true)
			req, _ := http.NewRequest(http.MethodGet, front.URL+"/old", nil)
			req.Header.Set("Range", "bytes=0-1048575")
			req.Header.Set("Cache-Control", "no-cache")
			fresh, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err = io.ReadAll(fresh.Body)
			fresh.Body.Close()
			if err != nil || !bytes.Equal(body, newData) {
				t.Fatalf("new version bytes=%d err=%v", len(body), err)
			}
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 1 && st.SharedFillWaiters == 0 })
			unblock()
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.CacheWriters == 0 })
			hot := rangeHTTP(t, client, front.URL+"/old", 0, len(newData)-1)
			body, err = io.ReadAll(hot.Body)
			hot.Body.Close()
			if err != nil || !bytes.Equal(body, newData) || gets.Load() != 2 {
				t.Fatalf("late old chunk replaced new cache: bytes=%d gets=%d err=%v", len(body), gets.Load(), err)
			}
		})
	}
}
