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
)

func TestLateOldRangeFailureCannotFenceNewGeneration(t *testing.T) {
	for _, mode := range []string{"new-etag", "same-etag-shorter", "same-generation-refreshed"} {
		t.Run(mode, func(t *testing.T) {
			const chunk = 1 << 20
			old, newer := bytes.Repeat([]byte("a"), 2*chunk), bytes.Repeat([]byte("b"), chunk+91)
			if mode == "same-generation-refreshed" {
				newer = old
			}
			var changed, newGated atomic.Bool
			var gets atomic.Int32
			oldEntered, oldRelease := make(chan struct{}), make(chan struct{})
			newRelease := make(chan struct{})
			unblockOld := sync.OnceFunc(func() { close(oldRelease) })
			unblockNew := sync.OnceFunc(func() { close(newRelease) })
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				isNew := changed.Load()
				body, tag := old, `"old"`
				if isNew {
					body = newer
					if mode == "new-etag" {
						tag = `"new"`
					}
				}
				w.Header().Set("ETag", tag)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if r.Method == http.MethodGet {
					gets.Add(1)
					if !isNew && r.Header.Get("Range") == "bytes=1048576-2097151" {
						close(oldEntered)
						select {
						case <-oldRelease:
							w.WriteHeader(http.StatusPreconditionFailed)
						case <-r.Context().Done():
						}
						return
					}
					if isNew && newGated.CompareAndSwap(false, true) {
						out := &originBoundaryWriter{ResponseWriter: w, boundary: 32 << 10, cross: func() error {
							select {
							case <-newRelease:
								return nil
							case <-r.Context().Done():
								return r.Context().Err()
							}
						}}
						http.ServeContent(out, r, "body", time.Time{}, bytes.NewReader(body))
						return
					}
				}
				http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(body))
			})
			front := httptest.NewServer(s)
			t.Cleanup(front.Close)
			t.Cleanup(unblockOld)
			t.Cleanup(unblockNew)
			client := &http.Client{Timeout: 10 * time.Second}
			warm := rangeHTTP(t, client, front.URL+"/version", 0, chunk-1)
			warmBody, err := io.ReadAll(warm.Body)
			warm.Body.Close()
			if err != nil || warm.StatusCode != 206 || !bytes.Equal(warmBody, old[:chunk]) {
				t.Fatalf("warm: status=%d bytes=%d error=%v", warm.StatusCode, len(warmBody), err)
			}
			type result struct {
				status int
				body   []byte
				err    error
			}
			oldDone := make(chan result, 1)
			go func() {
				req, _ := http.NewRequest(http.MethodGet, front.URL+"/version", nil)
				req.Header.Set("Range", "bytes=1048576-2097151")
				resp, err := client.Do(req)
				if err != nil {
					oldDone <- result{err: err}
					return
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				oldDone <- result{resp.StatusCode, body, err}
			}()
			select {
			case <-oldEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("old missing chunk did not reach origin gate")
			}
			changed.Store(true)
			req, _ := http.NewRequest(http.MethodGet, front.URL+"/version", nil)
			req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", len(newer)-1))
			req.Header.Set("Cache-Control", "no-cache")
			current, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer current.Body.Close()
			prefix := make([]byte, 4096)
			if _, err = io.ReadFull(current.Body, prefix); err != nil || !bytes.Equal(prefix, newer[:len(prefix)]) {
				t.Fatalf("new generation did not stream before old failure: %v", err)
			}
			unblockOld()
			select {
			case previous := <-oldDone:
				if previous.status != 502 || previous.err != nil {
					t.Fatalf("old version unavailable before headers: status=%d error=%v", previous.status, previous.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("old failure did not terminate")
			}
			if gets.Load() != 3 {
				t.Fatalf("old failure retried: GETs=%d want=3", gets.Load())
			}
			unblockNew()
			rest, err := io.ReadAll(current.Body)
			if err != nil || current.StatusCode != 206 || current.ContentLength != int64(len(newer)) || current.Header.Get("Content-Range") != fmt.Sprintf("bytes 0-%d/%d", len(newer)-1, len(newer)) || !bytes.Equal(append(prefix, rest...), newer) {
				t.Fatalf("late old failure damaged new generation: status=%d bytes=%d error=%v", current.StatusCode, len(prefix)+len(rest), err)
			}
			wantGets := int32(3)
			if mode == "same-generation-refreshed" {
				wantGets = 4
			}
			after := rangeHTTP(t, client, front.URL+"/version", chunk, len(newer)-1)
			body, err := io.ReadAll(after.Body)
			after.Body.Close()
			if err != nil || after.StatusCode != 206 || !bytes.Equal(body, newer[chunk:]) || gets.Load() != wantGets {
				t.Fatalf("new short tail not retained: status=%d bytes=%d GETs=%d error=%v", after.StatusCode, len(body), gets.Load(), err)
			}
		})
	}
}
