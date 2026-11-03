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

func TestPartialPurgeSparseExactAndPrefixLeaveOtherObjects(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		t.Run(fmt.Sprint(prefix), func(t *testing.T) {
			oldData, newData := bytes.Repeat([]byte("a"), 8<<20), bytes.Repeat([]byte("b"), 8<<20)
			var newer atomic.Bool
			var mu sync.Mutex
			gets := map[string]int{}
			s, _ := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) {
				data, tag := oldData, `"one"`
				if newer.Load() {
					data, tag = newData, `"two"`
				}
				w.Header().Set("ETag", tag)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if r.Method == "GET" {
					mu.Lock()
					gets[r.URL.Path]++
					mu.Unlock()
				}
				http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
			})
			front := httptest.NewServer(s)
			t.Cleanup(front.Close)
			client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
			read := func(path string, index int, expected byte) {
				t.Helper()
				req, _ := http.NewRequest("GET", front.URL+path, nil)
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", index*(1<<20)+17, index*(1<<20)+1040))
				resp, e := client.Do(req)
				if e != nil {
					t.Fatal(e)
				}
				got, e := io.ReadAll(resp.Body)
				resp.Body.Close()
				if e != nil || resp.StatusCode != 206 || !bytes.Equal(got, bytes.Repeat([]byte{expected}, 1024)) {
					t.Fatalf("%s chunk%d status=%d bytes=%d err=%v", path, index, resp.StatusCode, len(got), e)
				}
			}
			for _, index := range []int{0, 3, 7} {
				read("/folder/object", index, 'a')
			}
			read("/folder/sibling", 3, 'a')
			read("/folder-other/object", 7, 'a')
			path := "/folder/object"
			if prefix {
				path = "/folder/"
			}
			req, _ := http.NewRequest("PURGE", front.URL+path, nil)
			req.Header.Set("Authorization", "Bearer test-secret")
			resp, e := client.Do(req)
			if e != nil {
				t.Fatal(e)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatal(resp.StatusCode)
			}
			newer.Store(true)
			for _, index := range []int{0, 3, 7} {
				read("/folder/object", index, 'b')
			}
			sibling := 'a'
			wantSibling := 1
			if prefix {
				sibling = 'b'
				wantSibling = 2
			}
			read("/folder/sibling", 3, byte(sibling))
			read("/folder-other/object", 7, 'a')
			mu.Lock()
			defer mu.Unlock()
			if gets["/folder/object"] != 6 || gets["/folder/sibling"] != wantSibling || gets["/folder-other/object"] != 1 {
				t.Fatalf("sparse purge scope: %v", gets)
			}
		})
	}
}

func TestPartialPurgeDoesNotInterruptNonTargetActiveGET(t *testing.T) {
	data := bytes.Repeat([]byte("neighbor"), (2<<20)/8+2)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	var gets atomic.Int32
	s, _ := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) {
		gets.Add(1)
		w.Header().Set("ETag", `"neighbor"`)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		_, _ = w.Write(data[:6<<10])
		w.(http.Flusher).Flush()
		select {
		case <-gate:
			_, _ = w.Write(data[6<<10:])
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(release)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Get(front.URL + "/folder-other/object")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	prefix := make([]byte, 4096)
	if _, err = io.ReadFull(resp.Body, prefix); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/folder/object", "/folder/"} {
		req, _ := http.NewRequest("PURGE", front.URL+path, nil)
		req.Header.Set("Authorization", "Bearer test-secret")
		purged, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		io.Copy(io.Discard, purged.Body)
		purged.Body.Close()
		if purged.StatusCode != 200 {
			t.Fatal(purged.StatusCode)
		}
	}
	release()
	tail, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(append(prefix, tail...), data) {
		t.Fatalf("unrelated PURGE interrupted body: bytes=%d err=%v", len(tail), err)
	}
	hot, err := client.Get(front.URL + "/folder-other/object")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(hot.Body)
	hot.Body.Close()
	if err != nil || !bytes.Equal(got, data) || gets.Load() != 1 {
		t.Fatalf("unrelated PURGE revoked cache publication: GET=%d err=%v", gets.Load(), err)
	}
}

func TestPartialPurgeFailureCannotReturnSuccess(t *testing.T) {
	s, c := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) { t.Error("PURGE contacted origin") })
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("PURGE", front.URL+"/closed", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 500 || resp.StatusCode > 599 {
		t.Fatalf("failed invalidation reported success: %d", resp.StatusCode)
	}
}
