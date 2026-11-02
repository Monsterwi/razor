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

func TestPartialPurgeKeepsStartedResponseAndSeparatesNewGeneration(t *testing.T) {
	for _, full := range []bool{true, false} {
		t.Run(fmt.Sprint(full), func(t *testing.T) {
			oldData := bytes.Repeat([]byte("old-content-"), (3<<20)/12+3)
			newData := bytes.Repeat([]byte("new-content-"), len(oldData)/12)
			var version, gets, heads, newGets, ordinary atomic.Int32
			version.Store(1)
			oldGate, newGate := make(chan struct{}), make(chan struct{})
			releaseOld, releaseNew := sync.OnceFunc(func() { close(oldGate) }), sync.OnceFunc(func() { close(newGate) })
			newCancelled := make(chan struct{}, 1)
			s, _ := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) {
				selected := version.Load()
				if r.Header.Get("If-Match") == `"v1"` {
					selected = 1 // This origin still serves the selected old version.
				}
				data, gate := oldData, oldGate
				if selected == 2 {
					data, gate = newData, newGate
				}
				w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, selected))
				w.Header().Set("Cache-Control", "public,max-age=3600")
				if r.Method == "HEAD" {
					heads.Add(1)
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					return
				}
				gets.Add(1)
				if selected == 2 {
					newGets.Add(1)
				}
				if r.Header.Get("Range") == "" {
					ordinary.Add(1)
				}
				start, end := 0, len(data)-1
				if r.Header.Get("Range") != "" {
					if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
				}
				w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
				if r.Header.Get("Range") != "" {
					w.WriteHeader(206)
				}
				prefixEnd := min(start+(6<<10), end+1)
				_, _ = w.Write(data[start:prefixEnd])
				w.(http.Flusher).Flush()
				select {
				case <-gate:
					_, _ = w.Write(data[prefixEnd : end+1])
				case <-r.Context().Done():
					if selected == 2 {
						newCancelled <- struct{}{}
					}
				}
			})
			front := httptest.NewServer(s)
			t.Cleanup(front.Close)
			t.Cleanup(releaseNew)
			t.Cleanup(releaseOld)
			client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableCompression: true}}
			request := func(method, interval string) *http.Response {
				t.Helper()
				req, _ := http.NewRequest(method, front.URL+"/purge-object", nil)
				if interval != "" {
					req.Header.Set("Range", interval)
				}
				if method == "PURGE" {
					req.Header.Set("Authorization", "Bearer test-secret")
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				return resp
			}
			interval := ""
			if !full {
				interval = "bytes=17-3145746"
			}
			old := request("GET", interval)
			defer old.Body.Close()
			prefix := make([]byte, 4096)
			start, end := 0, len(oldData)
			if !full {
				start, end = 17, (3<<20)+19
			}
			if _, err := io.ReadFull(old.Body, prefix); err != nil || !bytes.Equal(prefix, oldData[start:start+len(prefix)]) {
				t.Fatalf("old prefix: %v", err)
			}
			purged := request("PURGE", "")
			io.Copy(io.Discard, purged.Body)
			purged.Body.Close()
			if purged.StatusCode != 200 {
				t.Fatalf("PURGE=%d", purged.StatusCode)
			}
			version.Store(2)
			fresh := request("GET", "")
			defer fresh.Body.Close()
			if _, err := io.ReadFull(fresh.Body, prefix); err != nil || fresh.StatusCode != 200 || !bytes.Equal(prefix, newData[:len(prefix)]) {
				t.Fatalf("new prefix status=%d err=%v", fresh.StatusCode, err)
			}
			releaseOld()
			oldTail, err := io.ReadAll(old.Body)
			if err != nil || !bytes.Equal(oldTail, oldData[start+len(prefix):end]) {
				t.Fatalf("started old response aborted: bytes=%d err=%v", len(oldTail), err)
			}
			waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillWaiters == 1 })
			select {
			case <-newCancelled:
				t.Fatal("old task exit cancelled new generation")
			default:
			}
			releaseNew()
			newTail, err := io.ReadAll(fresh.Body)
			if err != nil || !bytes.Equal(newTail, newData[len(prefix):]) {
				t.Fatalf("new response bytes=%d err=%v", len(newTail), err)
			}
			for _, interval := range []string{"", "bytes=11-65535"} {
				resp := request("GET", interval)
				got, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				want, status := newData, 200
				if interval != "" {
					want, status = newData[11:65536], 206
				}
				if err != nil || resp.StatusCode != status || !bytes.Equal(got, want) {
					t.Fatalf("old generation republished: status=%d bytes=%d err=%v", resp.StatusCode, len(got), err)
				}
			}
			wantHeads := int32(0)
			if !full {
				wantHeads = 1
			}
			wantOrdinary := int32(1)
			if full {
				wantOrdinary = 2
			}
			if newGets.Load() != 1 || ordinary.Load() != wantOrdinary || heads.Load() != wantHeads || (!full && gets.Load() != 3) {
				t.Fatalf("unexpected origin work GET=%d new=%d ordinary=%d HEAD=%d", gets.Load(), newGets.Load(), ordinary.Load(), heads.Load())
			}
		})
	}
}

func TestPartialPurgeOldRangeVersionFailureDoesNotRetryOrRetireNew(t *testing.T) {
	oldData, newData := bytes.Repeat([]byte("o"), 4<<20), bytes.Repeat([]byte("n"), 4<<20)
	var changed atomic.Bool
	var gets, refused atomic.Int32
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	s, _ := purgeServer(t, "test-secret", func(w http.ResponseWriter, r *http.Request) {
		data, tag := oldData, `"old"`
		if changed.Load() {
			data, tag = newData, `"new"`
		}
		w.Header().Set("ETag", tag)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			return
		}
		gets.Add(1)
		if r.Header.Get("If-Match") == `"old"` && changed.Load() {
			refused.Add(1)
			w.WriteHeader(412)
			return
		}
		if r.Header.Get("Range") == "bytes=0-2097151" {
			w.Header().Set("Content-Length", "2097152")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-2097151/%d", len(data)))
			w.WriteHeader(206)
			_, _ = w.Write(data[:6<<10])
			w.(http.Flusher).Flush()
			select {
			case <-gate:
				_, _ = w.Write(data[6<<10 : 2<<20])
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
	req, _ := http.NewRequest("GET", front.URL+"/version", nil)
	req.Header.Set("Range", "bytes=17-4194303")
	old, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Body.Close()
	prefix := make([]byte, 4096)
	if _, err = io.ReadFull(old.Body, prefix); err != nil || !bytes.Equal(prefix, oldData[:4096]) {
		t.Fatal("old prefix missing")
	}
	req, _ = http.NewRequest("PURGE", front.URL+"/version", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	purged, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, purged.Body)
	purged.Body.Close()
	if purged.StatusCode != 200 {
		t.Fatal(purged.StatusCode)
	}
	changed.Store(true)
	fresh, err := client.Get(front.URL + "/version")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(fresh.Body)
	fresh.Body.Close()
	if err != nil || !bytes.Equal(got, newData) {
		t.Fatal("new generation failed")
	}
	release()
	tail, err := io.ReadAll(old.Body)
	if err == nil || old.StatusCode != 206 || len(tail) != (2<<20)-17-len(prefix) || !bytes.Equal(tail, oldData[:len(tail)]) {
		t.Fatalf("version failure must truncate original response: status=%d tail=%d err=%v", old.StatusCode, len(tail), err)
	}
	fresh, err = client.Get(front.URL + "/version")
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(fresh.Body)
	fresh.Body.Close()
	if err != nil || !bytes.Equal(got, newData) || gets.Load() != 3 || refused.Load() != 1 {
		t.Fatalf("old private run retried or retired new: GET=%d refused=%d err=%v", gets.Load(), refused.Load(), err)
	}
}
