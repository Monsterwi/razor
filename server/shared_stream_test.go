package server_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Monsterwi/razor/server"
)

// The origin cannot finish its first cache fragment until both HTTP clients
// have received its prefix. This also distinguishes RWW from a fast cache hit.
func TestSharedGETStreamsBeforeFragmentCompleteAndSurvivesCreatorDisconnect(t *testing.T) {
	prefix := bytes.Repeat([]byte("prefix"), 1024)
	suffix := bytes.Repeat([]byte("suffix"), 1024)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Length", strconv.Itoa(len(prefix)+len(suffix)))
		w.Header().Set("ETag", `"one"`)
		_, _ = w.Write(prefix)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = w.Write(suffix)
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 3 * time.Second}
	creatorCtx, cancelCreator := context.WithCancel(context.Background())
	defer cancelCreator()
	req, _ := http.NewRequestWithContext(creatorCtx, http.MethodGet, front.URL+"/stream", nil)
	first, err := client.Do(req)
	if err != nil {
		t.Fatalf("creator did not receive a partial fragment: %v", err)
	}
	defer first.Body.Close()
	readPrefix := func(resp *http.Response) {
		t.Helper()
		got := make([]byte, len(prefix))
		if _, err := io.ReadFull(resp.Body, got); err != nil || !bytes.Equal(got, prefix) {
			t.Fatalf("stream prefix=%q, err=%v", got, err)
		}
	}
	readPrefix(first)
	second, err := client.Get(front.URL + "/stream")
	if err != nil {
		t.Fatalf("follower did not receive a partial fragment: %v", err)
	}
	defer second.Body.Close()
	readPrefix(second)
	if calls.Load() != 1 {
		t.Fatalf("shared stream made %d origin requests", calls.Load())
	}
	cancelCreator()
	first.Body.Close()
	unblock()
	got, err := io.ReadAll(second.Body)
	if err != nil || !bytes.Equal(got, suffix) {
		t.Fatalf("follower suffix=%q, err=%v", got, err)
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 })
	third, err := client.Get(front.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer third.Body.Close()
	got, err = io.ReadAll(third.Body)
	if err != nil || !bytes.Equal(got, append(prefix, suffix...)) || calls.Load() != 1 {
		t.Fatalf("completed stream was not reusable: length=%d, calls=%d, err=%v", len(got), calls.Load(), err)
	}
}
