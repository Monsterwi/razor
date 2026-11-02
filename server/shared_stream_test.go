package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/server"
)

// A fill can fail before headers, or abort a response that has already streamed.
// Neither case is a successful short response, including for chunked transfer.
func assertFailedSharedHTTP(t *testing.T, client *http.Client, target string) {
	t.Helper()
	resp, err := client.Get(target)
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("unexpected request failure: %v", err)
		}
		return
	}
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusBadGateway && err == nil {
		return
	}
	if resp.StatusCode != http.StatusOK || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("failed fill ended as status=%d err=%v", resp.StatusCode, err)
	}
}

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

func TestSharedGETTruncationAfterPrefixFailsEveryReaderAndNextRequestRefills(t *testing.T) {
	prefix := bytes.Repeat([]byte("partial"), 1024)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var origins atomic.Int32
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		if origins.Add(1) > 1 {
			_, _ = w.Write([]byte("recovered body"))
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(2*len(prefix)))
		_, _ = w.Write(prefix)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	t.Cleanup(unblock)
	client := &http.Client{Timeout: 5 * time.Second}
	readers := make([]*http.Response, 2)
	for i := range readers {
		resp, err := client.Get(front.URL + "/truncated-stream")
		if err != nil {
			t.Fatal(err)
		}
		readers[i] = resp
		t.Cleanup(func() { resp.Body.Close() })
		got := make([]byte, len(prefix))
		if _, err := io.ReadFull(resp.Body, got); err != nil || !bytes.Equal(got, prefix) {
			t.Fatalf("prefix length=%d err=%v", len(got), err)
		}
	}
	unblock()
	for _, resp := range readers {
		body, err := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || !errors.Is(err, io.ErrUnexpectedEOF) || len(body) != 0 {
			t.Errorf("truncation became successful EOF: status=%d length=%d err=%v", resp.StatusCode, len(body), err)
		}
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 })
	resp, err := client.Get(front.URL + "/truncated-stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "recovered body" || calls.Load() != 2 {
		t.Fatalf("failed stream was reused: body=%q calls=%d err=%v", body, calls.Load(), err)
	}
}

func TestSharedGETAllStreamingSubscribersLeaveCancelsOrigin(t *testing.T) {
	originCancelled := make(chan struct{})
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("prefix"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(originCancelled)
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	client := &http.Client{Timeout: 5 * time.Second}
	var cancels []context.CancelFunc
	for range 2 {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		t.Cleanup(cancel)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, front.URL+"/cancel", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		got := make([]byte, 6)
		if _, err := io.ReadFull(resp.Body, got); err != nil || string(got) != "prefix" {
			t.Fatalf("stream prefix=%q err=%v", got, err)
		}
	}
	for _, cancel := range cancels {
		cancel()
	}
	select {
	case <-originCancelled:
	case <-time.After(time.Second):
		t.Fatal("unused shared origin did not cancel")
	}
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 && st.CacheWriters == 0 &&
			st.Origin.InFlight == 0 && st.ResponseBufferInUseBytes == 0
	})
	if calls.Load() != 1 {
		t.Fatalf("cancelled shared stream made %d origin requests", calls.Load())
	}
}

func TestSharedGETSlowCreatorDoesNotBackpressureOtherSubscriber(t *testing.T) {
	prefix := bytes.Repeat([]byte("p"), 4096)
	suffix := bytes.Repeat([]byte("s"), 12<<20)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	s, calls := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Length", strconv.Itoa(len(prefix)+len(suffix)))
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
	client := &http.Client{Timeout: 5 * time.Second}
	first, err := client.Get(front.URL + "/slow-client")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Body.Close() })
	// The creator never consumes its body. The payload exceeds TCP send buffers.
	second, err := client.Get(front.URL + "/slow-client")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Body.Close()
	unblock()
	got, err := io.ReadAll(second.Body)
	if err != nil || !bytes.Equal(got, append(prefix, suffix...)) || calls.Load() != 1 {
		t.Fatalf("slow creator blocked peer: bytes=%d calls=%d err=%v", len(got), calls.Load(), err)
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 })
	// The completed writer must be visible to a new cache lookup even while
	// the first subscriber still retains its own writer/entry reference.
	third, err := client.Get(front.URL + "/slow-client")
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(third.Body)
	third.Body.Close()
	if err != nil || !bytes.Equal(got, append(prefix, suffix...)) || calls.Load() != 1 {
		t.Fatalf("retained reader hid committed hit: bytes=%d calls=%d err=%v", len(got), calls.Load(), err)
	}
	first.Body.Close()
	waitFillStats(t, s, func(st server.Stats) bool {
		return st.SharedFillWaiters == 0 && st.CacheWriters == 0 && st.ResponseBufferInUseBytes == 0
	})
	st := s.Stats()
	if st.ResponseBufferTotalAllocatedBytes > st.ResponseBufferCapacityBytes || st.BufferReserved != 0 {
		t.Fatalf("stream retained unbounded or leaked buffers: %+v", st)
	}
}

func TestSharedGETImmediateConnectionCloseAfterBodyKeepsCompletedCache(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 256<<10)
	var gets atomic.Int32
	s, _ := publicCacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("ETag", `"small"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodGet {
			gets.Add(1)
			_, _ = w.Write(body)
		}
	})
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	transport := &http.Transport{DisableKeepAlives: true}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	for i := range 64 {
		for range 2 {
			resp, err := client.Get(fmt.Sprintf("%s/small-%d", front.URL, i))
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
				t.Fatalf("response=%d length=%d err=%v", resp.StatusCode, len(got), err)
			}
		}
		if gets.Load() != int32(i+1) {
			t.Fatalf("completed response refetched after connection close: path=%d gets=%d", i, gets.Load())
		}
	}
}
