package server_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Monsterwi/razor/proxy"
	"github.com/Monsterwi/razor/server"
)

type terminalChunkBody struct {
	remaining []byte
	terminal  error
}

func (b *terminalChunkBody) Read(p []byte) (int, error) {
	if len(b.remaining) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.remaining)
	b.remaining = b.remaining[n:]
	if len(b.remaining) == 0 {
		return n, b.terminal
	}
	return n, nil
}
func (b *terminalChunkBody) Close() error { return nil }

type terminalChunkTransport struct {
	data          []byte
	terminal      error
	terminalAfter int
	mu            sync.Mutex
	ranges        []string
}

func (tr *terminalChunkTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	h := http.Header{"Cache-Control": {"public,max-age=3600"}, "Etag": {`"terminal-chunk"`}}
	start, end, status := 0, len(tr.data)-1, http.StatusOK
	body := io.ReadCloser(http.NoBody)
	if r.Method == http.MethodGet {
		value := r.Header.Get("Range")
		if value != "" {
			if _, err := fmt.Sscanf(value, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= len(tr.data) {
				return nil, fmt.Errorf("invalid origin Range %q", value)
			}
			status = http.StatusPartialContent
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(tr.data)))
		}
		tr.mu.Lock()
		tr.ranges = append(tr.ranges, value)
		terminal := io.EOF
		data := tr.data[start : end+1]
		if len(tr.ranges) == 1 {
			terminal = tr.terminal
			if tr.terminalAfter > 0 {
				data = data[:tr.terminalAfter]
			}
		}
		tr.mu.Unlock()
		body = &terminalChunkBody{remaining: data, terminal: terminal}
	}
	h.Set("Content-Length", strconv.Itoa(end-start+1))
	return &http.Response{StatusCode: status, Header: h, ContentLength: int64(end - start + 1), Body: body, Request: r}, nil
}

func (tr *terminalChunkTransport) requests() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]string(nil), tr.ranges...)
}

func TestPartialChunkTerminalReadResult(t *testing.T) {
	const chunk = 1 << 20
	for _, full := range []bool{false, true} {
		for _, size := range []int{128, chunk, 2 * chunk} {
			for _, failed := range []bool{false, true} {
				t.Run(fmt.Sprintf("full=%t/size=%d/error=%t", full, size, failed), func(t *testing.T) {
					terminal := io.EOF
					if failed {
						terminal = errors.New("terminal source failure")
					}
					tr := &terminalChunkTransport{data: partialRangeData(size), terminal: terminal}
					s, _ := cacheServer(t, func(http.ResponseWriter, *http.Request) {
						t.Error("injected transport was bypassed")
					}, proxy.WithTransport(tr))
					front := httptest.NewServer(s)
					defer front.Close()
					client := &http.Client{Timeout: 5 * time.Second}
					request := func(value string) (int, []byte, error) {
						t.Helper()
						req, err := http.NewRequest(http.MethodGet, front.URL+"/terminal", nil)
						if err != nil {
							t.Fatal(err)
						}
						req.Header.Set("Accept-Encoding", "identity")
						if value != "" {
							req.Header.Set("Range", value)
						}
						resp, err := client.Do(req)
						if err != nil {
							return 0, nil, err
						}
						body, err := io.ReadAll(resp.Body)
						resp.Body.Close()
						return resp.StatusCode, body, err
					}
					value, wantStatus := "", http.StatusOK
					if !full {
						value, wantStatus = fmt.Sprintf("bytes=0-%d", size-1), http.StatusPartialContent
					}
					status, body, err := request(value)
					if failed {
						if err == nil && status < 500 {
							t.Fatalf("source error accepted: status=%d bytes=%d", status, len(body))
						}
					} else if err != nil || status != wantStatus || !bytes.Equal(body, tr.data) {
						t.Fatalf("valid (n, EOF) rejected: status=%d bytes=%d err=%v", status, len(body), err)
					}
					waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 })
					if got := tr.requests(); len(got) != 1 {
						t.Fatalf("first response retried origin: %v", got)
					}
					if failed && size > chunk {
						status, body, err = request("bytes=0-1048575")
						if err != nil || status != http.StatusPartialContent || !bytes.Equal(body, tr.data[:chunk]) || len(tr.requests()) != 1 {
							t.Fatalf("completed prefix lost: status=%d bytes=%d origin=%v err=%v", status, len(body), tr.requests(), err)
						}
					}
					status, body, err = request(value)
					wantRequests := 1
					if failed {
						wantRequests = 2
					}
					gotRanges := tr.requests()
					if err != nil || status != wantStatus || !bytes.Equal(body, tr.data) || len(gotRanges) != wantRequests {
						t.Fatalf("cache reuse/refill: status=%d bytes=%d origin=%v err=%v", status, len(body), gotRanges, err)
					}
					if failed && size > chunk && gotRanges[1] != "bytes=1048576-2097151" {
						t.Fatalf("refill included completed prefix: %v", gotRanges)
					}
				})
			}
		}
	}
}

func TestRangeErrorAtNonfinalChunkBoundaryDoesNotPublish(t *testing.T) {
	const chunk = 1 << 20
	tr := &terminalChunkTransport{data: partialRangeData(2 * chunk), terminal: errors.New("first chunk source failure"), terminalAfter: chunk}
	s, _ := cacheServer(t, func(http.ResponseWriter, *http.Request) { t.Error("injected transport was bypassed") }, proxy.WithTransport(tr))
	front := httptest.NewServer(s)
	defer front.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	response := rangeHTTP(t, client, front.URL+"/terminal", 0, 2*chunk-1)
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr == nil && response.StatusCode < 500 {
		t.Fatalf("source error accepted: status=%d bytes=%d", response.StatusCode, len(body))
	}
	waitFillStats(t, s, func(st server.Stats) bool { return st.SharedFillTasks == 0 && st.SharedFillWaiters == 0 })
	response = rangeHTTP(t, client, front.URL+"/terminal", 0, chunk-1)
	body, readErr = io.ReadAll(response.Body)
	response.Body.Close()
	requests := tr.requests()
	if readErr != nil || response.StatusCode != http.StatusPartialContent || !bytes.Equal(body, tr.data[:chunk]) || len(requests) != 2 {
		t.Fatalf("failed first chunk reused: status=%d bytes=%d origin=%v err=%v", response.StatusCode, len(body), requests, readErr)
	}
	if requests[1] != "bytes=0-1048575" {
		t.Fatalf("wrong failed chunk refill: %v", requests)
	}
}
