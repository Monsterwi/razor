package server_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/server"
)

func TestPartialMetadataRefreshDoesNotExtendChunkFreshness(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().Unix())
	clock := func() time.Time { return time.Unix(now.Load(), 0) }
	var heads, gets atomic.Int32
	data := bytes.Repeat([]byte("freshness"), 1000)
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Date", clock().UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"unchanged"`)
		if r.Method == http.MethodHead {
			heads.Add(1)
			w.Header().Set("Cache-Control", "public,max-age=1")
		} else {
			gets.Add(1)
			w.Header().Set("Cache-Control", "public,max-age=10")
		}
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	})
	server.WithClock(clock)(s)
	front := httptest.NewServer(s)
	defer front.Close()
	for _, step := range []struct {
		seconds     int64
		heads, gets int32
	}{{0, 1, 1}, {2, 2, 1}, {9, 3, 2}, {0, 3, 2}} {
		now.Add(step.seconds)
		resp := rangeHTTP(t, front.Client(), front.URL+"/freshness", 17, 31)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data[17:32]) || resp.Header.Get("Cache-Control") != "no-cache" {
			t.Fatalf("response: status=%d body=%q error=%v", resp.StatusCode, body, err)
		}
		if heads.Load() != step.heads || gets.Load() != step.gets {
			t.Fatalf("metadata refresh changed chunk expiry: HEAD=%d GET=%d want HEAD=%d GET=%d", heads.Load(), gets.Load(), step.heads, step.gets)
		}
	}
}

func TestPartialUnsupportedRequestsRetainOriginSemantics(t *testing.T) {
	for _, header := range []http.Header{
		{"If-Match": {`"one"`}}, {"If-None-Match": {`"other"`}}, {"If-Range": {`"one"`}},
		{"If-Modified-Since": {"Sat, 01 Jan 2000 00:00:00 GMT"}}, {"If-Unmodified-Since": {"Sat, 01 Jan 2000 00:00:00 GMT"}},
		{"Authorization": {"Bearer client"}}, {"Cookie": {"client=one"}}, {"Accept-Encoding": {"gzip"}},
		{"Range": {"bytes=0-1,4-5"}},
	} {
		t.Run(fmt.Sprint(header), func(t *testing.T) {
			var heads, gets atomic.Int32
			data := []byte("0123456789")
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					heads.Add(1)
				} else {
					gets.Add(1)
				}
				for name, values := range header {
					if r.Header.Get(name) != values[0] {
						t.Errorf("origin lost %s: %q", name, r.Header.Get(name))
					}
				}
				w.Header().Set("ETag", `"one"`)
				w.Header().Set("Cache-Control", "public,max-age=3600")
				// This controlled origin ignores conditions/ranges so preservation is observable.
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				w.Write(data)
			})
			front := httptest.NewServer(s)
			defer front.Close()
			for range 2 {
				req, _ := http.NewRequest(http.MethodGet, front.URL+"/bypass", nil)
				req.Header = header.Clone()
				if req.Header.Get("Range") == "" {
					req.Header.Set("Range", "bytes=2-4")
				}
				resp, err := front.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || resp.StatusCode != 200 || !bytes.Equal(body, data) {
					t.Fatalf("bypass status=%d body=%q err=%v", resp.StatusCode, body, err)
				}
			}
			if heads.Load() != 0 || gets.Load() != 2 {
				t.Fatalf("unsupported request entered partial cache: HEAD=%d GET=%d", heads.Load(), gets.Load())
			}
		})
	}
}

func TestPartialIneligibleResponsesNeverBecomeSharedHits(t *testing.T) {
	for _, mode := range []string{"missing-etag", "weak-etag", "unknown-length", "private", "no-store", "unsupported-vary"} {
		t.Run(mode, func(t *testing.T) {
			var gets atomic.Int32
			data := []byte("0123456789abcdef")
			s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "public,max-age=3600")
				w.Header().Set("ETag", `"one"`)
				switch mode {
				case "missing-etag":
					w.Header().Del("ETag")
				case "weak-etag":
					w.Header().Set("ETag", `W/"one"`)
				case "private":
					w.Header().Set("Cache-Control", "private,max-age=3600")
				case "no-store":
					w.Header().Set("Cache-Control", "public,no-store,max-age=3600")
				case "unsupported-vary":
					w.Header().Set("Vary", "User-Agent")
				}
				if r.Method == http.MethodGet {
					gets.Add(1)
				}
				if mode == "unknown-length" {
					if r.Method == http.MethodGet {
						w.Header().Set("Content-Range", "bytes 2-6/16")
						w.WriteHeader(206)
					}
					w.(http.Flusher).Flush()
					if r.Method == http.MethodGet {
						w.Write(data[2:7])
					}
					return
				}
				http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
			})
			front := httptest.NewServer(s)
			defer front.Close()
			for range 2 {
				resp := rangeHTTP(t, front.Client(), front.URL+"/ineligible", 2, 6)
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data[2:7]) {
					t.Fatalf("passthrough status=%d body=%q error=%v", resp.StatusCode, body, err)
				}
			}
			if gets.Load() != 2 {
				t.Fatalf("ineligible response reused: GETs=%d", gets.Load())
			}
		})
	}
}
