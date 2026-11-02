package server_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

func partialRangeData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*17 + i/(1<<20)*31) % 251)
	}
	return data
}

func TestRangeCombinesAdjacentMissingChunksWithOriginalClientBounds(t *testing.T) {
	data := partialRangeData(3<<20 + 17)
	var mu sync.Mutex
	var ranges []string
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", `"merged"`)
		if r.Method == http.MethodGet {
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	defer front.Close()
	const start, end = 17, (1 << 20) + 4321
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/merged", nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, data[start:end+1]) ||
		resp.ContentLength != end-start+1 || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)) {
		t.Fatalf("client bounds changed: status=%d bytes=%d headers=%v err=%v", resp.StatusCode, len(body), resp.Header, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"bytes=0-2097151"}; !reflect.DeepEqual(ranges, want) {
		t.Fatalf("origin ranges=%v want=%v", ranges, want)
	}
}
