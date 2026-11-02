package server_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPartialQueryOrderIdentityAndExactPrefixPurge(t *testing.T) {
	red, blue := bytes.Repeat([]byte("red-"), 16000), bytes.Repeat([]byte("blue"), 16000)
	var gets atomic.Int32
	s, _ := purgeServer(t, "query-purge", func(w http.ResponseWriter, r *http.Request) {
		body, tag := red, `"red"`
		if r.URL.Query().Get("v") == "blue" {
			body, tag = blue, `"blue"`
		}
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("ETag", tag)
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(body))
	})
	front := httptest.NewServer(s)
	defer front.Close()
	request := func(method, path string, want []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, front.URL+path, nil)
		req.Host = "cache.test"
		if method == "PURGE" {
			req.Header.Set("Authorization", "Bearer query-purge")
		} else {
			req.Header.Set("Range", "bytes=3-35")
		}
		resp, err := front.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if method == "PURGE" {
			if err != nil || resp.StatusCode != 200 {
				t.Fatalf("PURGE %s: status=%d error=%v", path, resp.StatusCode, err)
			}
		} else if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, want[3:36]) || resp.Header.Get("Content-Range") != "bytes 3-35/64000" {
			t.Fatalf("GET %s: status=%d body=%q error=%v", path, resp.StatusCode, body, err)
		}
	}
	paths := []string{"/items/one?v=red&v=blue", "/items/one?v=blue&v=red", "/items/two?v=red", "/items-adjacent/one?v=red"}
	bodies := [][]byte{red, blue, red, red}
	for range 2 {
		for i, path := range paths {
			request(http.MethodGet, path, bodies[i])
		}
	}
	if gets.Load() != 4 {
		t.Fatalf("distinct query identities did not reuse independently: GETs=%d", gets.Load())
	}
	request("PURGE", paths[0], nil)
	request(http.MethodGet, paths[1], blue)
	if gets.Load() != 4 {
		t.Fatal("exact PURGE removed another query order")
	}
	request(http.MethodGet, paths[0], red)
	if gets.Load() != 5 {
		t.Fatal("exact PURGE left its query reachable")
	}
	request("PURGE", "/items/", nil)
	request(http.MethodGet, paths[3], red)
	if gets.Load() != 5 {
		t.Fatal("prefix PURGE removed adjacent path")
	}
	for i, path := range paths[:3] {
		request(http.MethodGet, path, bodies[i])
	}
	if gets.Load() != 8 {
		t.Fatalf("prefix PURGE left a query variant reachable: GETs=%d", gets.Load())
	}
}

func TestPartialAuthorityAndLanguageVariantsDoNotShareBytes(t *testing.T) {
	var gets atomic.Int32
	s, _ := cacheServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		data := bytes.Repeat([]byte(r.Header.Get("X-Forwarded-Host")+"/"+r.Header.Get("Accept-Language")+";"), 1000)
		w.Header().Set("Cache-Control", "public,max-age=3600")
		w.Header().Set("Vary", "Accept-Language")
		w.Header().Set("ETag", `"same-tag-on-different-representations"`)
		http.ServeContent(w, r, "body", time.Time{}, bytes.NewReader(data))
	})
	front := httptest.NewServer(s)
	defer front.Close()
	for range 2 {
		for _, host := range []string{"a.test", "b.test"} {
			for _, language := range []string{"en", "fr", ""} {
				req, _ := http.NewRequest(http.MethodGet, front.URL+"/variant", nil)
				req.Host = host
				req.Header.Set("Range", "bytes=0-31")
				if language != "" {
					req.Header.Set("Accept-Language", language)
				}
				resp, err := front.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				expected := bytes.Repeat([]byte(host+"/"+language+";"), 10)[:32]
				if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, expected) {
					t.Fatalf("variant %s %q: status=%d body=%q error=%v", host, language, resp.StatusCode, body, err)
				}
			}
		}
	}
	if gets.Load() != 6 {
		t.Fatalf("independent representations did not reuse: GETs=%d", gets.Load())
	}
}
