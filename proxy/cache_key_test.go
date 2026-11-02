package proxy

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"
)

func TestCacheKeySeparatesCompleteRequestIdentity(t *testing.T) {
	targets := []string{"http://a.test/item?id=1", "http://a.test/item?id=2", "http://b.test/item?id=1", "https://a.test/item?id=1", "http://a.test/a%2Fb", "http://a.test/a/b", "http://a.test/item?a=1&b=2", "http://a.test/item?", "http://a.test/item",
		"http://a.test/item?q=a+b", "http://a.test/item?q=a%20b",
		"http://a.test/item?flag", "http://a.test/item?flag=",
		"http://a.test/item?a=1&a=2", "http://a.test/item?a=1&a=1&a=2",
		"http://a.test/item?v=%2f", "http://a.test/item?v=%2F",
		"http://a.test/item?a=1&&b=2", "http://a.test/item?a=1;b=2"}
	seen := map[string]string{}
	for _, target := range targets {
		req := httptest.NewRequest("GET", target, nil)
		key := GetCacheKey(req)
		if other, ok := seen[key]; ok {
			t.Fatalf("%s and %s share cache identity", other, target)
		}
		seen[key] = target
	}
	plain := httptest.NewRequest("GET", "/item", nil)
	secure := httptest.NewRequest("GET", "/item", nil)
	secure.TLS = &tls.ConnectionState{}
	if GetCacheKey(plain) == GetCacheKey(secure) {
		t.Fatal("TLS origin-form request shares plain HTTP identity")
	}
}

func TestCacheKeyPreservesQueryOrder(t *testing.T) {
	for _, queries := range [][2]string{
		{"a=1&b=2", "b=2&a=1"},
		{"a=1&a=2&b=2", "b=2&a=2&a=1"},
		{"q=%26%3D&x=1", "x=1&q=%26%3D"},
		{"empty=&flag&x=1", "x=1&flag&empty="},
		{"&a=1&b=2", "b=2&&a=1"},
		{"a=%zz&b=2", "b=2&a=%zz"},
	} {
		t.Run(queries[1], func(t *testing.T) {
			first := httptest.NewRequest("GET", "http://a.test/item?"+queries[0], nil)
			second := httptest.NewRequest("GET", "http://a.test/item?"+queries[1], nil)
			if got, want := GetCacheKey(second), GetCacheKey(first); got == want {
				t.Fatalf("query permutation shared cache identity: %q == %q", got, want)
			}
			if first.URL.RawQuery != queries[0] || second.URL.RawQuery != queries[1] {
				t.Fatal("cache lookup rewrote the origin query")
			}
		})
	}
}
