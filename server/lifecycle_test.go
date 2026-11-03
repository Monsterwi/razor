package server_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Monsterwi/razor/balancer"
	"github.com/Monsterwi/razor/cache"
	appconfig "github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
	"github.com/Monsterwi/razor/server"
)

func lifecycleServer(t *testing.T, origin string, cfg *appconfig.Config) *server.Server {
	t.Helper()
	cfg.UpstreamTargets = []string{origin}
	cfg.UpstreamAlgo = balancer.R2Balancer
	c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache")}, []int64{32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo, proxy.WithTrustedProxies(cfg.TrustedProxies), proxy.WithCAFile(cfg.UpstreamCAFile))
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(cfg, c, p)
	t.Cleanup(func() { s.Shutdown() })
	return s
}
func TestServerAdmissionCancellationAndDrain(t *testing.T) {
	entered := make(chan struct{}, 1)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; <-r.Context().Done() }))
	defer origin.Close()
	cfg := appconfig.GetDefaultConfig()
	cfg.MaxRequests = 1
	cfg.MaxQueuedRequests = 1
	s := lifecycleServer(t, origin.URL, cfg)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "http://front.test/one", nil).WithContext(firstCtx))
	}()
	<-entered
	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	defer cancelQueued()
	queuedDone := make(chan struct{})
	go func() {
		defer close(queuedDone)
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "http://front.test/two", nil).WithContext(queuedCtx))
	}()
	deadline := time.After(time.Second)
	for s.Stats().Queued != 1 {
		select {
		case <-deadline:
			t.Fatal("request did not queue")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	rejected := httptest.NewRecorder()
	s.ServeHTTP(rejected, httptest.NewRequest("GET", "http://front.test/three", nil))
	if rejected.Code != 503 {
		t.Fatalf("overload status=%d", rejected.Code)
	}
	cancelQueued()
	<-queuedDone
	drainCtx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	if err := s.ShutdownContext(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain error=%v", err)
	}
	<-firstDone
	rejected = httptest.NewRecorder()
	s.ServeHTTP(rejected, httptest.NewRequest("GET", "http://front.test/four", nil))
	if rejected.Code != 503 {
		t.Fatal("accepted request after shutdown")
	}
	stats := s.Stats()
	if stats.InFlight != 0 || stats.Queued != 0 || stats.Origin.InFlight != 0 || stats.Cancelled < 2 || stats.Rejected < 2 {
		t.Fatalf("stats=%+v", stats)
	}
}
func TestCacheObjectBudgetBypassesWithoutTruncation(t *testing.T) {
	payload := "abcdef"
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		io.WriteString(w, payload)
	}))
	defer origin.Close()
	cfg := appconfig.GetDefaultConfig()
	cfg.MaxCacheObjectBytes = 3
	s := lifecycleServer(t, origin.URL, cfg)
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("GET", "http://front.test/object", nil))
		if w.Body.String() != payload {
			t.Fatalf("body=%q", w.Body.String())
		}
	}
	st := s.Stats()
	if st.Origin.Requests != 2 || st.CacheWriters != 0 || st.CacheWriteSkipped != 2 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestListenerBoundsSlowHeadersAndClients(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/small" {
			io.WriteString(w, "ok")
			return
		}
		chunk := make([]byte, 64<<10)
		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			select {
			case <-r.Context().Done():
				return
			default:
			}
		}
	}))
	defer origin.Close()
	cfg := appconfig.GetDefaultConfig()
	cfg.RequestHeaderTimeout = 1
	cfg.TransactionTimeoutIn = 1
	cfg.MaxClientConnections = 1
	cfg.LogfileDir = t.TempDir()
	s := lifecycleServer(t, origin.URL, cfg)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	defer func() { s.Shutdown(); <-done }()
	address := listener.Addr().String()
	slow, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	io.WriteString(slow, "GET /small HTTP/1.1\r\nHost: front.test\r\nX-Slow:")
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get("http://" + address + "/small")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(body) != "ok" {
		t.Fatalf("slow header blocked progress: body=%q error=%v", body, err)
	}
	blocked, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.Close()
	blocked.(*net.TCPConn).SetReadBuffer(1024)
	io.WriteString(blocked, "GET /large HTTP/1.1\r\nHost: front.test\r\n\r\n")
	deadline := time.After(5 * time.Second)
	for {
		st := s.Stats()
		if st.Requests >= 2 && st.InFlight == 0 && st.Origin.InFlight == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("slow client retained work: %+v", st)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestCacheWriterQueueCancellationAndOverflow(t *testing.T) {
	entered := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		if r.URL.Path == "/held" {
			close(entered)
			io.WriteString(w, "a")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		io.WriteString(w, "complete")
	}))
	defer origin.Close()
	cfg := appconfig.GetDefaultConfig()
	cfg.MaxCacheWriters = 1
	cfg.MaxQueuedCacheWriters = 1
	s := lifecycleServer(t, origin.URL, cfg)
	frontend := httptest.NewServer(s)
	defer frontend.Close()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstRequest, _ := http.NewRequestWithContext(firstCtx, "GET", frontend.URL+"/held", nil)
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		resp, _ := frontend.Client().Do(firstRequest)
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "GET", frontend.URL+"/queued", nil)
	queued := make(chan error, 1)
	go func() {
		resp, err := frontend.Client().Do(request)
		if resp != nil {
			resp.Body.Close()
		}
		queued <- err
	}()
	deadline := time.After(time.Second)
	for s.Stats().CacheQueued != 1 {
		select {
		case <-deadline:
			t.Fatal("writer did not queue")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	third, err := frontend.Client().Get(frontend.URL + "/overflow")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(third.Body)
	third.Body.Close()
	if err != nil || third.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cache overload response=%d %q %v", third.StatusCode, body, err)
	}
	cancel()
	if err := <-queued; !errors.Is(err, context.Canceled) {
		t.Fatalf("queue cancel=%v", err)
	}
	cancelFirst()
	<-firstDone
	deadline = time.After(time.Second)
	for {
		st := s.Stats()
		if st.CacheWriters == 0 && st.CacheQueued == 0 && st.InFlight == 0 {
			if st.BufferReserved != 0 || st.SharedFillFailed < 1 {
				t.Fatalf("stats=%+v", st)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("writer resources retained: %+v", st)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestOversizedUnknownLengthFillFailsAndReleasesReservation(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=60")
		io.WriteString(w, "abcdef")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer origin.Close()
	cfg := appconfig.GetDefaultConfig()
	cfg.MaxCacheWriters = 1
	cfg.MaxCacheObjectBytes = 3
	s := lifecycleServer(t, origin.URL, cfg)
	frontend := httptest.NewServer(s)
	defer frontend.Close()
	assertFailedSharedHTTP(t, frontend.Client(), frontend.URL+"/large")
	deadline := time.After(time.Second)
	for {
		st := s.Stats()
		if st.CacheWriteSkipped == 1 && st.CacheWriters == 0 {
			if st.InFlight != 0 || st.BufferReserved != 0 {
				t.Fatalf("stats=%+v", st)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("oversized fill retained reservation: %+v", st)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestCacheCommitFailureDoesNotPublishOrigin(t *testing.T) {
	var calls atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=60")
		io.WriteString(w, "complete origin body")
		w.(http.Flusher).Flush()
	}))
	defer origin.Close()
	cfg := appconfig.GetDefaultConfig()
	cfg.UpstreamTargets = []string{origin.URL}
	cfg.UpstreamAlgo = balancer.R2Balancer
	c, err := cache.NewCache(1, []string{filepath.Join(t.TempDir(), "cache")}, []int64{32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	p, err := proxy.NewHTTPProxy(cfg.UpstreamTargets, cfg.UpstreamAlgo)
	if err != nil {
		t.Fatal(err)
	}
	s := server.New(cfg, c, p)
	defer s.Shutdown()
	front := httptest.NewServer(s)
	defer front.Close()
	if err = c.Stripes[0].Fd.Close(); err != nil {
		t.Fatal(err)
	}
	assertFailedSharedHTTP(t, front.Client(), front.URL+"/object")
	// Storage failure outranks PURGE revocation: a known permanent error
	// rejects subsequent fills before origin I/O, rather than enabling private streaming.
	resp, err := front.Client().Get(front.URL + "/object")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable || string(body) == "complete origin body" {
		t.Fatalf("known storage failure was not rejected: status=%d body=%q err=%v", resp.StatusCode, body, err)
	}
	st := s.Stats()
	if calls.Load() != 1 || st.Hits != 0 || st.CacheWriteErrors == 0 || st.CacheWriters != 0 || st.BufferReserved != 0 {
		t.Fatalf("failed cache commit was reused or leaked: calls=%d stats=%+v", calls.Load(), st)
	}
	if err = s.Shutdown(); err == nil {
		t.Fatal("storage shutdown failure was swallowed")
	}
}

func TestSlowUploadCannotHoldOriginOrHandlerAfterEarlyResponse(t *testing.T) {
	var originErrors bytes.Buffer
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The early response abandons the upload, so this connection cannot be reused.
		w.Header().Set("Connection", "close")
		http.NewResponseController(w).EnableFullDuplex()
		io.WriteString(w, "early")
	}))
	origin.Config.ErrorLog = log.New(&originErrors, "", 0)
	origin.Start()
	defer func() {
		origin.Close()
		if originErrors.Len() != 0 {
			t.Errorf("origin errors: %s", originErrors.String())
		}
	}()
	cfg := appconfig.GetDefaultConfig()
	cfg.TransactionTimeoutIn = 1
	cfg.LogfileDir = t.TempDir()
	s := lifecycleServer(t, origin.URL, cfg)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	defer func() { s.Shutdown(); <-done }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	io.WriteString(conn, "GET /upload HTTP/1.1\r\nHost: front.test\r\nContent-Length: 100\r\n\r\nx")
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || string(body) != "early" {
		t.Fatalf("early response=%q error=%v", body, err)
	}
	deadline := time.After(2 * time.Second)
	for {
		st := s.Stats()
		if st.InFlight == 0 && st.Origin.InFlight == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("slow upload held resources: %+v", st)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
