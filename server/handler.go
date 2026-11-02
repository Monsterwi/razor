package server

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/logger"
	"github.com/Monsterwi/razor/proxy"
)

func (s *Server) handler(w http.ResponseWriter, r *http.Request) {
	runtime := requestRuntime(r)
	if runtime.config.SharedFillPublicStatic == 1 {
		recordPublicRequestIdentity(w, r)
	}
	key := []byte(fmt.Sprintf("%s:%s", runtime.namespace, runtime.proxy.CacheKey(r)))
	switch r.Method {
	case "PURGE":
		s.handlePurge(key, w, r)
	case http.MethodGet, http.MethodHead:
		s.handleGet(key, w, r)
	default:
		http.Error(w, "Unsupported Method", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(key []byte, w http.ResponseWriter, r *http.Request) {
	if rangeEligible(r) {
		s.serveRange(w, r, key)
		return
	}
	if !requestCacheable(r) {
		s.proxyWithoutCache(w, r)
		return
	}
	selected, selectedInfo, policy, lookupErr := s.lookupCached(key, r)
	if lookupErr != nil {
		s.proxyWithoutCache(w, r)
		return
	}
	if selected != nil {
		if requestPolicyFresh(r, policy, s.now()) {
			defer selected.Close()
			s.cacheStatus(w, "HIT")
			s.serveCacheHit(w, r, selected, selectedInfo, policy)
			return
		}
		selected.Close()
	}
	if r.Method == http.MethodHead {
		s.proxyWithoutCache(w, r)
		return
	}
	if requestRuntime(r).config.SharedFillPublicStatic == 1 && blockGETEligible(r) && s.serveBlocks(w, r, key, true) {
		return
	}
	// Capture the invalidation fence before origin work, never on a fresh HIT.
	token, _ := s.cache.BeginFill(key, key)
	s.serveSharedMiss(w, r, key, token)
}

func (s *Server) lookupCached(key []byte, r *http.Request) (*cache.CacheVC, *cache.CacheHTTPInfo, freshness, error) {
	// shortcut: bound negotiation fanout; excess variants safely bypass caching.
	keys, keysErr := s.cache.KeysForSelectorLimit(key, maxHTTPVariants)
	if keysErr != nil {
		return nil, nil, freshness{}, keysErr
	}
	for _, candidate := range keys {
		vc, err := s.cache.OpenReadCommitted([]byte(candidate))
		if err != nil {
			continue
		}
		info, err := vc.LoadHTTPInfo()
		if err != nil {
			vc.Close()
			continue
		}
		fields, valid := varyFields(info.ResponseHeaders)
		if policy, safe := responsePolicy(info, s.now()); valid && safe && string(representationKey(key, r.Header, fields)) == candidate {
			return vc, info, policy, nil
		}
		vc.Close()
	}
	return nil, nil, freshness{}, nil
}

func (s *Server) handlePurge(key []byte, w http.ResponseWriter, r *http.Request) {
	if requestRuntime(r).config.PurgeToken == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+requestRuntime(r).config.PurgeToken)) != 1 {
		logger.SetLogField(w, "purge_authorized", false)
		http.Error(w, "PURGE forbidden", http.StatusForbidden)
		return
	}
	logger.SetLogField(w, "purge_authorized", true)
	purgedKeys, err := s.cache.PurgeSelector(key, strings.HasSuffix(r.URL.EscapedPath(), "/") && r.URL.RawQuery == "")
	if err != nil {
		logger.SetLogField(w, "purge_result", "unavailable")
		http.Error(w, "PURGE unavailable", http.StatusServiceUnavailable)
		return
	}
	logger.SetLogField(w, "purge_count", len(purgedKeys))
	logger.SetLogField(w, "purge_result", "complete")
	s.cacheStatus(w, "PURGE")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "PURGED")
}

func copyHeaders(dst, src http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

func (s *Server) serveCacheHit(w http.ResponseWriter, r *http.Request, vc *cache.CacheVC, info *cache.CacheHTTPInfo, policy freshness) {
	copyHeaders(w.Header(), info.ResponseHeaders)
	now := s.now()
	age, valid := policy.currentAge(now)
	if !valid {
		policy, valid = responsePolicy(info, now)
		age = policy.age
	}
	if valid {
		w.Header().Set("Age", strconv.FormatInt(int64(age/time.Second), 10))
	}
	if clientNotModified(r, info.ResponseHeaders) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(info.Status)
	if r.Method == http.MethodHead {
		return
	}
	if vc.IsRWW() {
		// Headers have passed shared-cache admission. Make the start of the
		// response observable even if the producer fails before the first byte;
		// an unflushed abort on a reused connection can trigger client retries.
		if err := http.NewResponseController(w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			panic(http.ErrAbortHandler)
		}
	}
	if err := s.streamResponse(w, vc, r); err != nil {
		logger.Errorf("Cache response transfer failed: %v", err)
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) streamResponse(w http.ResponseWriter, body io.Reader, r *http.Request) error {
	runtime := requestRuntime(r)
	buffer, err := runtime.responseBuffers.acquire(r.Context())
	if err != nil {
		return err
	}
	defer runtime.responseBuffers.release(buffer)
	buf := buffer.data
	controller := http.NewResponseController(w)
	idle := time.Duration(runtime.config.TransactionTimeoutIn) * time.Second
	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			if err := r.Context().Err(); err != nil {
				return err
			}
			_ = controller.SetWriteDeadline(time.Now().Add(idle))
			written, err := w.Write(buf[:n])
			if err != nil {
				return err
			}
			if written != n {
				return io.ErrShortWrite
			}
			if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (s *Server) proxyWithoutCache(w http.ResponseWriter, r *http.Request) {
	s.cacheStatus(w, "BYPASS")
	resp, err := requestRuntime(r).proxy.Proxy(r)
	if err != nil {
		s.upstreamError(w, r, err)
		return
	}
	defer resp.RawBody().Close()
	copyHeaders(w.Header(), resp.Header())
	w.WriteHeader(resp.StatusCode())
	if r.Method != http.MethodHead {
		if err := s.streamResponse(w, resp.RawBody(), r); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}

func (s *Server) upstreamError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, proxy.ErrOverloaded) {
		status = http.StatusServiceUnavailable
		s.counters.rejected.Add(1)
	}
	if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
		status = http.StatusGatewayTimeout
	}
	http.Error(w, "Upstream unavailable", status)
}
func cacheSizeAllowed(maxObjectBytes int, key []byte, info *cache.CacheHTTPInfo, length int64) bool {
	if length > int64(maxObjectBytes) || len(key) > cache.MaxIndexKeyBytes {
		return false
	}
	size := len(info.RequestURL)
	for k, values := range info.ResponseHeaders {
		size += len(k)
		for _, v := range values {
			size += len(v)
		}
	}
	return size <= 64<<10
}
