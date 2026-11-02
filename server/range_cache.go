package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/proxy"
)

const rangeLayoutVersion = cache.PartialFormatVersion

var errRangeBypass = errors.New("range representation unsupported")
var errRangeChanged = errors.New("range representation changed")
var errRangeUnavailable = errors.New("range origin unavailable")
var errRangeOwnership = errors.New("range object writer unavailable")

type rangeObject = cache.PartialObject

func rangeEligible(r *http.Request) bool {
	if r.Method != http.MethodGet || len(r.Header.Values("Range")) != 1 || !strings.HasPrefix(r.Header.Get("Range"), "bytes=") || strings.Contains(r.Header.Get("Range"), ",") {
		return false
	}
	for _, name := range []string{"If-Range", "If-Match", "If-Unmodified-Since", "If-None-Match", "If-Modified-Since"} {
		if hasHeader(r.Header, name) {
			return false
		}
	}
	if value := r.Header.Get("Accept-Encoding"); value != "" && value != "identity" {
		return false
	}
	if connectionNominates(r.Header, "Range", "If-Range", "Accept-Encoding") {
		return false
	}
	check := r.Clone(r.Context())
	check.Header.Del("Range")
	check.Header.Del("If-Range")
	return requestCacheable(check)
}

func rangeIdentity(root []byte, origin *proxy.Origin, r *http.Request) ([]byte, string) {
	variant, _ := json.Marshal(struct{ Encoding, Language []string }{r.Header.Values("Accept-Encoding"), r.Header.Values("Accept-Language")})
	return rangeObjectKey(root, origin.Identity(), string(variant), int64(requestRuntime(r).config.RangeBlockBytes)), string(variant)
}
func rangeObjectKey(root []byte, origin, variant string, size int64) []byte {
	return cache.PartialObjectKey(root, origin, variant)
}
func rangeBlockKey(root []byte, m *rangeObject, index int64) []byte { return m.ChunkKey(root, index) }

func (s *Server) readRangeObject(r *http.Request, key []byte, origin, variant string, size int64) (*rangeObject, *cache.CacheHTTPInfo, error) {
	rt := requestRuntime(r)
	release, err := rt.rangeReaders.Acquire(r.Context(), time.Duration(rt.config.QueueTimeout)*time.Second)
	if err != nil {
		return nil, nil, err
	}
	defer release()
	return s.readRangeObjectData(key, origin, variant, size)
}

// Caller holds a reader or writer reservation, including bounded fragment scratch.
func (s *Server) readRangeObjectData(key []byte, origin, variant string, size int64) (*rangeObject, *cache.CacheHTTPInfo, error) {
	m, info, err := s.cache.ReadPartialObject(key, origin, variant)
	if err == nil && !strongRangeTag(m.ETag) {
		return nil, nil, errRangeBypass
	}
	return m, info, err
}

// Missing object metadata is unknown, not a new version; fixed ETag block
// validation still protects streaming through a cache smaller than the object.
func (s *Server) checkRangeVersion(root []byte, parent *cache.FillToken, m *rangeObject) error {
	if err := s.cache.CheckFill(parent); err != nil {
		return err
	}
	key := rangeObjectKey(root, m.Origin, m.Variant, m.ChunkBytes)
	current, _, err := s.readRangeObjectData(key, m.Origin, m.Variant, m.ChunkBytes)
	if err == nil && (current.ETag != m.ETag || current.Total != m.Total) {
		return errRangeChanged
	}
	return nil
}

func strongRangeTag(tag string) bool { return validETag(tag) && !strings.HasPrefix(tag, "W/") }
func identityEncoding(h http.Header) bool {
	return len(h.Values("Content-Encoding")) == 0 || (len(h.Values("Content-Encoding")) == 1 && h.Get("Content-Encoding") == "identity")
}

// rangeFill shares the existing bounded coordinator; every reader opens its own
// complete committed object, and detached producers retain their own resources.
func (s *Server) startRangeFill(r *http.Request, key []byte, token *cache.FillToken, prefetch bool, produce func(*fillTask, *http.Request, fillResources) fillResult) (*fillTask, func(), error) {
	resources := s.captureFillResources(r)
	if resources.config.SharedFillPublicStatic == 1 {
		r = resources.proxy.NormalizePublicRequest(r)
	}
	identity := r.Clone(r.Context())
	identity.Header.Del("Range")
	identity.Header.Del("If-Range")
	task, creator, err := s.fills.join(fillIdentity(identity, key, resources.version), token, resources.config)
	if err != nil {
		resources.release()
		return nil, nil, err
	}
	if creator {
		request := withRequestRuntime(r, task.ctx, resources.runtime)
		go func() {
			defer s.fills.finish(task)
			defer resources.release()
			var release func()
			var err error
			if prefetch {
				release, err = resources.writers.TryAcquire(task.ctx)
			} else {
				release, err = resources.writers.Acquire(task.ctx, time.Duration(resources.config.QueueTimeout)*time.Second)
			}
			result := fillResult{err: err}
			if err == nil {
				defer release()
				result = produce(task, request, resources)
			}
			if result.err == nil {
				result.err = task.ctx.Err()
			}
			if result.err == nil {
				result.err = s.cache.CheckFill(task.token)
			}
			task.publish(result)
		}()
	} else {
		resources.release()
	}
	return task, func() { s.fills.leave(task, creator) }, nil
}

func (s *Server) rangeFill(r *http.Request, key []byte, token *cache.FillToken, prefetch bool, produce func(*fillTask, *http.Request, fillResources) fillResult) error {
	task, leave, err := s.startRangeFill(r, key, token, prefetch, produce)
	if err != nil {
		return err
	}
	defer leave()
	select {
	case <-r.Context().Done():
		return r.Context().Err()
	case <-task.done:
	}
	if task.result.err != nil {
		return task.result.err
	}
	return s.cache.CheckFill(task.token)
}

func (s *Server) reserveRangeObject(task *fillTask, key []byte, parent *cache.FillToken, resources fillResources) (*cacheFill, error) {
	ctx, cancel := context.WithCancel(task.ctx)
	vc, err := s.cache.OpenWriteDerivedFill(ctx, parent, key)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("%w: %w", errRangeOwnership, err)
	}
	return &cacheFill{vc: vc, cancel: cancel, timeout: time.Duration(resources.config.CacheWriteTimeout) * time.Second, stats: &s.counters}, nil
}
func (s *Server) writeRangeObject(task *fillTask, fill *cacheFill, info *cache.CacheHTTPInfo, body io.Reader, limit int64) error {
	fill.vc.SetHTTPInfo(info)
	fill.limit = limit

	buf := make([]byte, 32<<10)
	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			fill.write(buf[:n])
			if fill.err != nil {
				return fill.err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if fill.written != limit {
		return io.ErrUnexpectedEOF
	}
	if err := task.ctx.Err(); err != nil {
		return err
	}
	return fill.commit()
}

func (s *Server) ensureRangeObject(r *http.Request, root, key []byte, parent *cache.FillToken, origin *proxy.Origin, variant string) (*rangeObject, *cache.CacheHTTPInfo, bool, error) {
	if m, info, err := s.readRangeObject(r, key, origin.Identity(), variant, int64(requestRuntime(r).config.RangeBlockBytes)); err == nil && requestFresh(r, info, s.now()) {
		return m, info, true, nil
	}
	token, err := s.cache.DeriveFill(parent, key)
	if err != nil {
		return nil, nil, false, err
	}
	err = s.rangeFill(r, key, token, false, func(task *fillTask, request *http.Request, resources fillResources) fillResult {
		previous, metadata, readErr := s.readRangeObject(request, key, origin.Identity(), variant, int64(requestRuntime(r).config.RangeBlockBytes))
		if readErr == nil && requestFresh(request, metadata, s.now()) {
			return fillResult{key: key}
		}
		if !origin.Available() {
			return fillResult{err: errRangeUnavailable}
		}
		fill, err := s.reserveRangeObject(task, key, parent, resources)
		if err != nil {
			return fillResult{err: err}
		}
		defer fill.abort(errors.New("incomplete range object"))
		request.Method = http.MethodHead
		request.Header.Del("Range")
		request.Header.Del("If-Range")
		request.Header.Set("Accept-Encoding", "identity")
		started := s.now()
		resp, headers, err := origin.ProxyWithOriginHeaders(request)
		if err != nil {
			return fillResult{err: err}
		}
		defer resp.RawBody().Close()
		info := cache.NewCacheHTTPInfo()
		info.RequestTime, info.ResponseTime = started, s.now()
		info.RequestMethod, info.RequestURL, info.Status = http.MethodGet, r.URL.String(), resp.StatusCode()
		info.ResponseHeaders = headers
		_, safe := responsePolicy(info, s.now())
		if !safe || resp.StatusCode() != 200 || !strongRangeTag(headers.Get("ETag")) || len(headers.Values("ETag")) != 1 || resp.RawResponse.ContentLength < 0 || !identityEncoding(headers) || len(resp.RawResponse.Trailer) != 0 {
			return fillResult{err: errRangeBypass}
		}
		if !cacheSizeAllowed(resources.config.MaxCacheObjectBytes, key, info, resp.RawResponse.ContentLength) {
			return fillResult{err: errRangeBypass}
		}
		var incarnation [16]byte
		if _, err = rand.Read(incarnation[:]); err != nil {
			return fillResult{err: err}
		}
		m := rangeObject{Version: rangeLayoutVersion, ChunkBytes: int64(resources.config.RangeBlockBytes), Total: resp.RawResponse.ContentLength, ETag: headers.Get("ETag"), Origin: origin.Identity(), Variant: variant, Generation: hex.EncodeToString(incarnation[:])}
		// A strong validator identifies unchanged content across freshness refreshes.
		if readErr == nil && previous.ETag == m.ETag && previous.Total == m.Total {
			m.Generation = previous.Generation
		}
		encoded, err := m.MarshalBinary()
		if err != nil || len(encoded) > 64<<10 {
			return fillResult{err: errRangeBypass}
		}
		info.ResponseHeaders = resp.Header().Clone()
		err = s.writeRangeObject(task, fill, info, bytes.NewReader(encoded), int64(len(encoded)))
		return fillResult{key: key, err: err}
	})
	if err != nil {
		return nil, nil, false, err
	}
	m, info, err := s.readRangeObject(r, key, origin.Identity(), variant, int64(requestRuntime(r).config.RangeBlockBytes))
	return m, info, false, err
}

func rangeBounds(value string, total int64) (int64, int64, error) {
	value = strings.TrimPrefix(value, "bytes=")
	left, right, ok := strings.Cut(value, "-")
	if !ok {
		return 0, 0, errRangeBypass
	}
	decimal := func(value string) (int64, error) {
		if value == "" {
			return 0, errRangeBypass
		}
		for _, v := range value {
			if v < '0' || v > '9' {
				return 0, errRangeBypass
			}
		}
		return strconv.ParseInt(value, 10, 64)
	}
	if left == "" {
		suffix, err := decimal(right)
		if err != nil || suffix <= 0 || total == 0 {
			return 0, 0, errRangeBypass
		}
		return max(int64(0), total-suffix), total - 1, nil
	}
	start, err := decimal(left)
	if err != nil || start >= total {
		return 0, 0, errRangeBypass
	}
	end := total - 1
	if right != "" {
		end, err = decimal(right)
		if err != nil || end < start {
			return 0, 0, errRangeBypass
		}
		end = min(end, total-1)
	}
	return start, end, nil
}

func (s *Server) readRangeBlock(r *http.Request, key []byte, m *rangeObject, index int64, prefetch bool) ([]byte, *cache.CacheHTTPInfo, func(), error) {
	rt := requestRuntime(r)
	var release func()
	var err error
	if prefetch {
		release, err = rt.rangeReaders.TryAcquire(r.Context())
	} else {
		release, err = rt.rangeReaders.Acquire(r.Context(), time.Duration(rt.config.QueueTimeout)*time.Second)
	}
	if err != nil {
		return nil, nil, func() {}, err
	}
	data, info, err := s.readRangeBlockData(key, m, index)
	if err != nil {
		release()
		return nil, nil, func() {}, err
	}
	return data, info, release, nil
}

func (s *Server) readRangeBlockData(key []byte, m *rangeObject, index int64) ([]byte, *cache.CacheHTTPInfo, error) {
	vc, err := s.cache.OpenReadCommitted(key)
	if err != nil {
		return nil, nil, err
	}
	defer vc.Close()
	info, err := vc.LoadHTTPInfo()
	if err != nil {
		return nil, nil, err
	}
	size := min(m.ChunkBytes, m.Total-index*m.ChunkBytes)
	if info.Status != http.StatusPartialContent || info.ResponseHeaders.Get("ETag") != m.ETag {
		return nil, nil, errRangeChanged
	}
	data, err := readExactRange(vc, size)
	if err != nil {
		return nil, nil, err
	}
	if int64(len(data)) != size {
		return nil, nil, io.ErrUnexpectedEOF
	}
	return data, info, nil
}
func (s *Server) rangeBlockFresh(r *http.Request, info *cache.CacheHTTPInfo) bool {
	normal := *info
	normal.Status = http.StatusOK
	normal.ResponseHeaders = info.ResponseHeaders.Clone()
	normal.ResponseHeaders.Del("Content-Range")
	return requestFresh(r, &normal, s.now())
}

func (s *Server) invalidateRange(r *http.Request, root []byte, parent *cache.FillToken, m *rangeObject) {
	key := rangeObjectKey(root, m.Origin, m.Variant, m.ChunkBytes)
	token, err := s.cache.DeriveFill(parent, key)
	if err == nil {
		current, _, err := s.readRangeObject(r, key, m.Origin, m.Variant, m.ChunkBytes)
		if err == nil && current.Generation == m.Generation {
			_ = s.cache.InvalidateFill(token)
		}
	}
	_ = s.cache.InvalidateFill(parent)
}

type rangeSubscription struct {
	body   io.Reader
	close  func()
	wait   func() error
	reused bool
}

func (s *Server) readRangeCoverage(r *http.Request, root []byte, m *rangeObject, segment int64) (cache.PartialCoverage, error) {
	rt := requestRuntime(r)
	release, err := rt.rangeReaders.Acquire(r.Context(), time.Duration(rt.config.QueueTimeout)*time.Second)
	if err != nil {
		return cache.PartialCoverage{}, err
	}
	defer release()
	return s.cache.PartialCoverage(root, m, segment)
}

func (s *Server) rangeFits(root []byte, m *rangeObject, start, end int64) bool {
	required := make(map[*cache.Stripe]int64, 2)
	for index := start / m.ChunkBytes; index <= min(start/m.ChunkBytes+1, end/m.ChunkBytes); index++ {
		stripe := s.cache.KeyToStripe(rangeBlockKey(root, m, index))
		if stripe == nil {
			return false
		}
		required[stripe] += min(m.ChunkBytes, m.Total-index*m.ChunkBytes) + (64 << 10)
		if required[stripe]+(1<<20) > stripe.Skip+stripe.Len-stripe.Start {
			return false
		}
	}

	return true
}
func (s *Server) proxyRangeBypass(w http.ResponseWriter, r *http.Request, origin *proxy.Origin) {
	if !origin.Available() {
		s.upstreamError(w, r, errRangeUnavailable)
		return
	}
	s.cacheStatus(w, "BYPASS")
	resp, _, err := origin.ProxyWithOriginHeaders(r)
	if err != nil {
		s.upstreamError(w, r, err)
		return
	}
	defer resp.RawBody().Close()
	copyHeaders(w.Header(), resp.Header())
	w.WriteHeader(resp.StatusCode())
	if err = s.streamResponse(w, resp.RawBody(), r); err != nil {
		panic(http.ErrAbortHandler)
	}
}
func (s *Server) serveRange(w http.ResponseWriter, r *http.Request, root []byte) {
	s.serveBlocks(w, r, root, false)
}
func (s *Server) serveBlocks(w http.ResponseWriter, r *http.Request, root []byte, full bool) bool {
	resources := s.captureFillResources(r)
	defer resources.release()
	parent, err := s.cache.BeginFill(root, root)
	if err != nil {
		s.proxyWithoutCache(w, r)
		return true
	}
	var origin *proxy.Origin
	var m *rangeObject
	var info *cache.CacheHTTPInfo
	// Prefer a healthy source; an unhealthy source is usable only for a complete
	// fresh interval. Pin one source before sending, never stitch replicas.
	for _, candidate := range resources.proxy.ConfiguredOrigins() {
		key, variant := rangeIdentity(root, candidate, r)
		cached, metadata, readErr := s.readRangeObject(r, key, candidate.Identity(), variant, int64(resources.config.RangeBlockBytes))
		if readErr != nil || !requestFresh(r, metadata, s.now()) {
			continue
		}
		if candidate.Available() {
			origin, m, info = candidate, cached, metadata
			break
		}
		if origin != nil {
			continue
		}
		if value := r.Header.Get("If-Range"); value != "" && (!strongRangeTag(value) || value != cached.ETag) {
			continue
		}
		start, end, boundsErr := requestedBlockBounds(r, cached.Total, full)
		complete := true
		if boundsErr == nil {
			for segment := start / cached.ChunkBytes / cache.PartialSegmentChunks; segment <= end/cached.ChunkBytes/cache.PartialSegmentChunks; segment++ {
				coverage, _ := s.readRangeCoverage(r, root, cached, segment)
				low := max(start/cached.ChunkBytes-segment*cache.PartialSegmentChunks, int64(0))
				high := min(end/cached.ChunkBytes-segment*cache.PartialSegmentChunks, int64(63))
				mask := (^uint64(0) << uint(low)) & (^uint64(0) >> uint(63-high))
				if coverage.Bits&mask != mask {
					complete = false
					break
				}
			}
		}
		if complete {
			origin, m, info = candidate, cached, metadata
		}
	}
	hit := m != nil
	if origin == nil {
		origin, err = resources.proxy.SelectOrigin(r)
		if err != nil {
			s.upstreamError(w, r, err)
			return true
		}
		key, variant := rangeIdentity(root, origin, r)
		m, info, hit, err = s.ensureRangeObject(r, root, key, parent, origin, variant)
	}
	if errors.Is(err, errRangeBypass) {
		if full {
			return false
		}
		s.proxyRangeBypass(w, r, origin)
		return true
	}
	if err != nil {
		if full && !errors.Is(err, errRangeOwnership) && r.Context().Err() == nil && s.cache.CheckFill(parent) == nil {
			return false
		}
		s.upstreamError(w, r, err)
		return true
	}
	if full && m.Total <= max(int64(1<<20), m.ChunkBytes) {
		return false
	}
	if value := r.Header.Get("If-Range"); value != "" && (!strongRangeTag(value) || value != m.ETag) {
		full := r.Clone(r.Context())
		full.Header.Del("Range")
		full.Header.Del("If-Range")
		s.proxyRangeBypass(w, full, origin)
		return true
	}
	start, end, err := requestedBlockBounds(r, m.Total, full)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", m.Total))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return true
	}
	if !s.rangeFits(root, m, start, end) {
		http.Error(w, "Range exceeds cache capacity", http.StatusServiceUnavailable)
		return true
	}
	first := start / m.ChunkBytes
	cursor := &rangeCursor{s: s, request: r, root: root, parent: parent, object: m, origin: origin, last: end / m.ChunkBytes, segment: -1}
	defer cursor.close()
	sub, err := cursor.open(first)
	if errors.Is(err, errRangeBypass) {
		s.proxyRangeBypass(w, r, origin)
		return true
	}
	if err != nil {
		s.upstreamError(w, r, err)
		return true
	}
	defer func() {
		if sub != nil {
			sub.close()
		}
	}()
	blockReused := sub.reused

	hit = hit && blockReused
	if err = s.checkRangeVersion(root, parent, m); err != nil {
		http.Error(w, "Range invalidated", http.StatusServiceUnavailable)
		return true
	}
	copyHeaders(w.Header(), info.ResponseHeaders)
	if full {
		w.Header().Del("Content-Range")
	} else {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, m.Total))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.Header().Set("Accept-Ranges", "bytes")
	// shortcut: require downstream validation until freshness from all blocks is combined.
	w.Header().Set("Cache-Control", "no-cache")
	if policy, ok := responsePolicy(info, s.now()); ok {
		w.Header().Set("Age", strconv.FormatInt(int64(policy.age/time.Second), 10))
	}
	complete := false
	defer func() {
		status := "MISS"
		if complete && hit {
			status = "HIT"
		}
		s.cacheStatus(w, status)
	}()
	if full {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusPartialContent)
	}
	if err = http.NewResponseController(w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		panic(http.ErrAbortHandler)
	}
	for index := first; index <= end/m.ChunkBytes; index++ {
		if index != first {
			sub, err = cursor.open(index)
			if err != nil {
				panic(http.ErrAbortHandler)
			}
		}
		hit = hit && sub.reused
		if s.checkRangeVersion(root, parent, m) != nil {
			panic(http.ErrAbortHandler)
		}
		low := max(start-index*m.ChunkBytes, int64(0))
		high := min(end-index*m.ChunkBytes+1, min(m.ChunkBytes, m.Total-index*m.ChunkBytes))
		if sub.reused {
			s.counters.rangeHits.Add(1)
			s.counters.rangeHitBytes.Add(uint64(high - low))
		} else {
			s.counters.rangeMisses.Add(1)
			s.counters.rangeMissBytes.Add(uint64(high - low))
		}
		if _, err = io.CopyN(io.Discard, sub.body, low); err != nil {
			panic(http.ErrAbortHandler)
		}
		// Hold the last byte until validation so normal client completion cannot cancel the aligned fill.
		limited := &io.LimitedReader{R: sub.body, N: high - low - 1}
		if err = s.streamResponse(w, limited, r); err != nil || limited.N != 0 {
			panic(http.ErrAbortHandler)
		}
		var last [1]byte
		if _, err = io.ReadFull(sub.body, last[:]); err != nil {
			panic(http.ErrAbortHandler)
		}
		if err = sub.wait(); err != nil {
			panic(http.ErrAbortHandler)
		}
		if err = s.streamResponse(w, bytes.NewReader(last[:]), r); err != nil {
			panic(http.ErrAbortHandler)
		}
		sub.close()
		sub = nil
	}

	complete = true
	return true
}

func readExactRange(body io.Reader, size int64) ([]byte, error) {
	data := make([]byte, int(size))
	if _, err := io.ReadFull(body, data); err != nil {
		return nil, err
	}
	var extra [1]byte
	n, err := body.Read(extra[:])
	if n != 0 {
		return nil, errors.New("range body exceeds expected length")
	}
	if err != io.EOF {
		if err == nil {
			err = io.ErrNoProgress
		}
		return nil, err
	}
	return data, nil
}

func requestedBlockBounds(r *http.Request, total int64, full bool) (int64, int64, error) {
	if full && total > 0 {
		return 0, total - 1, nil
	}
	return rangeBounds(r.Header.Get("Range"), total)
}
func blockGETEligible(r *http.Request) bool {
	if hasHeader(r.Header, "Range") || hasHeader(r.Header, "If-Range") {
		return false
	}
	probe := r.Clone(r.Context())
	probe.Header.Set("Range", "bytes=0-")
	return rangeEligible(probe)
}
