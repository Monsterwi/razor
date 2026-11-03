package server

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
)

// A full origin stream retains only its current writer. Completed chunks live
// in the cache; subscribers already holding a reader own its reference.
type partialGET struct {
	full          bool
	object        *rangeObject
	info          *cache.CacheHTTPInfo
	origin        *proxy.Origin
	identity, key string
	mu            sync.Mutex
	index, last   int64
	chunk         *rangeChunk
	changed       chan struct{}
}

func (s *Server) serveKnownPartialGET(w http.ResponseWriter, r *http.Request, root []byte) bool {
	for _, origin := range requestRuntime(r).proxy.ConfiguredOrigins() {
		key, variant := rangeIdentity(root, origin, r)
		if m, info, err := s.readRangeObject(r, key, origin.Identity(), variant, cache.PartialChunkBytes); err == nil && requestFresh(r, info, s.now()) {
			if m.Total == 0 || s.fills.hasPartialWork(partialDiscoveryKey(root, r), string(rangeBlockKey(root, m, 0))) {
				return s.serveBlocks(w, r, root, true)
			}
			for segment := int64(0); segment*cache.PartialSegmentChunks*m.ChunkBytes < m.Total; segment++ {
				coverage, err := s.readRangeCoverage(r, root, m, segment)
				if err == nil && coverage.Bits != 0 {
					return s.serveBlocks(w, r, root, true)
				}
			}
		}
	}
	return false
}

func (s *Server) fillPartialGET(task *fillTask, request *http.Request, root []byte, origin *proxy.Origin, resp *http.Response, info *cache.CacheHTTPInfo, resources fillResources) error {
	key, variant := rangeIdentity(root, origin, request)
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return err
	}
	m := &rangeObject{Version: cache.PartialFormatVersion, ChunkBytes: cache.PartialChunkBytes, Total: resp.ContentLength, ETag: info.ResponseHeaders.Get("ETag"), Origin: origin.Identity(), Variant: variant, Generation: hex.EncodeToString(generation[:])}
	encoded, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	identityRequest := request.Clone(request.Context())
	identityRequest.Header.Del("Range")
	identityRequest.Header.Del("If-Range")
	p := &partialGET{full: true, info: info, origin: origin, key: string(rangeBlockKey(root, m, 0)), last: (m.Total - 1) / m.ChunkBytes, changed: make(chan struct{})}
	p.identity = fillIdentity(identityRequest, []byte(p.key), resources.version)
	// Reserve the entire stream interval before metadata becomes readable.
	// Consumers wait for partialReady before accessing the metadata snapshot.
	s.fills.mu.Lock()
	task.partial = p
	s.fills.mu.Unlock()
	metadata, err := s.reserveRangeObject(task, key, task.token, resources)
	if err != nil {
		return err
	}
	defer metadata.abort(errors.New("incomplete partial metadata"))
	metadataWriter := metadata.vc
	if err := s.writeRangeObject(task, metadata, info, bytes.NewReader(encoded), int64(len(encoded))); err != nil {
		return err
	}
	m.Revision, err = metadataWriter.CommittedRevision()
	if err != nil {
		return err
	}
	p.object = m
	close(task.partialReady)
	for index := int64(0); index*m.ChunkBytes < m.Total; index++ {
		chunk := &rangeChunk{ready: make(chan struct{}), done: make(chan struct{})}
		p.mu.Lock()
		p.index, p.chunk = index, chunk
		close(p.changed)
		p.changed = make(chan struct{})
		p.mu.Unlock()
		err := s.fillGETChunk(task, request, root, m, index, resp, info, chunk, resources)
		chunk.err = err
		close(chunk.done)
		if err != nil {
			return err
		}
		p.mu.Lock()
		p.index, p.chunk = index+1, nil
		close(p.changed)
		p.changed = make(chan struct{})
		p.mu.Unlock()
	}
	if m.Total == 0 {
		var extra [1]byte
		n, err := resp.Body.Read(extra[:])
		if n != 0 || err != io.EOF {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		if len(resp.Trailer) != 0 {
			s.invalidateRange(request, root, task.token, m)
			return errors.New("origin trailers cannot be shared")
		}
	}
	return nil
}

func (s *Server) fillGETChunk(task *fillTask, request *http.Request, root []byte, m *rangeObject, index int64, resp *http.Response, info *cache.CacheHTTPInfo, chunk *rangeChunk, resources fillResources) error {
	start := index * m.ChunkBytes
	size := min(m.ChunkBytes, m.Total-start)
	chunkInfo := *info
	chunkInfo.Status = http.StatusPartialContent
	chunkInfo.ResponseHeaders = info.ResponseHeaders.Clone()
	chunkInfo.ResponseHeaders.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+size-1, m.Total))
	chunkInfo.ResponseHeaders.Set("Content-Length", strconv.FormatInt(size, 10))
	if err := s.fills.beginPartialChunk(task, index); err != nil {
		return err
	}
	defer s.fills.endPartialChunk(task)
	fill, err := s.reservePartialChunk(task, rangeBlockKey(root, m, index), task.token, size, resources)
	if err != nil {
		return err
	}
	defer fill.abort(errors.New("incomplete GET chunk"))
	fill.limit = size
	fill.vc.SetHTTPInfo(&chunkInfo)
	chunk.writer = fill.vc
	close(chunk.ready)
	buf := make([]byte, 32<<10)
	for remaining := size; remaining > 0; {
		n, err := resp.Body.Read(buf[:min(int64(len(buf)), remaining)])
		if n > 0 {
			fill.write(buf[:n])
			remaining -= int64(n)
			if fill.err != nil {
				return fill.err
			}
		}
		if err != nil {
			if err == io.EOF && remaining == 0 {
				break
			}
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
	if start+size == m.Total {
		var extra [1]byte
		n, err := resp.Body.Read(extra[:])
		if n != 0 || err != io.EOF {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		if len(resp.Trailer) != 0 {
			s.invalidateRange(request, root, task.token, m)
			return errors.New("origin trailers cannot be shared")
		}
	}
	return s.commitPartialChunk(fill, task, task.token, root, m, index, &chunkInfo)
}

func (c *rangeCursor) subscribePartial(index int64) (*rangeSubscription, error) {
	task, p := c.task, c.task.partial
	select {
	case <-c.request.Context().Done():
		return nil, c.request.Context().Err()
	case <-task.partialReady:
	case <-task.done:
		if task.result.err != nil {
			return nil, task.result.err
		}
	}
	var chunk *rangeChunk
	for {
		p.mu.Lock()
		current, active, changed := p.index, p.chunk, p.changed
		p.mu.Unlock()
		if index < current {
			data, _, release, err := c.s.readRangeBlock(c.request, rangeBlockKey(c.root, c.object, index), c.object, index, false)
			if err != nil {
				return nil, completedRangeReadError(err)
			}
			return &rangeSubscription{body: bytes.NewReader(data), close: release, wait: func() error { return nil }}, nil
		}
		if index == current && active != nil {
			chunk = active
			break
		}
		select {
		case <-c.request.Context().Done():
			return nil, c.request.Context().Err()
		case <-changed:
		case <-task.done:
			if task.result.err != nil {
				return nil, task.result.err
			}
		}
	}
	select {
	case <-c.request.Context().Done():
		return nil, c.request.Context().Err()
	case <-chunk.ready:
	case <-chunk.done:
		if chunk.err != nil {
			return nil, chunk.err
		}
	}
	rt := requestRuntime(c.request)
	release, err := rt.rangeReaders.Acquire(c.request.Context(), time.Duration(rt.config.QueueTimeout)*time.Second)
	if err != nil {
		return nil, err
	}
	reader, err := chunk.writer.OpenReader(c.request.Context())
	if err != nil {
		release()
		return nil, err
	}
	wait := func() error {
		select {
		case <-c.request.Context().Done():
			return c.request.Context().Err()
		case <-chunk.done:
			return chunk.err
		}
	}
	return &rangeSubscription{body: reader, close: func() { reader.Close(); release() }, wait: wait}, nil
}

func (s *Server) hasPartialObject(r *http.Request, root []byte) bool {
	for _, origin := range requestRuntime(r).proxy.ConfiguredOrigins() {
		key, variant := rangeIdentity(root, origin, r)
		if _, info, err := s.readRangeObject(r, key, origin.Identity(), variant, cache.PartialChunkBytes); err == nil && requestFresh(r, info, s.now()) {
			return true
		}
	}
	return false
}

// Cold GET and Range claim the same discovery task before any origin I/O.
// A Range-created discovery uses the existing HEAD path, then both readers use
// the same interval planner; a GET-created discovery feeds that planner directly.
func (s *Server) serveRangeDiscovery(w http.ResponseWriter, r *http.Request, root []byte) {
	parent, err := s.cache.BeginFill(root, root)
	if err != nil {
		s.proxyWithoutCache(w, r)
		return
	}
	resources := s.captureFillResources(r)
	request := r
	if resources.config.SharedFillPublicStatic == 1 {
		request = resources.proxy.NormalizePublicRequest(r)
	}
	identity := request.Clone(request.Context())
	identity.Header.Del("Range")
	identity.Header.Del("If-Range")
	task, creator, err := s.fills.joinPartial(fillIdentity(identity, root, resources.version), partialDiscoveryKey(root, request), parent, resources.config)
	if err != nil {
		resources.release()
		s.upstreamError(w, r, err)
		return
	}
	leave := sync.OnceFunc(func() { s.fills.leave(task, creator) })
	defer leave()
	if creator {
		originRequest := withRequestRuntime(request, task.ctx, resources.runtime)
		go func() {
			defer s.fills.finish(task)
			defer resources.release()
			origin, err := resources.proxy.SelectOrigin(originRequest)
			if err != nil {
				task.publish(fillResult{err: err})
				return
			}
			key, variant := rangeIdentity(root, origin, originRequest)
			m, info, _, err := s.ensureRangeObject(originRequest, root, key, parent, origin, variant)
			if err == nil {
				s.fills.mu.Lock()
				task.partial = &partialGET{object: m, info: info, origin: origin}
				s.fills.mu.Unlock()
				close(task.partialReady)
			}
			task.publish(fillResult{err: err})
			if err == nil {
				// Keep discovery ownership until subscribers acquire body demand.
				<-task.ctx.Done()
			}
		}()
	} else {
		resources.release()
	}
	select {
	case <-r.Context().Done():
		return
	case <-task.partialReady:
	case <-task.done:
	case <-task.streamReady:
		// This GET was not eligible for partial storage; preserve the Range request.
		s.proxyWithoutCache(w, r)
		return
	}
	select {
	case <-task.partialReady:
		p := task.partial
		var handoff func()
		if !p.full {
			handoff = leave
		}
		s.servePartialObject(w, r, root, parent, p.object, p.info, p.origin, false, false, task, handoff)
	default:
		if errors.Is(task.result.err, errRangeBypass) || errors.Is(task.result.err, errUnshareable) || task.result.err == nil {
			s.proxyWithoutCache(w, r)
			return
		}
		s.upstreamError(w, r, task.result.err)
	}
}

func partialDiscoveryKey(root []byte, request *http.Request) string {
	return string(representationKey(root, request.Header, []string{"Accept-Encoding", "Accept-Language"}))
}

func (g *fillGroup) joinPartial(id, root string, token *cache.FillToken, cfg *config.Config) (*fillTask, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, task := range g.tasks {
		if cfg.SharedFillPublicStatic == 1 && task.partialRoot == root && task.id != id && task.ctx.Err() == nil && g.cache.CheckFill(task.token) == nil {
			return nil, false, errRangeOwnership
		}
	}
	task, creator, err := g.joinLocked(id, token, cfg)
	if err == nil && creator {
		task.partialRoot = root
	}
	return task, creator, err
}

// Discovery stays registered through the first interval claim. The table is
// bounded by SharedFillMaxTasks, independent of object length.
func (g *fillGroup) hasPartialWork(root, object string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, task := range g.tasks {
		if task.ctx.Err() == nil && g.cache.CheckFill(task.token) == nil &&
			(task.partialRoot == root || task.rangeRun != nil && task.rangeRun.object == object) {
			return true
		}
	}
	return false
}
