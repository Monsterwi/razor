package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/proxy"
)

// Match the existing two-chunk disk admission window. Neither plans nor writer
// pointers grow with the requested interval or object length.
const maxRangeRunChunks = 2

type rangeChunk struct {
	ready, done chan struct{}
	writer      *cache.CacheVC
	err         error
}

type rangeRun struct {
	identity    string
	object      string
	first, last int64
	chunks      [maxRangeRunChunks]rangeChunk
}

// claimRange atomically joins an interval or reserves a non-overlapping run.
// A hit already shortened last before this call; an existing owner also cuts it.
func (g *fillGroup) claimRange(identity, object string, first, last int64, token *cache.FillToken, cfg *config.Config, started bool) (*fillTask, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	private := false
	if err := g.cache.CheckFill(token); err != nil {
		if !started || !errors.Is(err, cache.ErrPurged) {
			return nil, false, err
		}
		private = true
	}
	for _, task := range g.tasks {
		if task.ctx.Err() != nil {
			continue
		}
		if err := g.cache.CheckFill(task.token); err != nil && !(started && errors.Is(err, cache.ErrPurged)) {
			continue
		}
		// Already-started old cursors can share their old interval. New
		// requests have no permission to join a revoked task.
		if p := task.partial; p != nil && p.full && p.key == object {
			p.mu.Lock()
			current := p.index
			p.mu.Unlock()
			if first >= current && first <= p.last {
				if p.identity != identity {
					return nil, false, errRangeOwnership
				}
				return g.attachLocked(task, cfg)
			}
			if current > first && current <= last {
				last = current - 1
			}
		}
		run := task.rangeRun
		if run == nil || run.object != object {
			continue
		}
		for index := run.first; index <= run.last; index++ {
			select {
			case <-run.chunks[index-run.first].done:
				continue // Completed coverage can be evicted and claimed again.
			default:
			}
			if index == first {
				if run.identity != identity {
					return nil, false, errRangeOwnership
				}
				return g.attachLocked(task, cfg)
			}
			if index > first && index <= last {
				last = index - 1
			}
		}
	}
	// An older run may still own its next chunk after this prefix completed.
	// Keep it registered, but never attach a replacement to its completed slot.
	g.rangeSequence++
	id := fmt.Sprintf("range:%s:%d:%d", identity, first, g.rangeSequence)
	var task *fillTask
	var creator bool
	var err error
	if private {
		task, creator, err = g.newTaskLocked(id, token, cfg)
	} else {
		task, creator, err = g.joinLocked(id, token, cfg)
		if started && errors.Is(err, cache.ErrPurged) {
			task, creator, err = g.newTaskLocked(id, token, cfg)
		}
	}
	if err != nil || !creator {
		return task, creator, err
	}
	run := &rangeRun{identity: identity, object: object, first: first, last: last}
	for i := first; i <= last; i++ {
		run.chunks[i-first] = rangeChunk{ready: make(chan struct{}), done: make(chan struct{})}
	}
	task.rangeRun = run
	return task, true, nil
}

type rangeCursor struct {
	s        *Server
	request  *http.Request
	root     []byte
	parent   *cache.FillToken
	object   *rangeObject
	origin   *proxy.Origin
	last     int64
	segment  int64
	coverage cache.PartialCoverage
	task     *fillTask
	leave    func()
	handoff  func()
	started  bool
}

func (c *rangeCursor) close() {
	if c.leave != nil {
		c.leave()
		c.leave, c.task = nil, nil
	}
}

func (c *rangeCursor) open(index int64) (*rangeSubscription, error) {
	sub, err := c.openChunk(index)
	reader := &rangeChunkReader{cursor: c, index: index, sub: sub}
	if err != nil {
		if err = reader.recover(err); err != nil {
			reader.close()
			return nil, err
		}
	}
	return &rangeSubscription{body: reader, close: reader.close, wait: func() error { return reader.sub.wait() }, reused: reader.sub.reused}, nil
}

// A slow subscriber can outlive the disk's rolling allocation. Recover a missing
// active fragment or invalid completed chunk once under the original validator.
// Origin errors and version changes are terminal; already sent bytes are skipped.
// The original cursor retains demand for the following chunk during recovery.
type rangeChunkReader struct {
	cursor, recovery *rangeCursor
	index, offset    int64
	sub              *rangeSubscription
}

func (r *rangeChunkReader) recover(err error) error {
	if r.recovery != nil || (!errors.Is(err, cache.ErrNotResident) && !errors.Is(err, errRangeCachedChunk)) {
		return err
	}
	if r.sub != nil {
		if err := r.sub.wait(); err != nil {
			return err
		}
		r.sub.close()
		r.sub = nil
	}
	c := r.cursor
	if err := c.check(); err != nil {
		return err
	}
	r.recovery = &rangeCursor{s: c.s, request: c.request, root: c.root, parent: c.parent, object: c.object, origin: c.origin, last: r.index, segment: -1, started: c.started}
	r.sub, err = r.recovery.openChunk(r.index)
	if err != nil {
		return err
	}
	_, err = io.CopyN(io.Discard, r.sub.body, r.offset)
	return err
}

func (r *rangeChunkReader) Read(p []byte) (int, error) {
	n, err := r.sub.body.Read(p)
	r.offset += int64(n)
	if !errors.Is(err, cache.ErrNotResident) {
		return n, err
	}
	if err = r.recover(err); err != nil {
		return n, err
	}
	if n != 0 {
		return n, nil
	}
	n, err = r.sub.body.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *rangeChunkReader) close() {
	if r.sub != nil {
		r.sub.close()
		r.sub = nil
	}
	if r.recovery != nil {
		r.recovery.close()
	}
}

func (c *rangeCursor) openChunk(index int64) (*rangeSubscription, error) {
	if c.task != nil && c.task.partial != nil {
		return c.subscribePartial(index)
	}
	if c.task != nil && index >= c.task.rangeRun.first && index <= c.task.rangeRun.last {
		return c.subscribe(index)
	}
	c.close()
	segment := index / cache.PartialSegmentChunks
	if segment != c.segment {
		coverage, err := c.s.readRangeCoverage(c.request, c.root, c.object, segment)
		if c.request.Context().Err() != nil {
			return nil, c.request.Context().Err()
		}
		if err != nil {
			coverage = cache.PartialCoverage{}
		}
		c.segment, c.coverage = segment, coverage
	}
	mask := uint64(1) << uint(index%cache.PartialSegmentChunks)
	if c.coverage.Bits&mask != 0 {
		data, info, release, err := c.s.readRangeBlock(c.request, rangeBlockKey(c.root, c.object, index), c.object, index, false)
		if err == nil && c.s.rangeBlockFresh(c.request, info) {
			return &rangeSubscription{body: bytes.NewReader(data), close: release, wait: func() error { return nil }, reused: true}, nil
		}
		release()
		if err := c.repairCoverage(index, info); err != nil && !(c.started && errors.Is(err, cache.ErrPurged)) {
			return nil, err
		}
		c.coverage.Bits &^= mask
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	last := min(c.last, index+maxRangeRunChunks-1, (segment+1)*cache.PartialSegmentChunks-1)
	for next := index + 1; next <= last; next++ {
		if c.coverage.Bits&(uint64(1)<<uint(next%cache.PartialSegmentChunks)) != 0 {
			last = next - 1
			break
		}
	}
	token, err := c.s.cache.DeriveFill(c.parent, rangeBlockKey(c.root, c.object, index))
	if err != nil {
		if !c.started || !errors.Is(err, cache.ErrPurged) {
			return nil, err
		}
		token = c.parent
	}
	resources := c.s.captureFillResources(c.request)
	request := c.request
	if resources.config.SharedFillPublicStatic == 1 {
		request = resources.proxy.NormalizePublicRequest(request)
	}
	identityRequest := request.Clone(request.Context())
	identityRequest.Header.Del("Range")
	identityRequest.Header.Del("If-Range")
	objectKey := rangeBlockKey(c.root, c.object, 0)
	identity := fillIdentity(identityRequest, objectKey, resources.version)
	task, creator, err := c.s.fills.claimRange(identity, string(objectKey), index, last, token, resources.config, c.started)
	if err != nil {
		resources.release()
		return nil, err
	}
	c.task, c.leave = task, func() { c.s.fills.leave(task, creator) }
	if c.handoff != nil {
		c.handoff()
		c.handoff = nil
	}
	if creator {
		originRequest := withRequestRuntime(request, task.ctx, resources.runtime)
		go func() {
			defer c.s.fills.finish(task)
			defer resources.release()
			release, err := resources.writers.Acquire(task.ctx, time.Duration(resources.config.QueueTimeout)*time.Second)
			if err == nil {
				defer release()
				err = c.s.produceRangeRun(task, originRequest, c.root, c.parent, c.object, c.origin, resources)
			}
			for i := task.rangeRun.first; i <= task.rangeRun.last; i++ {
				chunk := &task.rangeRun.chunks[i-task.rangeRun.first]
				select {
				case <-chunk.done:
				default:
					chunk.err = err
					close(chunk.done)
				}
			}
			task.publish(fillResult{err: err})
		}()
	} else {
		resources.release()
	}
	return c.subscribe(index)
}

func (c *rangeCursor) subscribe(index int64) (*rangeSubscription, error) {
	if c.task.partial != nil {
		return c.subscribePartial(index)
	}
	chunk := &c.task.rangeRun.chunks[index-c.task.rangeRun.first]
	select {
	case <-c.request.Context().Done():
		return nil, c.request.Context().Err()
	case <-chunk.ready:
	case <-chunk.done:
		if chunk.err != nil {
			return nil, chunk.err
		}
	}
	wait := func() error {
		select {
		case <-c.request.Context().Done():
			return c.request.Context().Err()
		case <-chunk.done:
			if chunk.err == nil {
				c.coverage.Bits |= uint64(1) << uint(index%cache.PartialSegmentChunks)
			}
			return chunk.err
		}
	}
	completed := false
	select {
	case <-chunk.done:
		if chunk.err != nil {
			return nil, chunk.err
		}
		completed = true
	default:
	}
	if completed && !(c.started && chunk.writer != nil && errors.Is(c.s.cache.CheckFill(c.parent), cache.ErrPurged)) {
		data, _, release, err := c.s.readRangeBlock(c.request, rangeBlockKey(c.root, c.object, index), c.object, index, false)
		if err == nil {
			return &rangeSubscription{body: bytes.NewReader(data), close: release, wait: wait}, nil
		}
		if !c.started || chunk.writer == nil || !errors.Is(c.s.cache.CheckFill(c.parent), cache.ErrPurged) {
			return nil, completedRangeReadError(err)
		}
		// PURGE may race the committed lookup; the exact bounded writer still
		// holds this response's validated bytes, independent of its old key.
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
	return &rangeSubscription{body: reader, close: func() { reader.Close(); release() }, wait: wait}, nil
}

func (s *Server) produceRangeRun(task *fillTask, request *http.Request, root []byte, parent *cache.FillToken, m *rangeObject, origin *proxy.Origin, resources fillResources) error {
	run := task.rangeRun
	// Recheck after the atomic ownership claim: the original bitmap snapshot
	// may predate another producer's successful publication and retirement.
	coverage, _ := s.cache.PartialCoverage(root, m, run.first/cache.PartialSegmentChunks)
	var cached [maxRangeRunChunks]bool
	for index := run.first; index <= run.last; index++ {
		if coverage.Bits&(uint64(1)<<uint(index%cache.PartialSegmentChunks)) != 0 {
			_, info, err := s.readRangeBlockData(rangeBlockKey(root, m, index), m, index)
			cached[index-run.first] = err == nil && s.rangeBlockFresh(request, info)
		}
	}
	for index := run.first; index <= run.last; {
		if cached[index-run.first] {
			close(run.chunks[index-run.first].done)
			index++
			continue
		}
		last := index
		for last < run.last && !cached[last+1-run.first] {
			last++
		}
		if err := s.fetchRangeRun(task, request, root, parent, m, index, last, origin, resources); err != nil {
			return err
		}
		index = last + 1
	}
	return nil
}

func (s *Server) fetchRangeRun(task *fillTask, request *http.Request, root []byte, parent *cache.FillToken, m *rangeObject, first, last int64, origin *proxy.Origin, resources fillResources) error {
	if !origin.Available() {
		return errRangeUnavailable
	}
	start := first * m.ChunkBytes
	lastStart := last * m.ChunkBytes
	end := lastStart + min(m.ChunkBytes, m.Total-lastStart) - 1
	request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	request.Header.Del("If-Range")
	request.Header.Set("If-Match", m.ETag)
	request.Header.Set("Accept-Encoding", "identity")
	started := s.now()
	resp, headers, err := origin.ProxyWithOriginHeaders(request)
	if err != nil {
		return err
	}
	defer resp.RawBody().Close()
	if resp.StatusCode() != 206 || headers.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", start, end, m.Total) || len(headers.Values("Content-Range")) != 1 || headers.Get("ETag") != m.ETag || len(headers.Values("ETag")) != 1 || resp.RawResponse.ContentLength != end-start+1 || !identityEncoding(headers) || len(resp.RawResponse.Trailer) != 0 {
		s.invalidateRange(request, root, parent, m)
		if resp.StatusCode() == 200 && headers.Get("ETag") == m.ETag && resp.RawResponse.ContentLength == m.Total {
			return errRangeBypass
		}
		return errRangeChanged
	}
	info := cache.NewCacheHTTPInfo()
	info.RequestTime, info.ResponseTime = started, s.now()
	info.RequestMethod, info.RequestURL, info.Status = http.MethodGet, request.URL.String(), http.StatusOK
	info.ResponseHeaders = headers.Clone()
	info.ResponseHeaders.Del("Content-Range")
	if _, safe := responsePolicy(info, s.now()); !safe {
		s.invalidateRange(request, root, parent, m)
		return errRangeBypass
	}
	defer s.fills.endPartialChunk(task)
	for index := first; index <= last; index++ {
		if err := s.fills.beginPartialChunk(task); err != nil {
			return err
		}
		chunkStart := index * m.ChunkBytes
		size := min(m.ChunkBytes, m.Total-chunkStart)
		chunkInfo := *info
		chunkInfo.Status = http.StatusPartialContent
		chunkInfo.ResponseHeaders = resp.Header().Clone()
		chunkInfo.ResponseHeaders.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", chunkStart, chunkStart+size-1, m.Total))
		chunkInfo.ResponseHeaders.Set("Content-Length", strconv.FormatInt(size, 10))
		fill, err := s.reservePartialChunk(task, rangeBlockKey(root, m, index), parent, size, resources)
		if err != nil {
			return err
		}
		defer fill.abort(errors.New("incomplete range run"))
		fill.limit = size
		fill.vc.SetHTTPInfo(&chunkInfo)
		chunk := &task.rangeRun.chunks[index-task.rangeRun.first]
		chunk.writer = fill.vc
		close(chunk.ready)
		buf := make([]byte, 32<<10)
		remaining := size
		var readErr error
		for remaining > 0 {
			var n int
			n, readErr = resp.RawBody().Read(buf[:min(int64(len(buf)), remaining)])
			if n > 0 {
				fill.write(buf[:n])
				remaining -= int64(n)
				if fill.err != nil {
					return fill.err
				}
			}
			if readErr != nil {
				break
			}
		}
		if readErr != nil && readErr != io.EOF {
			return readErr
		}
		if remaining != 0 {
			if readErr == io.EOF {
				readErr = io.ErrUnexpectedEOF
			}
			return readErr
		}
		if index == last {
			var extra [1]byte
			n, terminal := resp.RawBody().Read(extra[:])
			if n != 0 || terminal != io.EOF {
				if terminal == nil {
					terminal = errors.New("range body exceeds expected length")
				}
				return terminal
			}
			if len(resp.RawResponse.Trailer) != 0 {
				s.invalidateRange(request, root, parent, m)
				return errors.New("range trailers cannot be shared")
			}
		}
		if err := task.ctx.Err(); err != nil {
			return err
		}
		if err := s.commitPartialChunk(fill, task, parent, root, m, index, &chunkInfo); err != nil {
			return err
		}
		close(chunk.done)
		s.fills.endPartialChunk(task)
		if readErr != nil && index != last {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}
