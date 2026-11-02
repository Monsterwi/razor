package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/config"
	"github.com/Monsterwi/razor/internal/budget"
	"github.com/Monsterwi/razor/proxy"
)

var errUnshareable = errors.New("origin response is not shareable")

// fillGroup owns pre-origin tasks. OpenDir starts only after a real writer exists.
// Admitted bodies stream through the cache writer; private bodies go only to the creator.
type fillGroup struct {
	mu                                                sync.Mutex
	ctx                                               context.Context
	cache                                             *cache.Cache
	tasks                                             map[string]*fillTask
	producers                                         sync.WaitGroup
	active, waiters                                   int
	joined, completed, unshareable, failed, cancelled uint64
}

type fillTask struct {
	id                                     string
	token                                  *cache.FillToken
	ctx                                    context.Context
	cancel                                 context.CancelFunc
	refs                                   int
	done, creatorGone, privateDone, exited chan struct{}
	private                                chan *http.Response
	streamReady                            chan struct{}
	stream                                 *cache.CacheVC
	result                                 fillResult
}

type fillResult struct {
	key    []byte
	err    error
	status string
}

// captureFillResources is the single ownership seam for detached producers.
// Runtime reload installs a retained snapshot here; never capture a client writer.
type fillResources struct {
	config  *config.Config
	proxy   *proxy.HTTPProxy
	writers *budget.Limiter
	release func()
	version uint64
	runtime *runtimeSnapshot
}

func (s *Server) captureFillResources(r *http.Request) fillResources {
	rt := requestRuntime(r)
	return fillResources{config: rt.config, proxy: rt.proxy, writers: rt.writers, release: s.retainRuntime(rt), version: rt.version, runtime: rt}
}

func fillIdentity(r *http.Request, key []byte, version uint64) string {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peer = r.RemoteAddr
	}
	identity, _ := json.Marshal(struct {
		Key, Method, Host, Peer string
		Headers                 http.Header
		TLS                     bool
		Version                 uint64
	}{string(key), r.Method, r.Host, peer, r.Header, r.TLS != nil, version})
	return fmt.Sprintf("%x", sha256.Sum256(identity))
}

func (g *fillGroup) join(id string, token *cache.FillToken, cfg *config.Config) (*fillTask, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.ctx.Err(); err != nil {
		return nil, false, err
	}
	if task := g.tasks[id]; task != nil {
		if g.cache.CheckFill(task.token) == nil && task.ctx.Err() == nil {
			if task.refs >= cfg.SharedFillMaxWaiters {
				return nil, false, budget.ErrOverloaded
			}
			task.refs++
			g.waiters++
			g.joined++
			return task, false, nil
		}
		delete(g.tasks, id)
	}
	if g.active >= cfg.SharedFillMaxTasks {
		return nil, false, budget.ErrOverloaded
	}
	if err := g.cache.CheckFill(token); err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(g.ctx, time.Duration(cfg.SharedFillTimeout)*time.Second)
	task := &fillTask{id: id, token: token, ctx: ctx, cancel: cancel, refs: 1, done: make(chan struct{}), creatorGone: make(chan struct{}), privateDone: make(chan struct{}), exited: make(chan struct{}), private: make(chan *http.Response), streamReady: make(chan struct{})}
	g.tasks[id] = task
	g.active++
	g.waiters++
	g.producers.Add(1)
	return task, true, nil
}
func (g *fillGroup) leave(task *fillTask, creator bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if creator {
		close(task.creatorGone)
	}
	task.refs--
	g.waiters--
	if task.refs == 0 {
		task.cancel()
	}
}
func (g *fillGroup) finish(task *fillTask) {
	task.cancel()
	g.mu.Lock()
	if g.tasks[task.id] == task {
		delete(g.tasks, task.id)
	}
	g.active--
	switch {
	case task.result.err == nil:
		g.completed++
	case errors.Is(task.result.err, errUnshareable):
		g.unshareable++
	case errors.Is(task.result.err, context.Canceled):
		g.cancelled++
	default:
		g.failed++
	}
	g.mu.Unlock()
	close(task.exited)
	g.producers.Done()
}
func (task *fillTask) publish(result fillResult) { task.result = result; close(task.done) }

func (task *fillTask) publishStream(writer *cache.CacheVC) {
	task.stream = writer
	close(task.streamReady)
}

func (s *Server) serveSharedMiss(w http.ResponseWriter, r *http.Request, key []byte, token *cache.FillToken) {
	status := "MISS"
	defer func() { s.cacheStatus(w, status) }()
	resources := s.captureFillResources(r)
	if resources.config.SharedFillPublicStatic == 1 {
		r = resources.proxy.NormalizePublicRequest(r)
	}
	task, creator, err := s.fills.join(fillIdentity(r, key, resources.version), token, resources.config)
	if err != nil {
		resources.release()
		s.counters.rejected.Add(1)
		http.Error(w, "Cache fill unavailable", http.StatusServiceUnavailable)
		return
	}
	defer s.fills.leave(task, creator)
	if creator {
		originRequest := withRequestRuntime(r, task.ctx, resources.runtime)
		go func() {
			defer s.fills.finish(task)
			defer resources.release()
			s.produceFill(task, originRequest, key, resources)
		}()
	} else {
		resources.release()
	}
	streaming := false
	select {
	case <-r.Context().Done():
		return
	case <-task.streamReady:
		streaming = true
	case <-task.done:
	}
	if !streaming && creator && errors.Is(task.result.err, errUnshareable) {
		status = "BYPASS"
		select {
		case resp := <-task.private:
			defer func() { close(task.privateDone); <-task.exited }()
			copyHeaders(w.Header(), resp.Header)
			if resp.StatusCode == http.StatusOK && clientNotModified(r, resp.Header) {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.WriteHeader(resp.StatusCode)
			if r.Method != http.MethodHead {
				if err := s.streamResponse(w, resp.Body, r); err != nil {
					panic(http.ErrAbortHandler)
				}
			}
		case <-task.ctx.Done():
			http.Error(w, "Origin response unavailable", http.StatusBadGateway)
		case <-r.Context().Done():
		}
		return
	}
	if !streaming && task.result.err != nil {
		status := http.StatusBadGateway
		if errors.Is(task.result.err, errUnshareable) || errors.Is(task.result.err, budget.ErrOverloaded) {
			status = http.StatusServiceUnavailable
		}
		if errors.Is(task.result.err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		if timeout, ok := task.result.err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
			status = http.StatusGatewayTimeout
		}
		http.Error(w, "Cache fill unavailable", status)
		return
	}
	if err := s.cache.CheckFill(task.token); err != nil {
		http.Error(w, "Cache fill invalidated", http.StatusServiceUnavailable)
		return
	}
	var vc *cache.CacheVC
	if streaming {
		vc, err = task.stream.OpenReader(r.Context())
	} else {
		vc, err = s.cache.OpenReadCommitted(task.result.key)
	}
	if err != nil {
		http.Error(w, "Completed object unavailable", http.StatusBadGateway)
		return
	}
	defer vc.Close()
	info, err := vc.LoadHTTPInfo()
	if err != nil {
		http.Error(w, "Completed object unavailable", http.StatusBadGateway)
		return
	}
	fields, valid := varyFields(info.ResponseHeaders)
	policy, safe := responsePolicy(info, s.now())
	if !valid || !safe || string(representationKey(key, r.Header, fields)) != string(vc.Key) {
		http.Error(w, "Completed object unavailable", http.StatusBadGateway)
		return
	}

	if !streaming && task.result.status != "" {
		status = task.result.status
	}
	s.serveCacheHit(w, r, vc, info, policy)
}

func (s *Server) privateFill(task *fillTask, resp *http.Response) {
	task.publish(fillResult{err: errUnshareable})
	select {
	case task.private <- resp:
		select {
		case <-task.privateDone:
		case <-task.creatorGone:
		case <-task.ctx.Done():
		}
	case <-task.creatorGone:
	case <-task.ctx.Done():
	}
}
