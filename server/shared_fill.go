package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/Monsterwi/razor/cache"
)

func (s *Server) produceFill(task *fillTask, r *http.Request, key []byte, resources fillResources) {
	result := s.fetchFill(task, r, key, resources)
	if errors.Is(result.err, errUnshareable) {
		// Private headers already released only to the creator.
		return
	}
	if result.err == nil {
		result.err = task.ctx.Err()
	}
	if result.err == nil {
		result.err = s.cache.CheckFill(task.token)
		if task.partial != nil && task.partial.full && errors.Is(result.err, cache.ErrPurged) {
			result.err = nil // The original stream completed without cache publication.
		}
	}
	task.publish(result)
}

func (s *Server) fetchFill(task *fillTask, r *http.Request, key []byte, resources fillResources) fillResult {
	// A prior task may have committed between request lookup and atomic creation.
	stored, previous, policy, err := s.lookupCached(key, r)
	if err != nil {
		return fillResult{err: err}
	}
	if stored != nil {
		defer stored.Close()
		if requestPolicyFresh(r, policy, s.now()) {
			return fillResult{key: stored.Key, status: "HIT"}
		}
	}
	release, err := resources.writers.Acquire(task.ctx, time.Duration(resources.config.QueueTimeout)*time.Second)
	if err != nil {
		return fillResult{err: err}
	}
	defer release()
	originRequest := withRequestRuntime(r, task.ctx, resources.runtime)
	if stored != nil {
		originRequest.Header.Del("If-None-Match")
		originRequest.Header.Del("If-Modified-Since")
		if tag := previous.ResponseHeaders.Get("ETag"); validETag(tag) {
			originRequest.Header.Set("If-None-Match", tag)
		} else if modified := previous.ResponseHeaders.Get("Last-Modified"); modified != "" {
			originRequest.Header.Set("If-Modified-Since", modified)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		requested := s.now()
		origin, err := resources.proxy.SelectOrigin(originRequest)
		if err != nil {
			return fillResult{err: err}
		}
		resp, originHeaders, err := origin.ProxyWithOriginHeaders(originRequest)
		if err != nil {
			return fillResult{err: err}
		}
		defer resp.RawBody().Close()
		if attempt > 0 && resp.StatusCode() == http.StatusNotModified {
			return fillResult{err: errors.New("unusable origin validation")}
		}
		info := cache.NewCacheHTTPInfo()
		info.RequestTime, info.ResponseTime = requested, s.now()
		info.RequestMethod, info.RequestURL, info.Status = r.Method, r.URL.String(), resp.StatusCode()
		info.ResponseHeaders = originHeaders
		if stored != nil && resp.StatusCode() == http.StatusNotModified {
			merged, valid := mergeValidation(previous, info, originRequest)
			resp.RawBody().Close()
			if valid && sameVary(previous.ResponseHeaders, merged.ResponseHeaders) {
				merged.ResponseHeaders = mergeResponseHeaders(previous.ResponseHeaders, resp.Header())
				ctx, cancel := context.WithTimeout(task.ctx, time.Duration(resources.config.CacheWriteTimeout)*time.Second)
				err = s.cache.UpdateHTTPInfo(ctx, task.token, stored, merged)
				cancel()
				if err == nil {
					return fillResult{key: stored.Key, status: "REVALIDATE"}
				}
			} else {
				_ = s.cache.InvalidateFill(task.token)
			}
			stored = nil
			originRequest = withRequestRuntime(r, task.ctx, resources.runtime)
			originRequest.Header.Del("If-None-Match")
			originRequest.Header.Del("If-Modified-Since")
			continue
		}
		_, cacheable := responsePolicy(info, s.now())
		if cacheable && blockGETEligible(r) && strongRangeTag(originHeaders.Get("ETag")) && len(originHeaders.Values("ETag")) == 1 && resp.RawResponse.ContentLength >= 0 && identityEncoding(originHeaders) && len(resp.RawResponse.Trailer) == 0 && cacheSizeAllowed(resources.config.MaxCacheObjectBytes, key, info, resp.RawResponse.ContentLength) {
			info.ResponseHeaders = resp.Header().Clone()
			return fillResult{err: s.fillPartialGET(task, r, key, origin, resp.RawResponse, info, resources)}
		}
		fields, _ := varyFields(originHeaders)
		representation := representationKey(key, r.Header, fields)
		info.ResponseHeaders = resp.Header().Clone()
		keys, err := s.cache.KeysForSelectorLimit(key, maxHTTPVariants)
		if err != nil {
			return fillResult{err: err}
		}
		var remove []string
		for _, old := range keys {
			if !sameRepresentationSchema(key, []byte(old), representation) {
				remove = append(remove, old)
			}
		}
		if !cacheable {
			_ = s.cache.InvalidateFill(task.token)
		}
		canAdd := len(keys) < maxHTTPVariants || containsField(keys, string(representation)) || len(remove) > 0
		if !cacheable || !canAdd || len(resp.RawResponse.Trailer) != 0 || !cacheSizeAllowed(resources.config.MaxCacheObjectBytes, representation, info, resp.RawResponse.ContentLength) {
			s.counters.cacheSkipped.Add(1)
			s.privateFill(task, resp.RawResponse)
			return fillResult{err: errUnshareable}
		}
		cacheCtx, cacheCancel := context.WithCancel(task.ctx)
		vc, err := s.cache.OpenWriteFillKeyReplacing(cacheCtx, task.token, representation, remove)
		if err != nil {
			cacheCancel()
			if !errors.Is(err, cache.ErrInvalidated) {
				return fillResult{err: err}
			}
			s.counters.cacheSkipped.Add(1)
			s.privateFill(task, resp.RawResponse)
			return fillResult{err: errUnshareable}
		}
		vc.SetHTTPInfo(info)
		fill := &cacheFill{vc: vc, cancel: cacheCancel, timeout: time.Duration(resources.config.CacheWriteTimeout) * time.Second, limit: int64(resources.config.MaxCacheObjectBytes), stats: &s.counters}
		defer fill.abort(errors.New("cache fill incomplete"))
		task.publishStream(vc)
		buf := make([]byte, 32*1024)
		for {
			n, readErr := resp.RawBody().Read(buf)
			if n > 0 {
				fill.write(buf[:n])
				if fill.err != nil {
					return fillResult{err: fill.err}
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return fillResult{err: readErr}
			}
		}
		if err := resp.RawBody().Close(); err != nil {
			return fillResult{err: err}
		}
		if len(resp.RawResponse.Trailer) != 0 {
			_ = s.cache.InvalidateFill(task.token)
			return fillResult{err: errors.New("origin trailers cannot be shared")}
		}
		if err := task.ctx.Err(); err != nil {
			return fillResult{err: err}
		}
		if err := fill.commit(); err != nil {
			return fillResult{err: err}
		}
		return fillResult{key: representation}
	}
	return fillResult{err: errors.New("unusable origin validation")}
}
