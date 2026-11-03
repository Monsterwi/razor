package server

import (
	"context"
	"errors"
	"time"

	"github.com/Monsterwi/razor/cache"
)

func (c *rangeCursor) check() error {
	err := c.s.checkRangeVersion(c.root, c.parent, c.object)
	if c.started && errors.Is(err, cache.ErrPurged) {
		return nil
	}
	return err
}

func (s *Server) reservePartialChunk(task *fillTask, key []byte, parent *cache.FillToken, size int64, resources fillResources) (*cacheFill, error) {
	ctx, cancel := context.WithCancel(task.ctx)
	vc, err := s.cache.OpenWritePartialChunk(ctx, parent, key, size)
	if err != nil {
		cancel()
		return nil, err
	}
	return &cacheFill{vc: vc, cancel: cancel, timeout: time.Duration(resources.config.CacheWriteTimeout) * time.Second, stats: &s.counters}, nil
}

func (s *Server) commitPartialChunk(fill *cacheFill, task *fillTask, parent *cache.FillToken, root []byte, m *rangeObject, index int64, info *cache.CacheHTTPInfo) error {
	writer := fill.vc
	if err := fill.commit(); err != nil {
		return err
	}
	if _, err := writer.CommittedRevision(); err != nil {
		if errors.Is(err, cache.ErrPurged) {
			return nil
		}
		return err
	}
	if err := s.cache.PublishPartialChunk(task.ctx, parent, root, m, index, info); err != nil && !errors.Is(err, cache.ErrPurged) {
		return err
	}
	return nil
}
