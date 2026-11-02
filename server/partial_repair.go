package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Monsterwi/razor/cache"
	"github.com/Monsterwi/razor/internal/budget"
)

func (c *rangeCursor) repairCoverage(index int64, info *cache.CacheHTTPInfo) error {
	rt := requestRuntime(c.request)
	release, err := rt.writers.Acquire(c.request.Context(), time.Duration(rt.config.QueueTimeout)*time.Second)
	if err != nil {
		return err
	}
	defer release()
	ctx, cancel := context.WithTimeout(c.request.Context(), time.Duration(rt.config.CacheWriteTimeout)*time.Second)
	defer cancel()
	return c.s.cache.RepairPartialChunk(ctx, c.parent, c.root, c.object, index, info, c.coverage)
}

var errRangeCachedChunk = errors.New("completed range chunk unavailable")

// Only completed local reads can become a refill; origin/RWW failures retain
// their original terminal error and cannot trigger a version-change retry.
func completedRangeReadError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, budget.ErrOverloaded) || errors.Is(err, cache.ErrInvalidated) || errors.Is(err, cache.ErrCacheClosed) {
		return err
	}
	return fmt.Errorf("%w: %w", errRangeCachedChunk, err)
}
