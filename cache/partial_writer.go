package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// OpenWritePartialChunk bounds a shared chunk stream and preserves its selected
// bytes after PURGE, while refusing any publication from the revoked parent.
func (c *Cache) OpenWritePartialChunk(ctx context.Context, parent *FillToken, key []byte, length int64) (*CacheVC, error) {
	if parent == nil || parent.parent != nil || length <= 0 || length > PartialChunkBytes || !strings.HasPrefix(string(key), fmt.Sprintf("%s\nrange:chunk:v%d:", parent.selector, PartialFormatVersion)) {
		return nil, errors.New("invalid partial chunk stream")
	}
	if err := validateIdentity(key, key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock := c.lockStripes()
	defer unlock()
	err := c.checkFillEpochsLocked(parent)
	if err == nil {
		token, err := c.deriveFillLocked(parent, key)
		if err != nil {
			return nil, err
		}
		vc, err := c.openWriteFillKeyReplacingLocked(ctx, token, key, nil)
		if err == nil {
			vc.writer.chunkLength = length
			vc.writer.watchStorage()
		}
		return vc, err
	}
	if !errors.Is(err, ErrPurged) {
		return nil, err
	}
	s := c.KeyToStripe(key)
	w := NewCacheWriter(s, key)
	if w.err != nil {
		w.cancel()
		return nil, w.err
	}
	w.cancel()
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.chunkLength = length
	w.fenceSelector = parent.selector
	for i, stripe := range c.Stripes {
		if stripe == s {
			w.generation, w.purgeEpoch, w.fencePolicy = parent.generations[i], parent.purges[i], parent.policies[i]
		}
	}
	w.purged.Store(true)
	w.od = NewOpenDirEntry(w)
	w.watchStorage()
	return &CacheVC{Stripe: s, Key: append([]byte(nil), key...), writing: true, writer: w, entry: w.od, generation: w.generation}, nil
}

func (w *CacheWriter) watchStorage() {
	// A purged stream is absent from OpenDir but must still wake on terminal I/O.
	w.stopStorageWatch = context.AfterFunc(w.Stripe.evacCtx, func() {
		err := w.Stripe.writeError()
		if err == nil {
			err = ErrCacheClosed
		}
		w.Abort(err)
	})
}
