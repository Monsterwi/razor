package cache

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
)

// Cache is the main entry point, managing multiple stripes (volumes).
// It roughly corresponds to Cache.cc / CacheProcessor in ATS.
type Cache struct {
	partialLocks [64]sync.Mutex
	Stripes      []*Stripe
	Scheme       string // e.g. "http"
}

// NewCache creates a new Cache with nStripes.
// In a real app, this would read from storage.config
func NewCache(nStripes int, paths []string, sizes []int64) (*Cache, error) {
	return NewCacheWithOffsets(nStripes, paths, sizes, nil)
}

// NewCacheWithOffsets creates a new Cache with nStripes, supporting offsets for split spans.
func NewCacheWithOffsets(nStripes int, paths []string, sizes []int64, offsets []int64, opts ...StripeOption) (*Cache, error) {
	if len(paths) != len(sizes) || len(paths) < nStripes {
		return nil, fmt.Errorf("mismatch in paths/sizes/stripes")
	}

	c := &Cache{
		Stripes: make([]*Stripe, nStripes),
		Scheme:  "http",
	}

	// Use offsets if provided, otherwise default to 0
	for i := 0; i < nStripes; i++ {
		offset := int64(0)
		if offsets != nil && i < len(offsets) {
			offset = offsets[i]
		}
		c.Stripes[i] = NewStripe(paths[i], sizes[i], offset, opts...)
	}

	return c, nil
}

// NewCacheFromStore creates a Cache from a Store using BuildStripeLayout.
func NewCacheFromStore(store *Store, nStripes int, opts ...StripeOption) (*Cache, error) {
	paths, sizes, offsets, err := store.BuildStripeLayout(nStripes)
	if err != nil {
		return nil, fmt.Errorf("failed to build stripe layout: %w", err)
	}

	return NewCacheWithOffsets(nStripes, paths, sizes, offsets, opts...)
}

func (c *Cache) Init() error {
	for i, s := range c.Stripes {
		// Calculate blocks (assuming 512 sector size for calculation)
		blocks := s.Len / 512
		if err := s.Init(blocks); err != nil {
			for _, opened := range c.Stripes[:i] {
				_ = opened.Close()
			}
			return err
		}
		if err := s.Open(); err != nil {
			for _, opened := range c.Stripes[:i] {
				_ = opened.Close()
			}
			return err
		}
	}
	return nil
}

func (c *Cache) Close() error {
	var errs []error
	for _, s := range c.Stripes {
		errs = append(errs, s.Close())
	}
	return errors.Join(errs...)
}

// SaveAll saves the state of all stripes (Header + Directory) to disk.
// This is useful for graceful shutdown or periodic saves.
func (c *Cache) SaveAll() error {
	for i, s := range c.Stripes {
		if err := s.Save(); err != nil {
			return fmt.Errorf("failed to save stripe %d: %w", i, err)
		}
	}
	return nil
}

// KeyToStripe maps a cache key to a specific stripe.
// ATS uses MD5 hash parts. Here we use a simple modulo on the key.
func (c *Cache) KeyToStripe(key []byte) *Stripe {
	if len(c.Stripes) == 0 {
		return nil
	}
	if len(c.Stripes) == 1 {
		return c.Stripes[0]
	}

	// Simple hash of the key to select stripe
	// ATS uses (key.slice32(2) >> DIR_TAG_WIDTH) % STRIPE_HASH_TABLE_SIZE
	// We'll use CRC32 or similar for simplicity on the whole key
	hash := crc32.ChecksumIEEE(key)
	idx := hash % uint32(len(c.Stripes))
	return c.Stripes[idx]
}

// OpenRead initiates a read operation.
// It returns a CacheVC if successful.
// This matches ATS design where Cache.open_read() returns CacheVC*.
func (c *Cache) OpenRead(key []byte) (*CacheVC, error) {
	stripe := c.KeyToStripe(key)
	if stripe == nil {
		return nil, fmt.Errorf("no stripe found")
	}

	return NewCacheVCRead(stripe, key)
}

// OpenWrite initiates a write operation.
// It returns a CacheVC if successful.
// This matches ATS design where Cache.open_write() returns CacheVC*.
func (c *Cache) OpenWrite(key []byte, opts int) (*CacheVC, error) {
	return c.OpenWriteContext(context.Background(), key, opts)
}

func (c *Cache) OpenWriteContext(ctx context.Context, key []byte, opts int) (*CacheVC, error) {
	token, err := c.BeginFill(key, key)
	if err != nil {
		return nil, err
	}
	return c.OpenWriteFill(ctx, token, opts)
}

// Remove deletes an entry from the cache.
// This corresponds to Cache::remove() in ATS.
// It supports multi-fragment removal automatically.
func (c *Cache) Remove(key []byte) error {
	if len(key) == 0 {
		return errors.New("empty cache key")
	}
	stripe := c.KeyToStripe(key)
	if stripe == nil {
		return errors.New("no stripe found")
	}
	stripe.mu.Lock()
	if stripe.closing {
		stripe.mu.Unlock()
		return ErrCacheClosed
	}
	stripe.OpenDir.generation.Add(1)
	writer := stripe.removeLocked(key)
	stripe.mu.Unlock()
	if writer != nil {
		writer.Abort(ErrInvalidated)
	}
	return nil
}

// Purge preserves the legacy trailing-slash prefix convention.
func (c *Cache) Purge(key []byte) []string {
	if len(key) == 0 {
		return nil
	}
	keys, _ := c.PurgeSelector(key, key[len(key)-1] == '/')
	return keys
}

// FragmentKey generates a key for the Nth fragment.
// idx 0 is the base key.
// firstKey is the First Key (Earliest Key) of the object.
func FragmentKey(firstKey []byte, idx int) []byte {
	if idx == 0 {
		return firstKey
	}

	// Use robust hash combination (aligned with ATS MD5(key, idx))
	h := md5.New()
	h.Write(firstKey)

	idxBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBytes, uint64(idx))
	h.Write(idxBytes)

	return h.Sum(nil)
}

// OpenReadCommitted never attaches to an in-progress writer. Metadata validation
// still happens at LoadHTTPInfo, so callers must treat a load failure as a miss.
func (c *Cache) OpenReadCommitted(key []byte) (*CacheVC, error) {
	stripe := c.KeyToStripe(key)
	if stripe == nil {
		return nil, fmt.Errorf("no stripe found")
	}
	stripe.mu.RLock()
	defer stripe.mu.RUnlock()
	if stripe.closing {
		return nil, ErrCacheClosed
	}
	if stripe.OpenDir.Get(key) != nil {
		return nil, ErrWriteInProgress
	}
	return &CacheVC{Stripe: stripe, Key: append([]byte(nil), key...), generation: stripe.OpenDir.Generation(), reading: true, reader: NewCacheReader(stripe, key)}, nil
}
