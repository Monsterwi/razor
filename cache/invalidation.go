package cache

import (
	"context"
	"errors"
	"sort"
	"strings"
)

var ErrInvalidated = errors.New("cache operation invalidated")

// FillToken must be captured before fetching origin bytes.
type FillToken struct {
	parent        *FillToken
	fenceSelector []byte
	fencePolicy   uint64
	stripe        *Stripe
	key, selector []byte
	generation    uint64
	purgeEpoch    uint64
	stripes       []*Stripe
	generations   []uint64
	purges        []uint64
	snapshot      []fillHead
	rangeSnapshot []fillHead
	wraps         []uint64
	policies      []uint64
	policy        uint64
}

func (c *Cache) BeginFill(key, selector []byte) (*FillToken, error) {
	if err := validateIdentity(key, selector); err != nil {
		return nil, err
	}
	if c.KeyToStripe(key) == nil {
		return nil, errors.New("no stripe found")
	}
	unlock := c.lockStripes()
	defer unlock()
	token := &FillToken{key: append([]byte(nil), key...), selector: append([]byte(nil), selector...), stripes: append([]*Stripe(nil), c.Stripes...), generations: make([]uint64, len(c.Stripes))}
	for i, stripe := range c.Stripes {
		if stripe.closing {
			return nil, ErrCacheClosed
		}
		token.generations[i] = stripe.OpenDir.Generation()
		token.purges = append(token.purges, stripe.OpenDir.purgeEpoch.Load())
		token.wraps = append(token.wraps, stripe.generation)
		token.policies = append(token.policies, stripe.OpenDir.policyEpoch(selector).Load())
	}
	var err error
	token.snapshot, err = c.fillSnapshotLocked(selector)
	if err != nil {
		return nil, err
	}
	token.rangeSnapshot, err = c.rangeSnapshotLocked(selector)
	return token, err
}

// DeriveFill gives each reserved range object an independent selector while
// retaining the original pre-origin URL invalidation fence through publication.
func (c *Cache) DeriveFill(parent *FillToken, key []byte) (*FillToken, error) {
	unlock := c.lockStripes()
	defer unlock()
	return c.deriveFillLocked(parent, key)
}

func (c *Cache) deriveFillLocked(parent *FillToken, key []byte) (*FillToken, error) {
	if parent == nil || parent.parent != nil || !strings.HasPrefix(string(key), string(parent.selector)+"\nrange:") {
		return nil, ErrInvalidated
	}
	if err := validateIdentity(key, key); err != nil {
		return nil, err
	}
	if err := c.checkFillEpochsLocked(parent); err != nil {
		return nil, err
	}
	token := *parent
	token.parent = parent
	token.rangeSnapshot = nil
	token.key, token.selector = append([]byte(nil), key...), append([]byte(nil), key...)
	token.policies, token.wraps = nil, nil
	for _, stripe := range c.Stripes {
		token.policies = append(token.policies, stripe.OpenDir.policyEpoch(key).Load())
		token.wraps = append(token.wraps, stripe.generation)
	}
	var err error
	token.snapshot, err = c.fillSnapshotLocked(key)
	return &token, err
}

func (c *Cache) OpenWriteFill(ctx context.Context, token *FillToken, opts int) (*CacheVC, error) {
	if token == nil {
		return nil, errors.New("invalid cache fill token")
	}
	return c.OpenWriteFillKey(ctx, token, token.key, opts)
}

// OpenWriteFillKey binds a representation chosen after origin headers, preserving
// the generation captured before the origin request on its destination stripe.
func (c *Cache) OpenWriteFillKey(ctx context.Context, token *FillToken, key []byte, opts int) (*CacheVC, error) {
	if token == nil || len(token.stripes) != len(c.Stripes) || len(token.policies) != len(c.Stripes) || len(token.purges) != len(c.Stripes) {
		return nil, errors.New("invalid cache fill token")
	}
	if err := validateIdentity(key, token.selector); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bound := *token
	bound.key = append([]byte(nil), key...)
	bound.stripe = c.KeyToStripe(key)
	for i, stripe := range c.Stripes {
		if token.stripes[i] != stripe {
			return nil, errors.New("foreign cache fill token")
		}
		if stripe == bound.stripe {
			bound.generation = token.generations[i]
			bound.purgeEpoch = token.purges[i]
			bound.policy = token.policies[i]
			if token.parent != nil {
				bound.fenceSelector = token.parent.selector
				bound.fencePolicy = token.parent.policies[i]
			}
		}
	}
	return newCacheVCWrite(ctx, &bound)
}

// KeysForSelector returns all representations across all stripes.
func (c *Cache) KeysForSelector(selector []byte) []string {
	var keys []string
	for _, stripe := range c.Stripes {
		keys = append(keys, stripe.DirMgr.DirTrie.KeysForSelector(selector)...)
	}
	sort.Strings(keys)
	return keys
}

// KeysForSelectorLimit bounds variant enumeration before any metadata reads.
func (c *Cache) KeysForSelectorLimit(selector []byte, limit int) ([]string, error) {
	var keys []string
	for _, s := range c.Stripes {
		next, err := s.DirMgr.DirTrie.KeysForSelectorLimit(selector, limit-len(keys))
		if err != nil {
			return nil, err
		}
		keys = append(keys, next...)
	}
	sort.Strings(keys)
	return keys, nil
}

// PurgeSelector invalidates exact URL selectors or explicit selector prefixes.
func (c *Cache) PurgeSelector(selector []byte, prefix bool) ([]string, error) {
	if err := validateIdentity(selector, selector); err != nil {
		return nil, err
	}
	// Lock in configured order so close cannot publish a clean checkpoint midway.
	unlock := c.lockStripes()
	for _, stripe := range c.Stripes {
		if stripe.closing {
			unlock()
			return nil, ErrCacheClosed
		}
	}
	var purged []string
	var writers []*CacheWriter
	for _, stripe := range c.Stripes {
		stripe.OpenDir.recordPurge(selector, prefix)
		keys := stripe.DirMgr.DirTrie.KeysForSelector(selector)
		if !prefix {
			keys = append(keys, stripe.DirMgr.DirTrie.Get(append(append([]byte(nil), selector...), []byte("\nrange:")...))...)
		}
		if prefix {
			keys = stripe.DirMgr.DirTrie.Get(selector)
		}
		for _, key := range keys {
			entry := stripe.OpenDir.retirePartial([]byte(key))
			stripe.removeDataLocked([]byte(key))
			if entry != nil && entry.Writer.chunkLength == 0 {
				writers = append(writers, entry.Writer)
			}
			purged = append(purged, key)
		}
	}
	unlock()
	for _, writer := range writers {
		writer.Abort(ErrPurged)
	}
	sort.Strings(purged)
	return purged, nil
}

// removeLocked runs under the same barrier as directory and RAM publication.
func (s *Stripe) removeLocked(key []byte) *CacheWriter {
	entry := s.OpenDir.detach(key)
	s.removeDataLocked(key)
	if entry != nil {
		return entry.Writer
	}
	return nil
}

func (s *Stripe) removeDataLocked(key []byte) {
	// A missing head still needs its RAM copy and any active reservation removed.
	s.DirMgr.DirTrie.Delete(key)
	for idx := 0; idx < int(s.DirMgr.FragmentsNum); idx++ {
		fragment := FragmentKey(key, idx)
		hit, _, _ := s.DirMgr.Get(fragment)
		if s.RamCache != nil {
			s.RamCache.Delete(fragment)
		}
		if !hit {
			break
		}
		_, _ = s.DirMgr.Delete(fragment)
	}
}

// reserveIdentityLocked bounds retained key history and keeps active reservations.
func (s *Stripe) reserveIdentityLocked(key, selector []byte) error {
	err := s.DirMgr.DirTrie.SetIdentity(key, selector)
	if !errors.Is(err, ErrIndexFull) {
		return err
	}
	for _, entry := range s.DirMgr.DirTrie.Snapshot() {
		k := []byte(entry.Key)
		if s.OpenDir.Get(k) != nil {
			continue
		}
		hit, _, dir := s.DirMgr.Get(k)
		if !hit || !s.dirValid(dir) {
			s.DirMgr.DirTrie.Delete(k)
		}
	}
	return s.DirMgr.DirTrie.SetIdentity(key, selector)
}

// abortOpenWriters is called after Close shuts admission, outside Stripe.mu.
func (s *Stripe) abortOpenWriters(err error) {
	s.mu.Lock()
	s.OpenDir.generation.Add(1)
	s.OpenDir.mutex.RLock()
	keys := make([]string, 0, len(s.OpenDir.entries))
	for key := range s.OpenDir.entries {
		keys = append(keys, key)
	}
	s.OpenDir.mutex.RUnlock()
	writers := make([]*CacheWriter, 0, len(keys))
	for _, key := range keys {
		if entry := s.OpenDir.detach([]byte(key)); entry != nil {
			writers = append(writers, entry.Writer)
		}
	}
	s.mu.Unlock()
	for _, writer := range writers {
		writer.Abort(err)
	}
}

// CheckFill reports whether a pre-origin token still belongs to the current
// invalidation epochs. Unlike writer CAS, a completed publication may change heads.
func (c *Cache) CheckFill(token *FillToken) error {
	unlock := c.lockStripes()
	defer unlock()
	return c.checkFillEpochsLocked(token)
}
