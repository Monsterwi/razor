package cache

import (
	"context"
	"errors"
)

// HTTP families are deliberately small; snapshot work never grows with cache size.
const MaxFillVariants = 64

type fillHead struct {
	stripe  *Stripe
	key     string
	dir     Dir
	present bool
	writer  *OpenDirEntry
}

func (c *Cache) fillSnapshotLocked(selector []byte) ([]fillHead, error) {
	var heads []fillHead
	for _, s := range c.Stripes {
		keys, err := s.DirMgr.DirTrie.KeysForSelectorLimit(selector, MaxFillVariants-len(heads))
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			hit, _, d := s.DirMgr.Get([]byte(key))
			heads = append(heads, fillHead{s, key, d, hit, s.OpenDir.Get([]byte(key))})
		}
	}
	return heads, nil
}

func (c *Cache) rangeSnapshotLocked(selector []byte) ([]fillHead, error) {
	prefix := append(append([]byte(nil), selector...), []byte(PartialObjectPrefix)...)
	var heads []fillHead
	for _, s := range c.Stripes {
		keys, err := s.DirMgr.DirTrie.KeysForPrefixLimit(prefix, MaxFillVariants-len(heads))
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			hit, _, d := s.DirMgr.Get([]byte(key))
			heads = append(heads, fillHead{s, key, d, hit, s.OpenDir.Get([]byte(key))})
		}
	}
	return heads, nil
}

func (c *Cache) lockStripes() func() {
	for _, s := range c.Stripes {
		s.mu.Lock()
	}
	return func() {
		for i := len(c.Stripes) - 1; i >= 0; i-- {
			c.Stripes[i].mu.Unlock()
		}
	}
}

func (c *Cache) checkFillEpochsLocked(token *FillToken) error {
	if token == nil || len(token.stripes) != len(c.Stripes) || len(token.generations) != len(c.Stripes) || len(token.purges) != len(c.Stripes) || len(token.wraps) != len(c.Stripes) || len(token.policies) != len(c.Stripes) {
		return ErrInvalidated
	}
	var purged error
	if token.parent != nil {
		if err := c.checkFillEpochsLocked(token.parent); err != nil {
			if !errors.Is(err, ErrPurged) {
				return err
			}
			purged = err
		}
	}
	for i, s := range c.Stripes {
		if s.storageErr != nil {
			return s.storageErr
		}
		if s.closing {
			return ErrCacheClosed
		}
		if token.stripes[i] != s || token.generations[i] != s.OpenDir.Generation() || token.policies[i] != s.OpenDir.policyEpoch(token.selector).Load() {
			return ErrInvalidated
		}
		if err := s.OpenDir.checkPurge(token.purges[i], token.selector); err != nil {
			purged = err
		}
	}
	return purged
}

func (c *Cache) checkFillLocked(token *FillToken) error {
	if err := c.checkFillEpochsLocked(token); err != nil {
		return err
	}
	for i, s := range c.Stripes {
		if token.wraps[i] != s.generation {
			return ErrInvalidated
		}
	}
	heads, err := c.fillSnapshotLocked(token.selector)
	if err != nil {
		return err
	}
	if len(heads) != len(token.snapshot) {
		return ErrInvalidated
	}
	for i, head := range heads {
		if head != token.snapshot[i] {
			return ErrInvalidated
		}
	}
	return nil
}

// OpenWriteFillKeyReplacing claims an unchanged selector snapshot and removes only
// its listed old representations, atomically with acquiring the destination writer.
func (c *Cache) OpenWriteFillKeyReplacing(ctx context.Context, token *FillToken, key []byte, remove []string) (*CacheVC, error) {
	if token == nil {
		return nil, ErrInvalidated
	}
	if err := validateIdentity(key, token.selector); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock := c.lockStripes()
	defer unlock()
	return c.openWriteFillKeyReplacingLocked(ctx, token, key, remove)
}

// OpenWriteDerivedFill keeps disk wrap from splitting the block snapshot and its
// writer claim, while retaining the original pre-origin URL invalidation fence.
func (c *Cache) OpenWriteDerivedFill(ctx context.Context, parent *FillToken, key []byte) (*CacheVC, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock := c.lockStripes()
	defer unlock()
	token, err := c.deriveFillLocked(parent, key)
	if err != nil {
		return nil, err
	}
	return c.openWriteFillKeyReplacingLocked(ctx, token, key, nil)
}

func (c *Cache) openWriteFillKeyReplacingLocked(ctx context.Context, token *FillToken, key []byte, remove []string) (*CacheVC, error) {
	if err := c.checkFillLocked(token); err != nil {
		return nil, err
	}
	for _, head := range token.snapshot {
		if head.writer != nil {
			return nil, ErrInvalidated
		}
	}
	for _, key := range remove {
		found := false
		for _, head := range token.snapshot {
			if head.key == key {
				found = true
			}
		}
		if !found {
			return nil, errors.New("replacement outside selector")
		}
	}
	bound := *token
	bound.key = append([]byte(nil), key...)
	bound.stripe = c.KeyToStripe(key)
	for i, s := range c.Stripes {
		if s == bound.stripe {
			bound.generation = token.generations[i]
			bound.purgeEpoch = token.purges[i]
			bound.policy = token.policies[i]
			if token.parent != nil {
				bound.fenceSelector = token.parent.selector
				bound.fencePolicy = token.parent.policies[i]
			}
		}
	}
	// Admission can fail without destroying any existing family member.
	vc, err := newCacheVCWriteLocked(ctx, &bound)
	if err != nil {
		return nil, err
	}
	for _, old := range remove {
		if old != string(key) {
			c.KeyToStripe([]byte(old)).removeLocked([]byte(old))
		}
	}
	return vc, nil
}

// InvalidateFill fences unfinished fills and removes only snapshot-owned heads.
// A writer reservation must not prevent fencing, but newer committed heads survive.
func (c *Cache) InvalidateFill(token *FillToken) error {
	return c.invalidateFill(token, false)
}

func (c *Cache) invalidateFill(token *FillToken, unchanged bool) error {
	unlock := c.lockStripes()
	check := c.checkFillEpochsLocked
	if unchanged {
		check = c.checkFillLocked
	}
	if err := check(token); err != nil {
		unlock()
		return err
	}
	heads, err := c.fillSnapshotLocked(token.selector)
	if err != nil {
		unlock()
		return err
	}
	rangeHeads, err := c.rangeSnapshotLocked(token.selector)
	if err != nil {
		unlock()
		return err
	}
	heads = append(heads, rangeHeads...)
	snapshots := append(append([]fillHead(nil), token.snapshot...), token.rangeSnapshot...)
	var writers []*CacheWriter
	for i, s := range c.Stripes {
		s.OpenDir.policyEpoch(token.selector).Add(1)
		for _, head := range heads {
			if head.stripe != s {
				continue
			}
			remove := !head.present && head.writer != nil
			for _, old := range snapshots {
				if head != old {
					continue
				}
				if !head.present {
					remove = true
					break
				}
				generation := token.wraps[i]
				if old.dir.phase() != (generation%2 != 0) {
					if generation == 0 {
						break
					}
					generation--
				}
				// The original head allocation, not its phase alone, prevents offset ABA.
				// Check its first byte: directory sizes round up beyond the allocation.
				remove = s.allocationValid(allocation{offset: int64(old.dir.offset()), generation: generation}, 1)
				break
			}
			if remove {
				if w := s.removeLocked([]byte(head.key)); w != nil {
					writers = append(writers, w)
				}
			}
		}
	}
	unlock()
	for _, w := range writers {
		w.Abort(ErrInvalidated)
	}
	return nil
}

// UpdateHTTPInfo refreshes a loaded, unconsumed committed reader's metadata. It
// writes only the head fragment; unchanged tail allocations remain owned by the
// original object version. A failed CAS never removes a newer representation.
func (c *Cache) UpdateHTTPInfo(ctx context.Context, token *FillToken, source *CacheVC, info *CacheHTTPInfo) error {
	if source == nil || !source.reading || source.closed.Load() || source.reader == nil || info == nil {
		return ErrInvalidated
	}
	encoded, err := info.MarshalBinary()
	if err != nil {
		return err
	}
	if len(encoded) > 64<<10 {
		return errors.New("HTTP metadata limit")
	}
	r := source.reader
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.writer != nil || r.Info == nil || r.fragmentIndex != 0 || r.totalRead != 0 || r.offset != 0 {
		return ErrInvalidated
	}
	cachedDoc, err := r.parseDocFromBuf()
	if err != nil {
		return err
	}
	// Failed refreshes must leave the reader's original fragment header intact.
	doc := *cachedDoc
	s := source.Stripe
	type retained struct {
		key        []byte
		dir        Dir
		allocation allocation
	}
	var tails []retained
	unlock := c.lockStripes()
	if err = c.checkFillLocked(token); err != nil {
		unlock()
		return err
	}
	owned := false
	for _, head := range token.snapshot {
		if head.stripe == s && head.key == string(source.Key) && head.present && head.dir == r.Dir && head.writer == nil {
			owned = true
		}
	}
	if !owned || source.generation != s.OpenDir.Generation() {
		unlock()
		return ErrInvalidated
	}
	originalHead := r.Dir
	head, _, current := s.DirMgr.Get(source.Key)
	if !head || current != r.Dir || !s.dirValid(current) {
		unlock()
		return ErrInvalidated
	}
	fragments := (doc.TotalLen + TargetFragmentSize - 1) / TargetFragmentSize
	if fragments > uint64(s.DirMgr.FragmentsNum) {
		unlock()
		return ErrInvalidated
	}
	for i := uint64(1); i < fragments; i++ {
		key := FragmentKey(source.Key, int(i))
		hit, _, dir := s.DirMgr.Get(key)
		if !hit || !s.dirValid(dir) {
			unlock()
			return ErrInvalidated
		}
		generation := s.generation
		if dir.phase() != s.Phase {
			if generation == 0 {
				unlock()
				return ErrInvalidated
			}
			generation--
		}
		tails = append(tails, retained{key, dir, allocation{offset: int64(dir.offset()), generation: generation}})
	}
	unlock()
	payload := r.buf[DocHeaderSize+int(doc.HLen):]
	doc.HLen = uint32(len(encoded))
	doc.Len = uint32(DocHeaderSize + len(encoded) + len(payload))
	data := make([]byte, doc.Len)
	copy(data[DocHeaderSize:], encoded)
	copy(data[DocHeaderSize+len(encoded):], payload)
	doc.Checksum = doc.CalculateChecksum(data[DocHeaderSize:])
	header, _ := doc.MarshalBinary()
	copy(data, header)
	size := (len(data) + SectorSize - 1) / SectorSize * SectorSize
	allocation, err := s.AggBuffer.addContext(ctx, data, size, nil)
	if err != nil {
		return err
	}
	if err = s.AggBuffer.flushThrough(ctx, allocation.sequence); err != nil {
		return err
	}
	unlock = c.lockStripes()
	defer unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = c.checkFillLocked(token); err != nil {
		return err
	}
	head, _, current = s.DirMgr.Get(source.Key)
	if !head || current != originalHead || source.generation != s.OpenDir.Generation() {
		return ErrInvalidated
	}
	if !s.allocationValid(allocation, len(data)) {
		return ErrInvalidated
	}
	for _, tail := range tails {
		hit, _, dir := s.DirMgr.Get(tail.key)
		if !hit || dir != tail.dir || !s.allocationValid(tail.allocation, int(dir.approxSize())) {
			return ErrInvalidated
		}
	}
	if _, err = s.DirMgr.Set(source.Key, Offset(allocation.offset), size, allocation.generation%2 != 0); err != nil {
		return err
	}
	if s.RamCache != nil {
		s.RamCache.Put(source.Key, data)
	}
	_, _, r.Dir = s.DirMgr.Get(source.Key)
	r.buf, r.Info, source.Info = data, info, info
	r.doc = nil
	return nil
}
