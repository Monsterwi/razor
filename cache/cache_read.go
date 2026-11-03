package cache

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// CacheReader handles reading data from the cache.
// It supports reading from memory (RamCache), Write Buffer (RWW), and Disk.
type CacheReader struct {
	ctx    context.Context
	Stripe *Stripe

	Key, FirstKey []byte
	Dir           Dir

	// Metadata
	Info *CacheHTTPInfo

	// State
	offset int64        // Read offset (relative to Data start of CURRENT fragment)
	writer *CacheWriter // If reading from a writer (RWW)

	// Multi-fragment state
	version       uint64
	purgeEpoch    uint64
	versionSet    bool
	fragmentIndex int
	totalRead     int64  // Total payload bytes read across all fragments
	totalLen      uint64 // Total length from Fragment 0's Doc (cached)

	// Internal
	buf     []byte // Buffer for reading data (contains [DocHeader][Info][Data])
	readErr error
	ram     *ramEntry
	doc     *Doc

	mutex sync.Mutex
}

// parseDocFromBuf reuses the header for the current immutable fragment.
func (r *CacheReader) parseDocFromBuf() (*Doc, error) {
	if r.doc != nil {
		return r.doc, nil
	}
	if len(r.buf) < DocHeaderSize {
		return nil, fmt.Errorf("buffer too small for doc header")
	}

	doc := &Doc{}
	if err := doc.UnmarshalBinary(r.buf[:DocHeaderSize]); err != nil {
		return nil, err
	}

	r.doc = doc
	return doc, nil
}

func NewCacheReader(s *Stripe, key []byte) *CacheReader {
	// Initialize with Key. Dir lookup is done lazily or in loadFragment.
	// But per ATS requirement, we should init these if possible.
	// However, usually we need to lock to lookup.
	// Let's copy key.
	k := make([]byte, len(key))
	copy(k, key)
	return &CacheReader{
		Stripe:     s,
		Key:        k,
		FirstKey:   k,
		purgeEpoch: s.OpenDir.purgeEpoch.Load(),
	}
}

// NewCacheReaderFromWriter creates a reader that reads from an active writer (RWW).
func NewCacheReaderFromWriter(s *Stripe, key []byte, w *CacheWriter) *CacheReader {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return &CacheReader{
		Stripe:     s,
		Key:        key,
		FirstKey:   key,
		writer:     w,
		version:    w.version,
		versionSet: true,
		Info:       w.Info, // Share info from writer
	}
}

// LoadHTTPInfo reads the metadata from disk or RamCache if not already loaded.
func (r *CacheReader) LoadHTTPInfo() (*CacheHTTPInfo, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.readErr != nil {
		return nil, r.readErr
	}
	if r.Info != nil {
		return r.Info, nil
	}

	if r.writer != nil {
		r.writer.mutex.RLock()
		defer r.writer.mutex.RUnlock()
		return r.writer.Info, r.writer.err
	}

	if err := r.loadFragment(0); err != nil {
		return nil, err
	}

	// Parse Doc to get HLen
	doc, err := r.parseDocFromBuf()
	if err != nil {
		return nil, err
	}

	if r.ram != nil && r.ram.info != nil && r.ram.checksum == doc.Checksum {
		r.Info = r.ram.info.clone()
		return r.Info, nil
	}

	// Parse Info from r.buf
	// r.buf contains [DocHeader][Info][Data]
	if doc.HLen > 0 {
		infoStart := DocHeaderSize
		infoEnd := DocHeaderSize + int(doc.HLen)
		if len(r.buf) >= infoEnd {
			info := NewCacheHTTPInfo()
			if err := info.UnmarshalBinary(r.buf[infoStart:infoEnd]); err != nil {
				return nil, err
			}
			r.Info = info
		} else {
			return nil, fmt.Errorf("buffer too small for http info")
		}
	} else {
		r.Info = NewCacheHTTPInfo() // Empty
	}

	return r.Info, nil
}

func (r *CacheReader) Read(p []byte) (n int, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.readErr != nil {
		return 0, r.readErr
	}
	defer func() {
		if err != nil && err != io.EOF {
			r.readErr = err
			r.buf = nil
		}
	}()

	// RWW Path (Assumes single fragment in buffer usually, or simpler stream)
	if r.writer != nil {
		return r.readFromWriter(p)
	}

	// Ensure current fragment is loaded
	if len(r.buf) == 0 {
		// Initial load (Fragment 0)
		if err := r.loadFragment(0); err != nil {
			return 0, err
		}
		r.fragmentIndex = 0 // Mark that we've loaded fragment 0

		// Optimization: Check if it's a single fragment object
		// If so, we can avoid complex multi-fragment logic check in loop
		// But our loop handles it naturally via totalLen check.
		// However, we can set a flag or just trust totalLen.
		// Actually, standard Read loop uses totalLen to determine EOF.
		// r.totalLen is set in parseDoc (called by loadFragment).
	}

	totalBytesRead := 0

	for totalBytesRead < len(p) {
		// Ensure current fragment data is available in buffer
		if len(r.buf) == 0 {
			// Check if we have read everything
			if uint64(r.totalRead) >= r.totalLen {
				if totalBytesRead == 0 {
					return 0, io.EOF
				}
				return totalBytesRead, nil
			}

			// Optimization: If the loaded doc (Fragment 0) says it's a single fragment,
			// and we ran out of buffer, it means EOF (or corrupted/truncated file).
			// We don't need to try loading next fragment.
			// But we need to access the Doc. parseDocFromBuf parses from r.buf, which is empty here!
			// We should have cached this state.
			// Since we don't store Doc in CacheReader, we rely on r.totalLen.
			// r.totalLen is authoritative. If we haven't reached it, we MUST load next fragment.
			// If SingleFragment is true, r.totalLen == r.doc.DataLen().
			// So totalLen check above covers it.

			// Load next fragment
			r.fragmentIndex++
			if err := r.loadFragment(r.fragmentIndex); err != nil {
				return totalBytesRead, err
			}
			r.offset = 0 // Reset offset for new fragment
		}

		// Parse Doc to get HLen for data offset calculation
		doc, err := r.parseDocFromBuf()
		if err != nil {
			return totalBytesRead, err
		}

		// Calculate available data in current fragment (from r.buf)
		// Data starts after DocHeader + HLen
		dataStart := int64(DocHeaderSize) + int64(doc.HLen)
		available := int64(len(r.buf)) - dataStart - r.offset

		if available <= 0 {
			// Current fragment exhausted, check if we've read all data before clearing buffer
			if uint64(r.totalRead) >= r.totalLen {
				// All data read, don't clear buffer, just return what we have
				if totalBytesRead == 0 {
					return 0, io.EOF
				}
				return totalBytesRead, nil
			}
			// Need next fragment, clear buffer to trigger reload
			r.buf = nil
			continue
		}

		toRead := int64(len(p) - totalBytesRead)
		if toRead > available {
			toRead = available
		}

		// Perform Memory Copy
		src := r.buf[dataStart+r.offset : dataStart+r.offset+toRead]
		nRead := copy(p[totalBytesRead:], src)

		r.offset += int64(nRead)
		r.totalRead += int64(nRead)
		totalBytesRead += nRead
	}

	return totalBytesRead, nil
}

// loadFragment performs the directory lookup and data load pipeline.
// Logic flow aligned with ATS CacheVC::handleRead.
func (r *CacheReader) loadFragment(idx int) (err error) {
	defer func() {
		if err != nil {
			// An unsuccessful validation must never leave bytes for a later Read.
			r.buf = nil
			r.readErr = err
		}
	}()

	r.ram, r.doc = nil, nil

	// 1. Determine Key
	currentKey := FragmentKey(r.FirstKey, idx)
	r.Key = currentKey
	r.fragmentIndex = idx

	// Lookup and copy the fragment under the same overwrite/publication lock.
	if err := r.handleRead(); err != nil {
		return err
	}

	// 4. Parse Doc
	if err := r.parseDoc(); err != nil {
		return err
	}

	// 5. Evacuation Check
	r.checkEvacuation(r.Dir, r.Key)

	return nil
}

func (r *CacheReader) handleRead() error {
	// The same lock excludes overwrite and publication for RAM and disk reads.
	r.Stripe.mu.RLock()
	defer r.Stripe.mu.RUnlock()
	if r.Stripe.storageErr != nil {
		return r.Stripe.storageErr
	}
	if r.Stripe.closing || r.Stripe.disk == nil {
		return ErrCacheClosed
	}
	if !r.versionSet {
		if err := r.Stripe.OpenDir.checkPurge(r.purgeEpoch, r.FirstKey); err != nil {
			return err
		}
	}
	hit, _, d := r.Stripe.DirMgr.Get(r.Key)
	if !hit {
		return fmt.Errorf("%w: fragment %d missing", ErrNotResident, r.fragmentIndex)
	}
	r.Dir = d
	off := int64(d.offset())
	if off < r.Stripe.Start || off >= r.Stripe.Skip+r.Stripe.Len {
		return fmt.Errorf("invalid fragment directory offset")
	}
	// A retained directory entry may outlive its ring allocation. At this point
	// dirValid can only reject normal wrap or an in-flight overwrite, not a
	// storage failure or malformed offset. Neither proves data corruption.
	if !r.Stripe.dirValid(d) {
		return fmt.Errorf("%w: fragment %d overwritten", ErrNotResident, r.fragmentIndex)
	}

	if r.loadFromRamCache() {
		return nil
	}
	return r.loadFromDisk()
}

func (r *CacheReader) loadFromRamCache() bool {
	if r.Stripe.RamCache == nil {
		return false
	}
	val, ok := r.Stripe.RamCache.getEntry(r.Key)
	if !ok {
		return false
	}
	r.buf, r.ram = val.data, val
	return true
}

func (r *CacheReader) loadFromDisk() error {
	if r.Stripe.disk == nil {
		return ErrCacheClosed
	}
	offset := int64(r.Dir.offset())
	header := make([]byte, DocHeaderSize)
	if _, err := r.Stripe.disk.ReadAt(header, offset); err != nil {
		return err
	}
	var doc Doc
	if err := doc.UnmarshalBinary(header); err != nil {
		return err
	}
	if doc.Len < DocHeaderSize || doc.Len > AggBufferSize || uint64(doc.Len) > r.Dir.approxSize() || int64(doc.Len) > r.Stripe.Skip+r.Stripe.Len-offset {
		return fmt.Errorf("invalid fragment bounds")
	}
	r.buf = make([]byte, int(doc.Len))
	if _, err := r.Stripe.disk.ReadAt(r.buf, offset); err != nil {
		return err
	}
	return nil
}

// parseDoc parses and validates the Doc header from r.buf.
// For Fragment 0, it caches r.totalLen for subsequent fragment reads.
func (r *CacheReader) parseDoc() error {
	doc, err := r.parseDocFromBuf()
	if err != nil {
		return err
	}

	if err := doc.validate(r.buf); err != nil {
		return err
	}
	if doc.Key != docKey(r.Key) || doc.FirstKey != docKey(r.FirstKey) {
		return fmt.Errorf("cache document identity mismatch")
	}
	if doc.TotalLen > uint64(r.Stripe.Skip+r.Stripe.Len-r.Stripe.Start) {
		return fmt.Errorf("cache object exceeds stripe bounds")
	}
	if r.fragmentIndex == 0 {
		if uint64(doc.DataLen()) != min(doc.TotalLen, uint64(TargetFragmentSize)) {
			return fmt.Errorf("invalid head fragment length")
		}
	} else {
		end := uint64(r.fragmentIndex)*TargetFragmentSize + uint64(doc.DataLen())
		if doc.HLen != 0 || doc.DataLen() == 0 || doc.TotalLen != end {
			return fmt.Errorf("invalid fragment chain")
		}
		if r.writer == nil && (end > r.totalLen || uint64(doc.DataLen()) != min(uint64(TargetFragmentSize), r.totalLen-uint64(r.fragmentIndex)*TargetFragmentSize)) {
			return fmt.Errorf("inconsistent fragment chain length")
		}
	}
	version := uint64(doc.SyncSerial)<<32 | uint64(doc.WriteSerial)
	if r.versionSet && r.version != version {
		return fmt.Errorf("cache object replaced during read")
	}
	r.version, r.versionSet = version, true

	// For Fragment 0, cache TotalLen (authoritative source)
	// Later fragments carry their logical end offset, checked above.
	if r.fragmentIndex == 0 {
		r.totalLen = doc.TotalLen
	}

	// Trim buffer to actual length if we read too much (approxSize > real size)
	if int64(len(r.buf)) > int64(doc.Len) {
		r.buf = r.buf[:doc.Len]
	}

	return nil
}

// readFromWriter reads data from an active writer (RWW).
// It uses r.fragmentIndex and r.offset to track progress, similar to standard Read.
func (r *CacheReader) readFromWriter(p []byte) (n int, err error) {
	// Simplified Logic using Writer.ReadAt
	for {
		// Try to read from Writer's memory buffer
		n, err := r.writer.ReadAtContext(r.ctx, r.totalRead, p)
		if err == ErrReadFromDisk {
			// Data is not in buffer (flushed to disk/RAM)
			// Fallback to loading fragment as if we were a normal reader

			// Calculate logical fragment index based on totalRead
			// Assuming standard fragment size logic matches writer's flushing logic
			// Note: We need to handle this carefully.
			// Ideally, we load the specific fragment corresponding to r.totalRead

			// For simplicity in this Refactor Step 1:
			// If we get ErrReadFromDisk, it means we are behind.
			// We temporarily unlock writer to avoid deadlock?
			// But ReadAt already unlocked it.

			// We need to implement the "Read from Disk" logic here for the RWW case.
			// Which is remarkably similar to the standard Read loop!

			// Actually, if we are in RWW mode, we are usually "tailing" the writer.
			// Being far behind is possible.

			// Let's reuse loadFragment logic?
			// Calculate which fragment we are in:
			fragIdx := int(r.totalRead / TargetFragmentSize)
			fragOffset := r.totalRead % TargetFragmentSize

			// Load that fragment
			if err := r.loadFragment(fragIdx); err != nil {
				return 0, err
			}

			// Read from loaded buffer
			// r.buf contains [DocHeader][Info][Data]
			// We need to find Data Start
			doc, err := r.parseDocFromBuf()
			if err != nil {
				return 0, err
			}
			dataStart := int64(DocHeaderSize) + int64(doc.HLen)

			// Check available in loaded fragment
			// Note: r.buf might contain the WHOLE fragment or just part?
			// loadFragment loads what's on disk.
			available := int64(len(r.buf)) - dataStart - fragOffset
			if available <= 0 {
				// This shouldn't happen if Writer said "ReadFromDisk"
				return 0, fmt.Errorf("inconsistent state: writer said disk, but disk empty")
			}

			toRead := int64(len(p))
			if toRead > available {
				toRead = available
			}

			copy(p, r.buf[dataStart+fragOffset:dataStart+fragOffset+toRead])

			r.totalRead += toRead
			return int(toRead), nil
		}

		if err != nil {
			return n, err
		}

		if n > 0 {
			r.totalRead += int64(n)
			return n, nil
		}

		// If n==0 and err==nil, loop (Wait inside ReadAt should handle waiting,
		// but ReadAt currently waits or returns EOF/Err).
		// Our ReadAt implementation waits.
	}
}

func (r *CacheReader) checkEvacuation(d Dir, key []byte) {
	objOff := int64(d.offset())

	// Check if within hit evacuate window (aligned with ATS within_hit_evacuate_window)
	if r.Stripe.withinHitEvacuateWindow(objOff) {
		r.Stripe.EvacMgr.Register(key, objOff, int(d.approxSize()))
	}
}
