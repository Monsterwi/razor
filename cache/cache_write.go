package cache

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// CacheWriter handles writing data to the cache.
// It buffers data and flushes to the AggregateWriteBuffer.
type CacheWriter struct {
	Stripe *Stripe
	Key    []byte

	// Metadata
	Info *CacheHTTPInfo

	// Write buffer
	buffer []byte

	// Streaming State
	frag0Data    []byte // Holds data for Fragment 0 (waiting for TotalLen)
	nextFragIdx  int    // Index of the next fragment to write
	totalWritten int64  // Total bytes written by caller

	// Streaming synchronization
	cond *sync.Cond

	// State
	mutex     sync.RWMutex
	closed    bool
	err       error // Error state if writer failed
	published []struct {
		key        []byte
		allocation allocation
		size       int
	}
	ctx           context.Context
	cancel        context.CancelFunc
	fenceSelector []byte
	fencePolicy   uint64
	generation    uint64
	policy        uint64
	selector      []byte
	version       uint64
	od            *OpenDirEntry // Reference to OpenDir entry
}

func NewCacheWriter(s *Stripe, key []byte) *CacheWriter {
	ctx, cancel := context.WithCancel(context.Background())
	w := &CacheWriter{
		Stripe:     s,
		ctx:        ctx,
		cancel:     cancel,
		generation: s.OpenDir.Generation(),
		policy:     s.OpenDir.policyEpoch(key).Load(),
		selector:   append([]byte(nil), key...),
		Key:        append([]byte(nil), key...),
		buffer:     make([]byte, 0, 4096), // Initial buffer size
	}
	var identity [8]byte
	if _, err := rand.Read(identity[:]); err != nil {
		w.err = fmt.Errorf("cache write identity: %w", err)
		w.closed = true
	} else {
		w.version = binary.BigEndian.Uint64(identity[:])
	}
	w.cond = sync.NewCond(&w.mutex)
	return w
}

// ReadAt implements a simpler interface for readers to get data from the writer.
// It abstracts away the fragment logic from the consumer.
// It returns bytes read, and an error if read failed or EOF.
// Uses read lock for concurrent readers, upgrades to write lock only when waiting.
func (w *CacheWriter) ReadAt(offset int64, p []byte) (int, error) {
	return w.ReadAtContext(context.Background(), offset, p)
}

func (w *CacheWriter) ReadAtContext(ctx context.Context, offset int64, p []byte) (n int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, func() { w.mutex.Lock(); w.cond.Broadcast(); w.mutex.Unlock() })
	defer stop()
	// If request size is 0, return
	if len(p) == 0 {
		return 0, nil
	}

	for {
		// Use read lock for reading data
		w.mutex.RLock()
		if err := ctx.Err(); err != nil {
			w.mutex.RUnlock()
			return 0, err
		}

		if w.err != nil {
			err := w.err
			w.mutex.RUnlock()
			return 0, err
		}
		// Calculate available data based on what's been written
		// w.totalWritten includes buffered data + flushed fragments
		available := w.totalWritten - offset

		if available > 0 {
			// We have data! But where is it?
			// It could be in flushed fragments (RAM/Disk) or in current buffer.

			// Current buffer starts at: w.totalWritten - len(w.buffer)
			bufferStart := w.totalWritten - int64(len(w.buffer))

			if offset >= bufferStart {
				// It's in the buffer!
				bufOffset := offset - bufferStart
				toRead := int64(len(p))
				if toRead > available {
					toRead = available
				}
				copy(p, w.buffer[bufOffset:bufOffset+toRead])
				w.mutex.RUnlock()
				return int(toRead), nil
			}

			// Check frag0Data
			// frag0Data contains the first fragment (offset 0 to len(frag0Data))
			// It is kept in memory until Close().
			if len(w.frag0Data) > 0 {
				if offset < int64(len(w.frag0Data)) {
					// It's in frag0Data!
					toRead := int64(len(p))
					frag0Avail := int64(len(w.frag0Data)) - offset
					if toRead > frag0Avail {
						toRead = frag0Avail
					}
					copy(p, w.frag0Data[offset:offset+toRead])
					w.mutex.RUnlock()
					return int(toRead), nil
				}
			}

			// If we are here, the data is OLDER than current buffer AND not in frag0Data (or offset > frag0Data).
			// It implies it has been flushed to disk/RAM.
			w.mutex.RUnlock()
			return 0, ErrReadFromDisk // Sentinel error
		}

		// No data available yet
		if w.closed {
			err := w.err
			w.mutex.RUnlock()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}

		// Need to wait for more data
		// cond.Wait() requires write lock, so we need to upgrade
		w.mutex.RUnlock()

		// Upgrade to write lock for cond.Wait()
		w.mutex.Lock()
		// Double-check after acquiring write lock
		if err := ctx.Err(); err != nil {
			w.mutex.Unlock()
			return 0, err
		}
		// Write and Close may both run while upgrading the lock. Recheck data
		// and errors at the top before deciding that a closed writer means EOF.
		if w.closed || w.totalWritten > offset || w.err != nil {
			w.mutex.Unlock()
			continue
		}
		// Wait for more data (releases lock, re-acquires on return)
		w.cond.Wait()
		w.mutex.Unlock()
		// Loop back to try reading again with read lock
	}
}

// Abort signals that the write operation failed.
func (w *CacheWriter) Abort(err error) {
	if w.cancel != nil {
		w.cancel()
	}
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.closed {
		return
	}
	w.abortLocked(err)
}

func (w *CacheWriter) abortLocked(err error) {
	if err == nil {
		err = fmt.Errorf("cache write aborted")
	}
	w.err = err
	w.closed = true
	w.buffer = nil
	w.frag0Data = nil
	w.Stripe.mu.Lock()
	for _, fragment := range w.published {
		if !w.Stripe.allocationValid(fragment.allocation, fragment.size) {
			continue
		}
		if w.Stripe.DirMgr != nil && !w.Stripe.DirMgr.deleteAt(fragment.key, fragment.allocation.offset) {
			continue
		}
		if w.Stripe.RamCache != nil {
			w.Stripe.RamCache.Delete(fragment.key)
		}
	}
	if w.Stripe.OpenDir.Get(w.Key) == w.od {
		w.Stripe.DirMgr.DirTrie.Delete(w.Key)
	}
	w.Stripe.mu.Unlock()
	w.published = nil
	// Terminal readability and active-key ownership change under the same writer lock.
	w.Stripe.OpenDir.releaseWriter(w.Key, w.od)
	if w.od != nil {
		w.od.signalReady()
	}
	w.cond.Broadcast()
}

// REFACTOR: direct call to AggBuffer.Add
// Write appends data to the writer.
func (w *CacheWriter) Write(p []byte) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		if w.err != nil {
			return 0, w.err
		}
		return 0, fmt.Errorf("writer closed")
	}

	if err := w.ctx.Err(); err != nil {
		w.abortLocked(err)
		return 0, err
	}
	if int64(len(p)) > w.Stripe.Skip+w.Stripe.Len-w.Stripe.Start-w.totalWritten {
		err := fmt.Errorf("object exceeds stripe data area")
		w.abortLocked(err)
		return 0, err
	}
	// Consume caller buffers one fragment at a time; retained memory is independent
	// of the size of a single Write call.
	for len(p) > 0 {
		take := TargetFragmentSize - len(w.buffer)
		if take > len(p) {
			take = len(p)
		}
		w.buffer = append(w.buffer, p[:take]...)
		w.totalWritten += int64(take)
		n += take
		p = p[take:]
		if err := w.processBuffer(); err != nil {
			w.abortLocked(err)
			return n, err
		}
	}
	w.cond.Broadcast()
	return n, nil
}

// processBuffer checks if we have enough data to write fragments.
func (w *CacheWriter) processBuffer() error {
	chunkSize := TargetFragmentSize

	for len(w.buffer) >= chunkSize {
		// Extract a chunk
		chunk := w.buffer[:chunkSize]

		if w.nextFragIdx == 0 {
			// Special case for Fragment 0:
			// We CANNOT write it yet because we don't know TotalLen (unless we want to update it later, which is hard).
			// We hold it in memory.
			w.frag0Data = make([]byte, len(chunk))
			copy(w.frag0Data, chunk)

			// Advance buffer
			w.buffer = w.buffer[chunkSize:]
			w.nextFragIdx++
		} else {
			// Fragment 1..N: Write immediately
			// We don't need CacheHTTPInfo for these fragments (hlen=0)
			err := w.writeFragment(w.nextFragIdx, chunk, nil)
			if err != nil {
				return err
			}

			// Advance buffer
			w.buffer = w.buffer[chunkSize:]
			w.nextFragIdx++
		}
	}
	return nil
}

// Close finishes the write operation.
// It writes remaining data and finally Fragment 0.
func (w *CacheWriter) Close() (closeErr error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return w.err
	}
	defer func() {
		if closeErr != nil {
			w.abortLocked(closeErr)
		}
	}()

	// Serialize Metadata (CacheHTTPInfo) for Fragment 0
	var infoBytes []byte
	var err error
	if w.Info != nil {
		infoBytes, err = w.Info.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal http info: %v", err)
		}
	}

	// Handle remaining buffer
	if len(w.buffer) > 0 {
		if w.nextFragIdx == 0 {
			// Total data < chunkSize. Frag 0 is everything.
			// We haven't set frag0Data yet.
			w.frag0Data = w.buffer
			w.buffer = nil
			// We will write it below as Frag 0.
		} else {
			// We have a partial last fragment (Frag N)
			err := w.writeFragment(w.nextFragIdx, w.buffer, nil)
			if err != nil {
				return err
			}
			w.nextFragIdx++
			w.buffer = nil
		}
	}

	// Now write Fragment 0 (if we have data OR if it's an empty object)
	// Even empty object needs Frag 0 with Metadata.
	if w.frag0Data == nil && w.totalWritten == 0 {
		w.frag0Data = []byte{}
	}

	if w.frag0Data != nil {
		// Write Fragment 0
		// This is the commit point.
		err := w.writeFragment(0, w.frag0Data, infoBytes)
		if err != nil {
			return err
		}
	}

	w.closed = true
	w.buffer = nil
	w.frag0Data = nil
	w.published = nil
	// Terminal readability and active-key ownership change under the same writer lock.
	w.Stripe.OpenDir.releaseWriter(w.Key, w.od)
	if w.od != nil {
		w.od.signalReady()
	}
	// Notify any remaining readers that we are done (EOF)
	w.cond.Broadcast()

	return nil
}

// writeFragment writes a single fragment to storage.
func (w *CacheWriter) writeFragment(idx int, data []byte, infoBytes []byte) error {
	// Determine keys
	thisKey := FragmentKey(w.Key, idx)

	// Padded keys for Doc struct
	firstKey := docKey(w.Key)
	currentKey := docKey(thisKey)

	// Info is only for Fragment 0
	currentHLen := uint32(len(infoBytes))

	// Calculate size
	docPayloadSize := len(infoBytes) + len(data)
	totalDocSize := DocHeaderSize + docPayloadSize
	approxSize := (totalDocSize + SectorSize - 1) / SectorSize * SectorSize

	doc := &Doc{
		Magic:       DocMagic,
		VMajor:      docIdentityVersion,
		Len:         uint32(totalDocSize),
		TotalLen:    uint64(w.totalWritten), // Final total len (valid for Frag 0 at Close)
		FirstKey:    firstKey,
		Key:         currentKey,
		HLen:        currentHLen,
		SyncSerial:  uint32(w.version >> 32),
		WriteSerial: uint32(w.version),
	}

	// Serialize Doc Struct
	docBytes, err := doc.MarshalBinary()
	if err != nil {
		return err
	}

	// Combine: [DocStruct] + [HTTPInfo] + [Data]
	fullData := make([]byte, 0, totalDocSize)
	fullData = append(fullData, docBytes...)
	if len(infoBytes) > 0 {
		fullData = append(fullData, infoBytes...)
	}
	fullData = append(fullData, data...)

	// Checksum
	doc.Checksum = doc.CalculateChecksum(fullData[DocHeaderSize:])

	// Patch Checksum
	docBytes, _ = doc.MarshalBinary()
	copy(fullData[0:], docBytes)

	a, err := w.Stripe.AggBuffer.addContext(w.ctx, fullData, approxSize, nil)
	if err != nil {
		return err
	}
	if err = w.Stripe.AggBuffer.flushThrough(w.ctx, a.sequence); err != nil {
		return err
	}
	w.Stripe.mu.Lock()
	defer w.Stripe.mu.Unlock()
	if w.Stripe.storageErr != nil {
		return w.Stripe.storageErr
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if w.Stripe.closing {
		return ErrCacheClosed
	}
	if (len(w.fenceSelector) > 0 && w.fencePolicy != w.Stripe.OpenDir.policyEpoch(w.fenceSelector).Load()) || w.generation != w.Stripe.OpenDir.Generation() || w.policy != w.Stripe.OpenDir.policyEpoch(w.selector).Load() || (w.od != nil && w.Stripe.OpenDir.Get(w.Key) != w.od) {
		return ErrInvalidated
	}
	if !w.Stripe.allocationValid(a, len(fullData)) {
		return fmt.Errorf("fragment overwritten before publication")
	}
	if idx == 0 {
		for _, p := range w.published {
			hit, _, d := w.Stripe.DirMgr.Get(p.key)
			if !hit || int64(d.offset()) != p.allocation.offset || d.phase() != (p.allocation.generation%2 != 0) || !w.Stripe.allocationValid(p.allocation, p.size) {
				return fmt.Errorf("object fragment overwritten before commit")
			}
		}
	}
	_, err = w.Stripe.DirMgr.Set(thisKey, Offset(a.offset), approxSize, a.generation%2 != 0)
	if err != nil {
		return err
	}
	w.published = append(w.published, struct {
		key        []byte
		allocation allocation
		size       int
	}{append([]byte(nil), thisKey...), a, len(fullData)})
	if w.Stripe.RamCache != nil {
		w.Stripe.RamCache.Put(thisKey, fullData)
	}
	if idx == 0 && w.Stripe.DirMgr.DirTrie != nil {
		if err := w.Stripe.DirMgr.DirTrie.SetIdentity(w.Key, w.selector); err != nil {
			return err
		}
	}
	return nil
}
