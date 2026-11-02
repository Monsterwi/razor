package cache

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"time"
)

// OpenDirEntry represents a directory entry for a document that is currently being written.
// Ref: trafficserver/src/iocore/cache/P_CacheInternal.h (OpenDirEntry)
type OpenDirEntry struct {
	mutex sync.RWMutex

	// The single active writer for this document (Leader)
	Writer *CacheWriter

	// Broadcast channel: closed when Header is ready or Error occurs.
	// Readers wait on this channel.
	ReadySignal chan struct{}
	readyOnce   sync.Once

	// Error state if Writer failed
	Error error

	// Creation time for timeout detection
	CreatedAt time.Time

	// Reference count for active readers (including the writer itself initially)
	RefCount int
}

func NewOpenDirEntry(w *CacheWriter) *OpenDirEntry {
	return &OpenDirEntry{
		Writer:      w,
		ReadySignal: make(chan struct{}),
		CreatedAt:   time.Now(),
		RefCount:    1, // Writer counts as 1 ref
	}
}

// OpenDir manages concurrent access to documents being written.
type OpenDir struct {
	mutex        sync.RWMutex
	entries      map[string]*OpenDirEntry
	generation   atomic.Uint64
	policyEpochs [1024]atomic.Uint64
}

func NewOpenDir() *OpenDir {
	return &OpenDir{
		entries: make(map[string]*OpenDirEntry),
	}
}

// Get returns the entry for a key if it exists.
func (od *OpenDir) Get(key []byte) *OpenDirEntry {
	od.mutex.RLock()
	defer od.mutex.RUnlock()
	return od.entries[string(key)]
}

// Add attempts to add a new entry. Returns the existing entry if conflict occurs.
func (od *OpenDir) Add(key []byte, w *CacheWriter) (*OpenDirEntry, bool) {
	od.mutex.Lock()
	defer od.mutex.Unlock()

	k := string(key)
	if entry, exists := od.entries[k]; exists {
		return entry, false // Conflict (Follower)
	}

	entry := NewOpenDirEntry(w)
	w.od = entry
	od.entries[k] = entry
	return entry, true // Success (Leader)
}

// Remove decrements refcount and removes entry if 0.
func (od *OpenDir) Remove(key []byte) {
	od.mutex.Lock()
	defer od.mutex.Unlock()

	k := string(key)
	entry, ok := od.entries[k]
	if !ok {
		return
	}

	entry.mutex.Lock()
	entry.RefCount--
	shouldDelete := entry.RefCount <= 0
	entry.mutex.Unlock()

	if shouldDelete {
		delete(od.entries, k)
	}
}

// ForceRemove removes an entry regardless of refcount (e.g. timeout cleanup).
func (od *OpenDir) ForceRemove(key []byte) {
	od.mutex.Lock()
	defer od.mutex.Unlock()
	delete(od.entries, string(key))
}

// Acquire returns an entry with a reader reference. Get is only a lookup.
func (od *OpenDir) Acquire(key []byte) *OpenDirEntry {
	od.mutex.Lock()
	defer od.mutex.Unlock()
	entry := od.entries[string(key)]
	if entry != nil {
		entry.mutex.Lock()
		entry.RefCount++
		entry.mutex.Unlock()
	}
	return entry
}

// Release cannot drop a replacement entry after an invalidation.
func (od *OpenDir) Release(key []byte, expected *OpenDirEntry) {
	od.release(key, expected, false)
}

// A terminal writer no longer owns the key. Existing readers retain their
// exact entry and writer independently of the active-writer directory.
func (od *OpenDir) releaseWriter(key []byte, expected *OpenDirEntry) {
	od.release(key, expected, true)
}

func (od *OpenDir) release(key []byte, expected *OpenDirEntry, writerDone bool) {
	if expected == nil {
		return
	}
	od.mutex.Lock()
	defer od.mutex.Unlock()
	expected.mutex.Lock()
	expected.RefCount--
	remove := expected.RefCount <= 0
	expected.mutex.Unlock()
	if (writerDone || remove) && od.entries[string(key)] == expected {
		delete(od.entries, string(key))
	}
}

func (entry *OpenDirEntry) signalReady() { entry.readyOnce.Do(func() { close(entry.ReadySignal) }) }

// Generation fences queued work across invalidation. Purges advance it under Stripe.mu.
func (od *OpenDir) Generation() uint64 { return od.generation.Load() }

func (od *OpenDir) detach(key []byte) *OpenDirEntry {
	od.mutex.Lock()
	defer od.mutex.Unlock()
	entry := od.entries[string(key)]
	delete(od.entries, string(key))
	if entry != nil {
		entry.mutex.Lock()
		entry.Error = ErrInvalidated
		entry.mutex.Unlock()
		entry.signalReady()
		if entry.Writer.cancel != nil {
			entry.Writer.cancel()
		}
	}
	return entry
}

// shortcut: fixed slots bound policy history; hash collisions conservatively
// reject fills for another selector, without cancelling unrelated reader streams.
func (od *OpenDir) policyEpoch(selector []byte) *atomic.Uint64 {
	digest := sha256.Sum256(selector)
	slot := int(digest[0])<<2 | int(digest[1]&3)
	return &od.policyEpochs[slot]
}
