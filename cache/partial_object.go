package cache

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const PartialFormatVersion = 3
const PartialChunkBytes int64 = 1 << 20
const PartialSegmentChunks int64 = 64
const PartialObjectPrefix = "\nrange:object:"

// PartialObject is representation metadata; coverage lives in separate segments.
// Generation identifies a cache lifetime independently of the origin validator.
type PartialObject struct {
	Version                           int
	ChunkBytes                        int64
	Total                             int64
	ETag, Origin, Variant, Generation string
	Revision                          uint64 `json:"-"`
}

func PartialObjectKey(root []byte, origin, variant string) []byte {
	hash := sha256.Sum256([]byte(origin + "\x00" + variant))
	return []byte(fmt.Sprintf("%s%sv%d:%x", root, PartialObjectPrefix, PartialFormatVersion, hash))
}
func (m *PartialObject) identity() [32]byte {
	return sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%s:%d:%d:%d", m.Origin, m.Variant, m.ETag, m.Generation, m.Version, m.ChunkBytes, m.Total)))
}
func (m *PartialObject) ChunkKey(root []byte, index int64) []byte {
	return []byte(fmt.Sprintf("%s\nrange:chunk:v%d:%x:%d", root, PartialFormatVersion, m.identity(), index))
}
func (m *PartialObject) segmentKey(root []byte, index int64) []byte {
	return []byte(fmt.Sprintf("%s\nrange:segment:v%d:%x:%d", root, PartialFormatVersion, m.identity(), index))
}
func (m *PartialObject) MarshalBinary() ([]byte, error) { return json.Marshal(m) }

func (c *Cache) ReadPartialObject(key []byte, origin, variant string) (*PartialObject, *CacheHTTPInfo, error) {
	data, info, revision, err := c.readPartialRecord(key)
	if err != nil {
		return nil, nil, err
	}
	var m PartialObject
	if json.Unmarshal(data, &m) != nil || m.Version != PartialFormatVersion || m.ChunkBytes != PartialChunkBytes || m.Total < 0 || m.Origin != origin || m.Variant != variant || len(m.Generation) != 32 {
		return nil, nil, errors.New("invalid partial object metadata")
	}
	m.Revision = revision
	return &m, info, nil
}
func (c *Cache) readPartialRecord(key []byte) ([]byte, *CacheHTTPInfo, uint64, error) {
	vc, err := c.OpenReadCommitted(key)
	if err != nil {
		return nil, nil, 0, err
	}
	defer vc.Close()
	info, err := vc.LoadHTTPInfo()
	if err != nil {
		return nil, nil, 0, err
	}
	data, err := io.ReadAll(io.LimitReader(vc, (64<<10)+1))
	if err != nil {
		return nil, nil, 0, err
	}
	if len(data) > 64<<10 {
		return nil, nil, 0, errors.New("partial metadata too large")
	}
	return data, info, vc.reader.version, nil
}

type PartialCoverage struct {
	Bits     uint64
	Revision uint64 `json:"-"`
}
type partialSegment struct {
	Version  int
	Identity [32]byte
	Index    int64
	PartialCoverage
}

// Coverage reads one bounded record; no map grows with the number of objects.
func (c *Cache) PartialCoverage(root []byte, m *PartialObject, segment int64) (PartialCoverage, error) {
	key := m.segmentKey(root, segment)
	lock := &c.partialLocks[crc32.ChecksumIEEE(key)%uint32(len(c.partialLocks))]
	lock.Lock()
	defer lock.Unlock()
	return c.readPartialCoverage(key, m, segment)
}

// Hold the segment lock through the read so replacement cannot look like a miss.
func (c *Cache) readPartialCoverage(key []byte, m *PartialObject, segment int64) (PartialCoverage, error) {
	data, _, revision, err := c.readPartialRecord(key)
	if err != nil {
		return PartialCoverage{}, err
	}
	var record partialSegment
	if json.Unmarshal(data, &record) != nil || record.Version != PartialFormatVersion || record.Identity != m.identity() || record.Index != segment || revision == 0 {
		return PartialCoverage{}, errors.New("invalid partial segment metadata")
	}
	record.Revision = revision
	return record.PartialCoverage, nil
}

// PublishPartialChunk is called only after the validated data writer commits.
func (c *Cache) PublishPartialChunk(ctx context.Context, parent *FillToken, root []byte, m *PartialObject, index int64, info *CacheHTTPInfo) error {
	return c.updatePartialCoverage(ctx, parent, root, m, index, info, nil)
}

// RepairPartialChunk cannot erase a publication made since the failed read.
func (c *Cache) RepairPartialChunk(ctx context.Context, parent *FillToken, root []byte, m *PartialObject, index int64, info *CacheHTTPInfo, observed PartialCoverage) error {
	return c.updatePartialCoverage(ctx, parent, root, m, index, info, &observed)
}
func (c *Cache) updatePartialCoverage(ctx context.Context, parent *FillToken, root []byte, m *PartialObject, index int64, info *CacheHTTPInfo, observed *PartialCoverage) error {
	if index < 0 || m.Total <= 0 || index > (m.Total-1)/PartialChunkBytes {
		return errors.New("invalid partial chunk index")
	}
	segment := index / PartialSegmentChunks
	key := m.segmentKey(root, segment)
	lock := &c.partialLocks[crc32.ChecksumIEEE(key)%uint32(len(c.partialLocks))]
	lock.Lock()
	defer lock.Unlock()
	coverage, _ := c.readPartialCoverage(key, m, segment)
	if observed != nil && coverage != *observed {
		return nil
	}
	mask := uint64(1) << uint(index%PartialSegmentChunks)
	if observed == nil {
		coverage.Bits |= mask
	} else {
		coverage.Bits &^= mask
	}
	data, err := json.Marshal(partialSegment{PartialFormatVersion, m.identity(), segment, coverage})
	if err != nil {
		return err
	}
	vc, err := c.OpenWriteDerivedFill(ctx, parent, key)
	if err != nil {
		return err
	}
	defer vc.Abort(errors.New("incomplete partial metadata"))
	vc.SetHTTPInfo(info)
	if _, err = vc.Write(data); err != nil {
		return err
	}
	return vc.Close()
}
