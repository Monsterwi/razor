package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func partialFixture(t testing.TB) (*Cache, *PartialObject, []byte, *FillToken, *CacheHTTPInfo) {
	t.Helper()
	c, err := NewCache(1, []string{filepath.Join(t.TempDir(), "cache")}, []int64{32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	root := []byte("http://cache.test/partial")
	m := &PartialObject{Version: PartialFormatVersion, ChunkBytes: PartialChunkBytes, Total: 4 * PartialChunkBytes, ETag: `"one"`, Origin: "http://origin.test", Generation: "0123456789abcdef0123456789abcdef"}
	parent, err := c.BeginFill(root, root)
	if err != nil {
		t.Fatal(err)
	}
	info := NewCacheHTTPInfo()
	info.Status = http.StatusPartialContent
	info.ResponseHeaders.Set("ETag", m.ETag)
	info.ResponseHeaders.Set("Cache-Control", "public,max-age=3600")
	return c, m, root, parent, info
}

func publishPartialFixture(t testing.TB, c *Cache, m *PartialObject, root []byte, parent *FillToken, info *CacheHTTPInfo, index int64) []byte {
	t.Helper()
	data := bytes.Repeat([]byte{byte('a' + index)}, int(PartialChunkBytes))
	writer, err := c.OpenWriteDerivedFill(context.Background(), parent, m.ChunkKey(root, index))
	if err != nil {
		t.Fatal(err)
	}
	writer.SetHTTPInfo(info)
	if _, err = writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = c.PublishPartialChunk(context.Background(), parent, root, m, index, info); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPartialLateRepairPreservesRefilledChunk(t *testing.T) {
	c, m, root, parent, info := partialFixture(t)
	data := publishPartialFixture(t, c, m, root, parent, info, 0)
	observed, err := c.PartialCoverage(root, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	key := m.ChunkKey(root, 0)
	stripe := c.KeyToStripe(key)
	stripe.mu.Lock()
	_, err = stripe.DirMgr.Delete(key)
	stripe.RamCache.Delete(key)
	stripe.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan error, 1)
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(unblock)
	repaired := make(chan error, 1)
	go func() {
		r, err := c.OpenReadCommitted(key)
		if err == nil {
			_, err = io.ReadAll(r)
			r.Close()
		}
		failed <- err
		<-resume
		repaired <- c.RepairPartialChunk(context.Background(), parent, root, m, 0, info, observed)
	}()
	if err := <-failed; !errors.Is(err, ErrNotResident) {
		unblock()
		t.Fatalf("old read did not fail before refill: %v", err)
	}
	publishPartialFixture(t, c, m, root, parent, info, 0)
	current, err := c.PartialCoverage(root, m, 0)
	if err != nil || current.Bits != 1 || current.Revision == observed.Revision {
		unblock()
		t.Fatalf("refill did not replace publication: before=%+v after=%+v err=%v", observed, current, err)
	}
	unblock()
	if err := <-repaired; err != nil {
		t.Fatal(err)
	}
	after, err := c.PartialCoverage(root, m, 0)
	if err != nil || after != current {
		t.Fatalf("late repair erased newer publication: current=%+v after=%+v err=%v", current, after, err)
	}
	body, err := readStorageObject(c, string(key))
	if err != nil || !bytes.Equal(body, data) {
		t.Fatalf("refill data lost: bytes=%d err=%v", len(body), err)
	}
}

func TestPartialConcurrentRepairAndPublicationsPreserveOtherChunks(t *testing.T) {
	c, m, root, parent, info := partialFixture(t)
	publishPartialFixture(t, c, m, root, parent, info, 0)
	observed, err := c.PartialCoverage(root, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		if err := c.RepairPartialChunk(context.Background(), parent, root, m, 0, info, observed); err != nil {
			t.Error(err)
		}
	})
	for index := int64(1); index < 4; index++ {
		workers.Go(func() {
			<-start
			publishPartialFixture(t, c, m, root, parent, info, index)
		})
	}
	close(start)
	workers.Wait()
	coverage, err := c.PartialCoverage(root, m, 0)
	if err != nil || coverage.Bits&14 != 14 {
		t.Fatalf("concurrent completion lost coverage: bits=%b err=%v", coverage.Bits, err)
	}
	for index := int64(1); index < 4; index++ {
		body, err := readStorageObject(c, string(m.ChunkKey(root, index)))
		if err != nil || !bytes.Equal(body, bytes.Repeat([]byte{byte('a' + index)}, int(PartialChunkBytes))) {
			t.Fatalf("chunk %d data lost: bytes=%d err=%v", index, len(body), err)
		}
	}
}

func TestPartialStorageFailureNeverPublishesReadableCoverage(t *testing.T) {
	for _, failure := range []struct {
		name  string
		err   error
		short bool
	}{{"short", io.ErrShortWrite, true}, {"EIO", syscall.EIO, false}, {"ENOSPC", syscall.ENOSPC, false}} {
		for _, failAt := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/write-%d", failure.name, failAt), func(t *testing.T) {
				c, m, root, parent, info := partialFixture(t)
				stripe := c.Stripes[0]
				stripe.disk = &faultDisk{File: stripe.Fd, start: stripe.Start, failAt: failAt, writeErr: failure.err, short: failure.short}
				writer, err := c.OpenWriteDerivedFill(context.Background(), parent, m.ChunkKey(root, 0))
				if err != nil {
					t.Fatal(err)
				}
				writer.SetHTTPInfo(info)
				reader, err := writer.OpenReader(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				data := bytes.Repeat([]byte("s"), int(PartialChunkBytes))
				if _, err = writer.Write(data); err != nil {
					t.Fatal(err)
				}
				prefix := make([]byte, 4096)
				if _, err = io.ReadFull(reader, prefix); err != nil || !bytes.Equal(prefix, data[:len(prefix)]) {
					t.Fatalf("RWW prefix before storage failure: %v", err)
				}
				err = writer.Close()
				if failAt == 2 {
					if err != nil {
						t.Fatal(err)
					}
					err = c.PublishPartialChunk(context.Background(), parent, root, m, 0, info)
				}
				if !errors.Is(err, failure.err) {
					t.Fatalf("publication failure=%v want=%v", err, failure.err)
				}
				if coverage, err := c.PartialCoverage(root, m, 0); err == nil && coverage.Bits != 0 {
					t.Fatalf("failed storage left readable coverage: %+v", coverage)
				}
				if failAt == 1 {
					if n, err := reader.Read(prefix); n != 0 || !errors.Is(err, failure.err) {
						t.Fatalf("failed writer buffer remained readable: n=%d err=%v", n, err)
					}
				}
				if _, err := readStorageObject(c, string(m.ChunkKey(root, 0))); !errors.Is(err, failure.err) {
					t.Fatalf("new reader hid failed storage: %v", err)
				}
			})
		}
	}
}

func TestPartialRepairAfterMissingSegmentCannotEraseRecreatedCoverage(t *testing.T) {
	c, m, root, parent, info := partialFixture(t)
	publishPartialFixture(t, c, m, root, parent, info, 0)
	observed, err := c.PartialCoverage(root, m, 0)
	if err != nil {
		t.Fatal(err)
	}
	key := m.segmentKey(root, 0)
	stripe := c.KeyToStripe(key)
	stripe.mu.Lock()
	_, err = stripe.DirMgr.Delete(key)
	stripe.RamCache.Delete(key)
	stripe.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PartialCoverage(root, m, 0); !errors.Is(err, ErrNotResident) {
		t.Fatalf("missing segment was reconstructed: %v", err)
	}
	publishPartialFixture(t, c, m, root, parent, info, 0)
	if err := c.RepairPartialChunk(context.Background(), parent, root, m, 0, info, observed); err != nil {
		t.Fatal(err)
	}
	coverage, err := c.PartialCoverage(root, m, 0)
	if err != nil || coverage.Bits != 1 || coverage.Revision == observed.Revision {
		t.Fatalf("recreated segment lost its new coverage: %+v err=%v", coverage, err)
	}
}

func BenchmarkPartialCoverage(b *testing.B) {
	for _, operation := range []string{"read", "publish"} {
		b.Run(operation, func(b *testing.B) {
			c, m, root, parent, info := partialFixture(b)
			info.RequestTime, info.ResponseTime = time.Now(), time.Now()
			publishPartialFixture(b, c, m, root, parent, info, 0)
			data, metadata, _, err := c.readPartialRecord(m.segmentKey(root, 0))
			if err != nil {
				b.Fatal(err)
			}
			header, err := metadata.MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if operation == "read" {
					_, err = c.PartialCoverage(root, m, 0)
				} else {
					err = c.PublishPartialChunk(context.Background(), parent, root, m, 0, info)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(data)), "segment-B")
			b.ReportMetric(float64(DocHeaderSize+len(header)+len(data)), "record-B")
		})
	}
}
