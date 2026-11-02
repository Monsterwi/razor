package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

func TestPurgedParentStreamsLaterChunkWithoutJoiningReplacement(t *testing.T) {
	c, m, root, parent, info := partialFixture(t)
	key := m.ChunkKey(root, 1)
	if _, err := c.PurgeSelector(root, false); err != nil {
		t.Fatal(err)
	}
	fresh, err := c.BeginFill(root, root)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := c.OpenWritePartialChunk(context.Background(), fresh, key, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Abort(io.ErrUnexpectedEOF)
	if _, err = newer.Write([]byte("ne")); err != nil {
		t.Fatal(err)
	}
	old, err := c.OpenWritePartialChunk(context.Background(), parent, key, 7)
	if err != nil {
		t.Fatalf("private continuation: %v", err)
	}
	old.SetHTTPInfo(info)
	r, err := old.OpenReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = old.Write([]byte("oldtail")); err != nil {
		t.Fatal(err)
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = old.CommittedRevision(); !errors.Is(err, ErrPurged) {
		t.Fatalf("private revision: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "oldtail" {
		t.Fatalf("private bytes=%q err=%v", got, err)
	}
	if _, err = newer.Write([]byte("w")); err != nil {
		t.Fatalf("old finish stopped replacement: %v", err)
	}
	if err = newer.Close(); err != nil {
		t.Fatal(err)
	}
	assertInvalidationBody(t, c, string(key), "new")
	if err = c.PublishPartialChunk(context.Background(), parent, root, m, 1, info); !errors.Is(err, ErrPurged) {
		t.Fatalf("old coverage: %v", err)
	}
}

func TestPartialStreamCannotBypassLengthOrMetadataFence(t *testing.T) {
	c, m, root, parent, _ := partialFixture(t)
	if _, err := c.PurgeSelector(root, false); err != nil {
		t.Fatal(err)
	}
	for _, length := range []int64{-1, 0, PartialChunkBytes + 1} {
		if w, err := c.OpenWritePartialChunk(context.Background(), parent, m.ChunkKey(root, 0), length); err == nil {
			w.Abort(io.ErrUnexpectedEOF)
			t.Fatalf("admitted invalid length %d", length)
		}
	}
	if w, err := c.OpenWritePartialChunk(context.Background(), parent, PartialObjectKey(root, m.Origin, m.Variant), 3); err == nil {
		w.Abort(io.ErrUnexpectedEOF)
		t.Fatal("metadata used private chunk publication")
	}
	for _, body := range []string{"ab", "abcd"} {
		w, err := c.OpenWritePartialChunk(context.Background(), parent, m.ChunkKey(root, 0), 3)
		if err != nil {
			t.Fatal(err)
		}
		r, err := w.OpenReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := w.Write([]byte(body))
		closeErr := w.Close()
		if writeErr == nil && closeErr == nil {
			t.Fatalf("wrong length %d succeeded", len(body))
		}
		if _, err = io.ReadAll(r); err == nil {
			t.Fatalf("wrong length %d became successful EOF", len(body))
		}
		r.Close()
	}
}

func TestUnsafeInvalidationStillAbortsPurgedPartialStream(t *testing.T) {
	c, m, root, parent, _ := partialFixture(t)
	if _, err := c.PurgeSelector(root, false); err != nil {
		t.Fatal(err)
	}
	w, err := c.OpenWritePartialChunk(context.Background(), parent, m.ChunkKey(root, 0), 3)
	if err != nil {
		t.Fatal(err)
	}
	r, err := w.OpenReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fresh, err := c.BeginFill(root, root)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.InvalidateFill(fresh); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("old")); !errors.Is(err, ErrInvalidated) || errors.Is(err, ErrPurged) {
		t.Fatalf("unsafe write=%v", err)
	}
	if _, err = io.ReadAll(r); !errors.Is(err, ErrInvalidated) || errors.Is(err, ErrPurged) {
		t.Fatalf("unsafe reader=%v", err)
	}
	if next, err := c.OpenWritePartialChunk(context.Background(), parent, m.ChunkKey(root, 1), 3); !errors.Is(err, ErrInvalidated) || errors.Is(err, ErrPurged) {
		if next != nil {
			next.Abort(io.ErrUnexpectedEOF)
		}
		t.Fatalf("unsafe late chunk=%v", err)
	}
}

func TestPartialCloseAndPurgePreserveExactBytesWithoutLatePublication(t *testing.T) {
	c := safetyCache(t)
	body := bytes.Repeat([]byte{'x'}, 4096)
	for i := 0; i < 20; i++ {
		root := []byte(fmt.Sprintf("/race-%d", i))
		key := []byte(fmt.Sprintf("%s\nrange:chunk:v3:race:0", root))
		parent, err := c.BeginFill(root, root)
		if err != nil {
			t.Fatal(err)
		}
		w, err := c.OpenWritePartialChunk(context.Background(), parent, key, int64(len(body)))
		if err != nil {
			t.Fatal(err)
		}
		r, err := w.OpenReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write(body); err != nil {
			t.Fatal(err)
		}
		first := make([]byte, 1)
		if _, err = io.ReadFull(r, first); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; results <- w.Close() }()
		go func() { defer wg.Done(); <-start; _, err := c.PurgeSelector(root, false); results <- err }()
		close(start)
		wg.Wait()
		for j := 0; j < 2; j++ {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		writeStorageObject(t, c, string(key), []byte("new"))
		tail, err := io.ReadAll(r)
		r.Close()
		if err != nil || !bytes.Equal(tail, body[1:]) {
			t.Fatalf("round %d old bytes=%d err=%v", i, len(tail), err)
		}
		assertInvalidationBody(t, c, string(key), "new")
	}
}

func TestPurgeHistoryBoundsUnboundTokensWithoutStoppingKnownOtherWriter(t *testing.T) {
	c := safetyCache(t)
	key := []byte("/unrelated")
	token, err := c.BeginFill(key, key)
	if err != nil {
		t.Fatal(err)
	}
	w, err := c.OpenWriteFill(context.Background(), token, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 65; i++ {
		if _, err := c.PurgeSelector([]byte(fmt.Sprintf("/other-%d", i)), false); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.CheckFill(token); !errors.Is(err, ErrPurged) {
		t.Fatalf("unbounded old token history=%v", err)
	}
	if _, err = w.Write([]byte("keep")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatalf("known unrelated writer stopped: %v", err)
	}
	assertInvalidationBody(t, c, string(key), "keep")
}

func TestUnloadedReaderCannotAdoptPostPurgeReplacement(t *testing.T) {
	c := safetyCache(t)
	key := []byte("/lazy-reader")
	writeStorageObject(t, c, string(key), []byte("old"))
	r, err := c.OpenReadCommitted(key)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = c.PurgeSelector(key, false); err != nil {
		t.Fatal(err)
	}
	writeStorageObject(t, c, string(key), []byte("new"))
	if body, err := io.ReadAll(r); !errors.Is(err, ErrPurged) || len(body) != 0 {
		t.Fatalf("old unbound reader adopted replacement: %q, %v", body, err)
	}
	assertInvalidationBody(t, c, string(key), "new")
}

func TestPurgeRejectsFailedStripeBeforeChangingAnyStripe(t *testing.T) {
	dir := t.TempDir()
	c, err := NewCache(2, []string{filepath.Join(dir, "one"), filepath.Join(dir, "two")}, []int64{32 << 20, 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	key := []byte("/healthy")
	writeStorageObject(t, c, string(key), []byte("keep"))
	var failedKey []byte
	for i := 0; ; i++ {
		failedKey = []byte(fmt.Sprintf("/failed-%d", i))
		if c.KeyToStripe(failedKey) != c.KeyToStripe(key) {
			break
		}
	}
	failed := c.KeyToStripe(failedKey)
	failed.disk = &faultDisk{File: failed.Fd, start: failed.Start, failAt: 1, writeErr: syscall.EIO}
	w, err := c.OpenWrite(failedKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("fail")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("fixture storage failure=%v", err)
	}
	if _, err = c.PurgeSelector(key, false); !errors.Is(err, syscall.EIO) {
		t.Fatalf("failed storage PURGE reported=%v", err)
	}
	assertInvalidationBody(t, c, string(key), "keep")
}

func TestPurgedPartialWriterFinishesExactReaderWithoutRepublishing(t *testing.T) {
	c, m, root, parent, info := partialFixture(t)
	key := m.ChunkKey(root, 0)
	body := bytes.Repeat([]byte{'a'}, int(PartialChunkBytes))
	w, err := c.OpenWritePartialChunk(context.Background(), parent, key, int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	w.SetHTTPInfo(info)
	defer w.Abort(io.ErrUnexpectedEOF)
	r, err := w.OpenReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = w.Write(body[:4096]); err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 4096)
	if _, err = io.ReadFull(r, prefix); err != nil || !bytes.Equal(prefix, body[:4096]) {
		t.Fatalf("prefix: %v", err)
	}
	if _, err = c.PurgeSelector(root, false); err != nil {
		t.Fatal(err)
	}
	fresh, err := c.BeginFill(root, root)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := c.OpenWritePartialChunk(context.Background(), fresh, key, 3)
	if err != nil {
		t.Fatal(err)
	}
	newer.SetHTTPInfo(info)
	if _, err = newer.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err = newer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write(body[4096:]); err != nil {
		t.Fatalf("purged stream write: %v", err)
	}
	if err = w.Close(); err != nil {
		t.Fatalf("purged stream finish: %v", err)
	}
	if _, err = w.CommittedRevision(); !errors.Is(err, ErrPurged) {
		t.Fatalf("private stream reported committed: %v", err)
	}
	tail, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(tail, body[4096:]) {
		t.Fatalf("old tail bytes=%d err=%v", len(tail), err)
	}
	if err = c.PublishPartialChunk(context.Background(), parent, root, m, 0, info); !errors.Is(err, ErrPurged) {
		t.Fatalf("old coverage publication: %v", err)
	}
	assertInvalidationBody(t, c, string(key), "new")
}

func TestPurgeLeavesOtherPrefixFillAndReaderUsable(t *testing.T) {
	c := safetyCache(t)
	target := []byte("/purged/object")
	other := []byte("/other/object")
	targetToken, _ := c.BeginFill(target, target)
	otherToken, _ := c.BeginFill(other, other)
	w, err := c.OpenWriteFill(context.Background(), otherToken, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.SetHTTPInfo(NewCacheHTTPInfo())
	if _, err = w.Write([]byte("old")); err != nil {
		t.Fatal(err)
	}
	r, err := w.OpenReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	prefix := make([]byte, 3)
	if _, err = io.ReadFull(r, prefix); err != nil {
		t.Fatal(err)
	}
	if _, err = c.PurgeSelector([]byte("/purged/"), true); err != nil {
		t.Fatal(err)
	}
	if err = c.CheckFill(targetToken); !errors.Is(err, ErrInvalidated) {
		t.Fatalf("target token escaped purge: %v", err)
	}
	if err = c.CheckFill(otherToken); err != nil {
		t.Fatalf("unrelated fill invalidated: %v", err)
	}
	if _, err = w.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	tail, err := io.ReadAll(r)
	if err != nil || string(tail) != "tail" {
		t.Fatalf("unrelated reader = %q, %v", tail, err)
	}
	assertInvalidationBody(t, c, string(other), "oldtail")
}
