package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

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
