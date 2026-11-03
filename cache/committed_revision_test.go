package cache

import (
	"context"
	"errors"
	"testing"
)

func TestCommittedRevisionRemainsBoundToOriginalWriter(t *testing.T) {
	c, m, root, parent, info := partialFixture(t)
	key := PartialObjectKey(root, m.Origin, m.Variant)
	write := func(object *PartialObject) *CacheVC {
		t.Helper()
		w, err := c.OpenWriteDerivedFill(context.Background(), parent, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.CommittedRevision(); err == nil {
			t.Fatal("active writer exposed a committed revision")
		}
		data, err := object.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		w.SetHTTPInfo(info)
		if _, err = w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		return w
	}
	first := write(m)
	selected, _, err := c.ReadPartialObject(key, m.Origin, m.Variant)
	if err != nil {
		t.Fatal(err)
	}
	// The same representation can receive a freshness update with identical
	// Generation; it still belongs to a distinct committed metadata record.
	second := write(m)
	current, _, err := c.ReadPartialObject(key, m.Origin, m.Variant)
	if err != nil {
		t.Fatal(err)
	}
	oldRevision, err := first.CommittedRevision()
	if err != nil || oldRevision != selected.Revision || oldRevision == current.Revision {
		t.Fatalf("writer adopted replacement revision old=%d selected=%d current=%d err=%v", oldRevision, selected.Revision, current.Revision, err)
	}
	newRevision, err := second.CommittedRevision()
	if err != nil || newRevision != current.Revision {
		t.Fatal("replacement revision differs from its writer")
	}
	m.Revision = oldRevision
	if err = c.InvalidatePartialObject(parent, root, m); err != nil {
		t.Fatal(err)
	}
	still, _, err := c.ReadPartialObject(key, m.Origin, m.Variant)
	if err != nil || still.Revision != newRevision {
		t.Fatalf("late old failure retired refreshed metadata: %v", err)
	}
	failed, err := c.OpenWriteDerivedFill(context.Background(), parent, m.ChunkKey(root, 0))
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected incomplete metadata")
	failed.Abort(failure)
	if _, err = failed.CommittedRevision(); !errors.Is(err, failure) {
		t.Fatalf("failed writer exposed revision: %v", err)
	}
}
