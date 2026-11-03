package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestDerivedAdmissionAfterStorageWrapKeepsOriginalFence(t *testing.T) {
	c := safetyCache(t)
	rootKey := []byte("/range")
	key := []byte("/range\nrange:block:one")
	parent, err := c.BeginFill(rootKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := c.DeriveFill(parent, key)
	if err != nil {
		t.Fatal(err)
	}
	stripe := c.KeyToStripe(key)
	padding := bytes.Repeat([]byte("x"), 1<<20)
	wrapped := false
	for round := 0; round < 40; round++ {
		writeStorageObject(t, c, "unrelated-wrap", padding)
		stripe.mu.RLock()
		wrapped = stripe.generation != stale.wraps[0]
		stripe.mu.RUnlock()
		if wrapped {
			break
		}
	}
	if !wrapped {
		t.Fatal("test did not wrap the disk")
	}
	if err = c.CheckFill(parent); err != nil {
		t.Fatalf("unrelated disk wrap changed URL fence: %v", err)
	}
	if writer, err := c.OpenWriteFillKeyReplacing(context.Background(), stale, key, nil); !errors.Is(err, ErrInvalidated) {
		if writer != nil {
			writer.Abort(io.EOF)
		}
		t.Fatalf("old split snapshot should be stale after disk wrap: %v", err)
	}
	writer, err := c.OpenWriteDerivedFill(context.Background(), parent, key)
	if err != nil {
		t.Fatalf("valid original URL fence could not reserve a block: %v", err)
	}
	if _, err = writer.Write([]byte("verified")); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	assertInvalidationBody(t, c, string(key), "verified")
}

func TestDerivedAdmissionRejectsInvalidParentsAndCancelledContext(t *testing.T) {
	for _, name := range []string{"purge", "policy", "foreign-key", "derived-parent", "nil-parent", "cancelled", "closed"} {
		t.Run(name, func(t *testing.T) {
			c := safetyCache(t)
			rootKey := []byte("/range")
			key := []byte("/range\nrange:block:one")
			parent, err := c.BeginFill(rootKey, rootKey)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			want := ErrInvalidated
			switch name {
			case "purge":
				_, err = c.PurgeSelector(rootKey, false)
			case "policy":
				err = c.InvalidateFill(parent)
			case "foreign-key":
				key = []byte("/other\nrange:block:one")
			case "derived-parent":
				parent, err = c.DeriveFill(parent, key)
			case "nil-parent":
				parent = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "closed":
				err = c.Close()
				want = ErrCacheClosed
			}
			if err != nil {
				t.Fatal(err)
			}
			writer, err := c.OpenWriteDerivedFill(ctx, parent, key)
			if writer != nil {
				writer.Abort(io.EOF)
			}
			if !errors.Is(err, want) {
				t.Fatalf("admission error=%v, want %v", err, want)
			}
		})
	}
}

func TestDerivedAdmissionPreservesOwnerAndPublicationFence(t *testing.T) {
	for _, name := range []string{"owner-only", "purge", "policy"} {
		t.Run(name, func(t *testing.T) {
			c := safetyCache(t)
			rootKey := []byte("/range")
			key := []byte("/range\nrange:block:one")
			parent, err := c.BeginFill(rootKey, rootKey)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := c.OpenWriteDerivedFill(context.Background(), parent, key)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = writer.Write([]byte("old")); err != nil {
				t.Fatal(err)
			}
			other, err := c.OpenWriteDerivedFill(context.Background(), parent, key)
			if other != nil {
				other.Abort(io.EOF)
			}
			if err == nil {
				t.Fatal("another caller took the active block writer")
			}
			if name == "owner-only" {
				if err = writer.Close(); err != nil {
					t.Fatalf("rejected caller damaged the owner: %v", err)
				}
				assertInvalidationBody(t, c, string(key), "old")
				return
			}
			if name == "policy" {
				err = c.InvalidateFill(parent)
			} else {
				_, err = c.PurgeSelector(rootKey, false)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); !errors.Is(err, ErrInvalidated) {
				t.Fatalf("writer escaped its original URL fence: %v", err)
			}
		})
	}
}

func TestDerivedWriterRetainsOriginalPolicyFence(t *testing.T) {
	c := safetyCache(t)
	root, _ := c.BeginFill([]byte("/range"), []byte("/range"))
	derived, err := c.DeriveFill(root, []byte("/range\nrange:block:one"))
	if err != nil {
		t.Fatal(err)
	}
	w, err := c.OpenWriteFill(context.Background(), derived, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("old block")); err != nil {
		t.Fatal(err)
	}
	if err = c.InvalidateFill(root); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); !errors.Is(err, ErrInvalidated) {
		t.Fatalf("old writer committed after policy change: %v", err)
	}
	if _, err = c.DeriveFill(root, []byte("/range\nrange:block:two")); !errors.Is(err, ErrInvalidated) {
		t.Fatalf("old parent derived new fill: %v", err)
	}
}

func TestDerivedSelectorsExceedVariantLimitAndPurgeOnlyExactFamily(t *testing.T) {
	c := safetyCache(t)
	root, _ := c.BeginFill([]byte("/range"), []byte("/range"))
	for i := 0; i < 65; i++ {
		key := []byte(fmt.Sprintf("/range\nrange:block:%d", i))
		token, err := c.DeriveFill(root, key)
		if err != nil {
			t.Fatal(i, err)
		}
		w, err := c.OpenWriteFill(context.Background(), token, 0)
		if err != nil {
			t.Fatal(err)
		}
		w.Write([]byte("verified"))
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	w, _ := c.OpenWrite([]byte("/range-adjacent"), 0)
	w.Write([]byte("keep"))
	w.Close()
	if _, err := c.BeginFill([]byte("/range"), []byte("/range")); err != nil {
		t.Fatal(err)
	}
	removed, err := c.PurgeSelector([]byte("/range"), false)
	if err != nil || len(removed) != 65 {
		t.Fatalf("purged=%d err=%v", len(removed), err)
	}
	r, _ := c.OpenReadCommitted([]byte("/range-adjacent"))
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil || string(body) != "keep" {
		t.Fatalf("adjacent=%q %v", body, err)
	}
}

func TestUnsafePolicyRemovesOnlyCapturedRangeManifest(t *testing.T) {
	c := safetyCache(t)
	write := func(key, body string) {
		root, _ := c.BeginFill([]byte("/item"), []byte("/item"))
		tok, err := c.DeriveFill(root, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		w, err := c.OpenWriteFill(context.Background(), tok, 0)
		if err != nil {
			t.Fatal(err)
		}
		w.Write([]byte(body))
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	const key = "/item\nrange:object:v3:origin"
	write(key, "old")
	old, _ := c.BeginFill([]byte("/item"), []byte("/item"))
	if err := c.InvalidateFill(old); err != nil {
		t.Fatal(err)
	}
	r, _ := c.OpenReadCommitted([]byte(key))
	_, err := io.ReadAll(r)
	r.Close()
	if err == nil {
		t.Fatal("unsafe ordinary policy left manifest reachable")
	}
	write(key, "first")
	stale, _ := c.BeginFill([]byte("/item"), []byte("/item"))
	write(key, "newer")
	if err := c.InvalidateFill(stale); err != nil {
		t.Fatal(err)
	}
	r, _ = c.OpenReadCommitted([]byte(key))
	body, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(body) != "newer" {
		t.Fatalf("newer manifest destroyed: %q %v", body, err)
	}
}
