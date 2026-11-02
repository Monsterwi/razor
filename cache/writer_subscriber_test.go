package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestWriterSubscribersHaveIndependentProgressAndCancellation(t *testing.T) {
	c := safetyCache(t)
	w, err := c.OpenWrite([]byte("subscribers"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort(io.ErrUnexpectedEOF)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := w.OpenReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := w.OpenReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := w.Write([]byte("prefix")); err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 6)
	if _, err := io.ReadFull(first, prefix); err != nil || string(prefix) != "prefix" {
		t.Fatalf("prefix=%q err=%v", prefix, err)
	}
	blocked := make(chan error, 1)
	go func() { _, err := first.Read(make([]byte, 1)); blocked <- err }()
	cancel()
	select {
	case err := <-blocked:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled reader returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader cancellation did not wake the blocked read")
	}
	first.Close()
	if _, err := w.Write([]byte("suffix")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(second)
	if err != nil || string(body) != "prefixsuffix" {
		t.Fatalf("independent subscriber=%q err=%v", body, err)
	}
}

func TestCommittedWriterIsCacheHitWhileStreamingSubscriberStillOpen(t *testing.T) {
	c := safetyCache(t)
	key := []byte("committed-with-subscriber")
	w, err := c.OpenWrite(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	subscriber, err := w.OpenReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Close()
	if _, err := w.Write([]byte("complete bytes")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	hit, err := c.OpenReadCommitted(key)
	if err != nil {
		t.Fatalf("retained subscriber hid committed object: %v", err)
	}
	defer hit.Close()
	body, err := io.ReadAll(hit)
	if err != nil || string(body) != "complete bytes" {
		t.Fatalf("committed hit=%q err=%v", body, err)
	}
	body, err = io.ReadAll(subscriber)
	if err != nil || string(body) != "complete bytes" {
		t.Fatalf("original subscriber=%q err=%v", body, err)
	}
}

func TestLateWriterSubscriptionCannotJoinOrReleaseReplacement(t *testing.T) {
	c := safetyCache(t)
	key := []byte("replaced-writer")
	old, err := c.OpenWrite(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("old origin failed")
	old.Abort(cause)
	next, err := c.OpenWrite(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Abort(io.ErrUnexpectedEOF)
	if _, err := next.Write([]byte("replacement bytes")); err != nil {
		t.Fatal(err)
	}
	late, err := old.OpenReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(late)
	late.Close()
	late.Close()
	if !errors.Is(err, cause) || len(body) != 0 {
		t.Fatalf("old subscriber joined replacement: %q %v", body, err)
	}
	if conflict, err := c.OpenWrite(key, 0); err == nil {
		conflict.Abort(io.ErrUnexpectedEOF)
		t.Fatal("old subscriber released the replacement's writer reference")
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := next.OpenReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	body, err = io.ReadAll(reader)
	if err != nil || string(body) != "replacement bytes" {
		t.Fatalf("late completed subscriber=%q err=%v", body, err)
	}
}

func TestWriterAbortWakesEveryBlockedSubscriberWithCause(t *testing.T) {
	c := safetyCache(t)
	w, err := c.OpenWrite([]byte("failed-subscribers"), 0)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 8)
	for range 8 {
		r, err := w.OpenReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			defer r.Close()
			_, err := r.Read(make([]byte, 1))
			results <- err
		}()
	}
	cause := errors.New("upstream failed before headers")
	w.Abort(cause)
	for range 8 {
		select {
		case err := <-results:
			if !errors.Is(err, cause) {
				t.Fatalf("subscriber lost terminal cause: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("writer failure did not wake subscriber")
		}
	}
	if err := w.Close(); !errors.Is(err, cause) {
		t.Fatalf("close lost abort cause: %v", err)
	}
}

func TestWriterSubscriberReceivesFinalBytesWhenWriteAndCloseRaceRead(t *testing.T) {
	c := safetyCache(t)
	payload := bytes.Repeat([]byte("final"), 128)
	for i := range 256 {
		w, err := c.OpenWrite([]byte(fmt.Sprintf("final-write-%d", i)), 0)
		if err != nil {
			t.Fatal(err)
		}
		r, err := w.OpenReader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		var body []byte
		var readErr error
		go func() { body, readErr = io.ReadAll(r); close(done) }()
		if _, err := w.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		<-done
		r.Close()
		if readErr != nil || !bytes.Equal(body, payload) {
			t.Fatalf("final write lost bytes: iteration=%d length=%d err=%v", i, len(body), readErr)
		}
	}
}
