package cache

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestPartialRecoveryRejectsOldFormatsWithoutChangingUnrelatedFiles(t *testing.T) {
	for _, kind := range []string{"object-version", "object-layout", "segment-version", "legacy-namespace", "checkpoint-corrupt"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "cache")
			business := filepath.Join(dir, "business.db")
			unrelatedSidecar := filepath.Join(dir, "other.checkpoint")
			for _, file := range []string{business, unrelatedSidecar} {
				if err := os.WriteFile(file, []byte("unrelated bytes\x00keep"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			otherPath := filepath.Join(dir, "neighbor", "store")
			other, otherStripe := openStorageTest(t, otherPath, 8<<20, 0, 0)
			writeStorageObject(t, other, "neighbor", []byte("neighbor bytes"))
			if err := other.Close(); err != nil {
				t.Fatal(err)
			}
			protected := make(map[string][]byte)
			for _, file := range []string{business, unrelatedSidecar, otherPath, otherStripe.checkpointPath()} {
				data, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				protected[file] = data
			}
			c, s := openStorageTest(t, path, 32<<20, 0, 0)
			root := []byte("http://cache.test/old-format")
			m, info := partialRecoveryObject()
			parent, err := c.BeginFill(root, root)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "object-version":
				old := *m
				old.Version--
				savePartialRecoveryObject(t, c, root, &old, parent, info)
			case "object-layout":
				old := *m
				old.ChunkBytes /= 2
				savePartialRecoveryObject(t, c, root, &old, parent, info)
			case "legacy-namespace":
				key := strings.Replace(string(PartialObjectKey(root, m.Origin, m.Variant)), ":v3:", ":v2:", 1)
				writeStorageObject(t, c, key, []byte(`{"Version":2}`))
				writeStorageObject(t, c, string(root), []byte("legacy complete object"))
			default:
				savePartialRecoveryObject(t, c, root, m, parent, info)
				publishPartialFixture(t, c, m, root, parent, info, 0)
				if kind == "segment-version" {
					data, err := json.Marshal(partialSegment{Version: PartialFormatVersion - 1, Identity: m.identity(), Index: 0, PartialCoverage: PartialCoverage{Bits: 1}})
					if err != nil {
						t.Fatal(err)
					}
					w, err := c.OpenWriteDerivedFill(context.Background(), parent, m.segmentKey(root, 0))
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
				}
			}
			if err = c.Close(); err != nil {
				t.Fatal(err)
			}
			if kind == "checkpoint-corrupt" {
				data, err := os.ReadFile(s.checkpointPath())
				if err != nil {
					t.Fatal(err)
				}
				data[0] ^= 1
				if err = os.WriteFile(s.checkpointPath(), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			c, _ = openStorageTest(t, path, 32<<20, 0, 0)
			recovered, _, err := c.ReadPartialObject(PartialObjectKey(root, m.Origin, m.Variant), m.Origin, m.Variant)
			if kind == "segment-version" {
				if err != nil {
					t.Fatal(err)
				}
				if coverage, err := c.PartialCoverage(root, recovered, 0); err == nil {
					t.Fatalf("old segment interpreted as current coverage: %+v", coverage)
				}
			} else if err == nil {
				t.Fatalf("incompatible %s interpreted as object: %+v", kind, recovered)
			}
			if err = c.Close(); err != nil {
				t.Fatal(err)
			}
			for file, want := range protected {
				got, err := os.ReadFile(file)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("recovery changed protected %s: %v", file, err)
				}
			}
			other, _ = openStorageTest(t, otherPath, 8<<20, 0, 0)
			assertInvalidationBody(t, other, "neighbor", "neighbor bytes")
		})
	}
}

func TestPartialRecoveryPublicationAndCheckpointKillWindows(t *testing.T) {
	root := []byte("http://cache.test/recovery-window")
	if path := os.Getenv("RAZOR_PARTIAL_RECOVERY_PATH"); path != "" {
		c, s := openStorageTest(t, path, 32<<20, 0, 0)
		m, info := partialRecoveryObject()
		parent, err := c.BeginFill(root, root)
		if err != nil {
			t.Fatal(err)
		}
		savePartialRecoveryObject(t, c, root, m, parent, info)
		window := os.Getenv("RAZOR_PARTIAL_RECOVERY_WINDOW")
		if window == "before-data" || window == "partial-data" {
			s.disk = &killedWriteDisk{File: s.Fd, start: s.Start, window: window}
		}
		w, err := c.OpenWritePartialChunk(context.Background(), parent, m.ChunkKey(root, 0), PartialChunkBytes)
		if err != nil {
			t.Fatal(err)
		}
		w.SetHTTPInfo(info)
		if _, err = w.Write(bytes.Repeat([]byte{'a'}, int(PartialChunkBytes))); err != nil {
			t.Fatal(err)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		if window == "partial-bitmap" {
			s.disk = &killedWriteDisk{File: s.Fd, start: s.Start, window: "partial-data"}
		}
		if window != "chunk-published" {
			if err = c.PublishPartialChunk(context.Background(), parent, root, m, 0, info); err != nil {
				t.Fatal(err)
			}
		}
		if window == "before-clean-index" {
			s.disk = &killedWriteDisk{File: s.Fd, start: s.Start, window: window}
		}
		if window == "before-clean-index" || window == "clean-close" {
			if err = c.Close(); err != nil {
				t.Fatal(err)
			}
		}
		fmt.Println("ready-to-kill")
		select {}
	}
	for _, window := range []string{"before-data", "partial-data", "chunk-published", "partial-bitmap", "bitmap-published", "before-clean-index", "clean-close"} {
		t.Run(window, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache")
			c, _ := openStorageTest(t, path, 32<<20, 0, 0)
			old, info := partialRecoveryObject()
			old.ETag, old.Generation = `"historical"`, "fedcba9876543210fedcba9876543210"
			info.ResponseHeaders.Set("ETag", old.ETag)
			parent, err := c.BeginFill(root, root)
			if err != nil {
				t.Fatal(err)
			}
			savePartialRecoveryObject(t, c, root, old, parent, info)
			publishPartialFixture(t, c, old, root, parent, info, 0)
			if err = c.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestPartialRecoveryPublicationAndCheckpointKillWindows$")
			cmd.Env = append(os.Environ(), "RAZOR_PARTIAL_RECOVERY_PATH="+path, "RAZOR_PARTIAL_RECOVERY_WINDOW="+window)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			})
			ready := make(chan bool, 1)
			go func() {
				scan := bufio.NewScanner(stdout)
				for scan.Scan() {
					if scan.Text() == "ready-to-kill" {
						ready <- true
						return
					}
				}
				ready <- false
			}()
			select {
			case ok := <-ready:
				if !ok {
					_ = cmd.Wait()
					t.Fatalf("child failed before %s: %s", window, stderr.String())
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("child did not reach %s", window)
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = cmd.Wait(); err == nil {
				t.Fatal("killed child exited successfully")
			}
			c, _ = openStorageTest(t, path, 32<<20, 0, 0)
			m, _ := partialRecoveryObject()
			recovered, _, err := c.ReadPartialObject(PartialObjectKey(root, m.Origin, m.Variant), m.Origin, m.Variant)
			if window != "clean-close" {
				if err == nil {
					t.Fatalf("unclean metadata recovered generation=%s", recovered.Generation)
				}
				for _, candidate := range []*PartialObject{old, m} {
					if body, err := readStorageObject(c, string(candidate.ChunkKey(root, 0))); err == nil || len(body) != 0 {
						t.Fatalf("unclean chunk hit=%d bytes err=%v", len(body), err)
					}
				}
				return
			}
			if err != nil || recovered.Generation != m.Generation || recovered.ETag != m.ETag {
				t.Fatalf("reliable metadata recovery=%+v err=%v", recovered, err)
			}
			coverage, err := c.PartialCoverage(root, recovered, 0)
			if err != nil || coverage.Bits != 1 {
				t.Fatalf("reliable coverage=%+v err=%v", coverage, err)
			}
			body, err := readStorageObject(c, string(recovered.ChunkKey(root, 0)))
			if err != nil || !bytes.Equal(body, bytes.Repeat([]byte{'a'}, int(PartialChunkBytes))) {
				t.Fatalf("reliable chunk=%d bytes err=%v", len(body), err)
			}
		})
	}
}

func partialRecoveryObject() (*PartialObject, *CacheHTTPInfo) {
	m := &PartialObject{Version: PartialFormatVersion, ChunkBytes: PartialChunkBytes, Total: PartialChunkBytes,
		ETag: `"saved"`, Origin: "http://origin.test", Generation: "0123456789abcdef0123456789abcdef"}
	info := NewCacheHTTPInfo()
	info.Status = 200
	info.ResponseHeaders.Set("ETag", m.ETag)
	info.ResponseHeaders.Set("Cache-Control", "public,max-age=3600")
	return m, info
}

func savePartialRecoveryObject(t *testing.T, c *Cache, root []byte, m *PartialObject, parent *FillToken, info *CacheHTTPInfo) {
	t.Helper()
	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	w, err := c.OpenWriteDerivedFill(context.Background(), parent, PartialObjectKey(root, m.Origin, m.Variant))
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
}

func TestPartialRecoveryRepairsCoverageWhenOtherStripeCannotSaveData(t *testing.T) {
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "one"), filepath.Join(dir, "two")}
	sizes := []int64{32 << 20, 32 << 20}
	c, err := NewCache(2, paths, sizes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	m, info := partialRecoveryObject()
	var root []byte
	for i := 0; i < 1000; i++ {
		candidate := []byte(fmt.Sprintf("http://cache.test/split-%d", i))
		dataStripe := c.KeyToStripe(m.ChunkKey(candidate, 0))
		if dataStripe != c.KeyToStripe(m.segmentKey(candidate, 0)) && dataStripe != c.KeyToStripe(PartialObjectKey(candidate, m.Origin, m.Variant)) {
			root = candidate
			break
		}
	}
	if root == nil {
		t.Fatal("fixture did not separate data from metadata")
	}
	parent, err := c.BeginFill(root, root)
	if err != nil {
		t.Fatal(err)
	}
	savePartialRecoveryObject(t, c, root, m, parent, info)
	data := bytes.Repeat([]byte{'a'}, int(PartialChunkBytes))
	w, err := c.OpenWritePartialChunk(context.Background(), parent, m.ChunkKey(root, 0), int64(len(data)))
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
	if err = c.PublishPartialChunk(context.Background(), parent, root, m, 0, info); err != nil {
		t.Fatal(err)
	}
	failed := c.KeyToStripe(m.ChunkKey(root, 0))
	failed.disk = &faultDisk{File: failed.Fd, start: failed.Start, syncErr: syscall.EIO}
	if err = c.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("failed data save=%v", err)
	}
	c, err = NewCache(2, paths, sizes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	recovered, recoveredInfo, err := c.ReadPartialObject(PartialObjectKey(root, m.Origin, m.Variant), m.Origin, m.Variant)
	if err != nil || recovered.Generation != m.Generation {
		t.Fatalf("metadata stripe did not recover: %v", err)
	}
	coverage, err := c.PartialCoverage(root, recovered, 0)
	if err != nil || coverage.Bits != 1 {
		t.Fatalf("saved coverage=%+v, %v", coverage, err)
	}
	if body, err := readStorageObject(c, string(m.ChunkKey(root, 0))); !errors.Is(err, ErrNotResident) || len(body) != 0 {
		t.Fatalf("unsaved data became a valid hit: bytes=%d err=%v", len(body), err)
	}
	fresh, err := c.BeginFill(root, root)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.RepairPartialChunk(context.Background(), fresh, root, recovered, 0, recoveredInfo, coverage); err != nil {
		t.Fatal(err)
	}
	coverage, err = c.PartialCoverage(root, recovered, 0)
	if err != nil || coverage.Bits != 0 {
		t.Fatalf("missing data repair=%+v, %v", coverage, err)
	}
	refill, err := c.OpenWritePartialChunk(context.Background(), fresh, recovered.ChunkKey(root, 0), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	refill.SetHTTPInfo(recoveredInfo)
	if _, err = refill.Write(data); err != nil {
		t.Fatal(err)
	}
	if err = refill.Close(); err != nil {
		t.Fatal(err)
	}
	if err = c.PublishPartialChunk(context.Background(), fresh, root, recovered, 0, recoveredInfo); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = NewCache(2, paths, sizes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	got, err := readStorageObject(c, string(m.ChunkKey(root, 0)))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reliable refill recovery=%d bytes, %v", len(got), err)
	}
	coverage, err = c.PartialCoverage(root, recovered, 0)
	if err != nil || coverage.Bits != 1 {
		t.Fatalf("reliable refill coverage=%+v, %v", coverage, err)
	}
}

func TestPartialCheckpointPublishFailureIsReportedAndDoesNotPreventOtherStripeClose(t *testing.T) {
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "one"), filepath.Join(dir, "two")}
	sizes := []int64{32 << 20, 32 << 20}
	c, err := NewCache(2, paths, sizes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	m, info := partialRecoveryObject()
	root := []byte("http://cache.test/checkpoint-failure")
	parent, err := c.BeginFill(root, root)
	if err != nil {
		t.Fatal(err)
	}
	savePartialRecoveryObject(t, c, root, m, parent, info)
	data := publishPartialFixture(t, c, m, root, parent, info, 0)
	failed := c.KeyToStripe(m.ChunkKey(root, 0))
	var otherKey []byte
	for i := 0; ; i++ {
		otherKey = []byte(fmt.Sprintf("/other-stripe-%d", i))
		if c.KeyToStripe(otherKey) != failed {
			break
		}
	}
	writeStorageObject(t, c, string(otherKey), []byte("retained"))
	checkpoint := failed.checkpointPath()
	if err = os.Mkdir(checkpoint, 0700); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err == nil {
		t.Fatal("checkpoint Rename failure reported reliable save")
	}
	if entries, err := filepath.Glob(filepath.Join(dir, ".bakemono-checkpoint-*")); err != nil || len(entries) != 0 {
		t.Fatalf("failed checkpoint temporary files=%v err=%v", entries, err)
	}
	c, err = NewCache(2, paths, sizes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Init(); err != nil {
		t.Fatalf("failed close left stripe owned: %v", err)
	}
	if got, err := readStorageObject(c, string(m.ChunkKey(root, 0))); err == nil || len(got) != 0 {
		t.Fatalf("failed checkpoint became hit: %d/%d bytes err=%v", len(got), len(data), err)
	}
	assertInvalidationBody(t, c, string(otherKey), "retained")
	if entry, err := os.Stat(checkpoint); err != nil || !entry.IsDir() {
		t.Fatalf("recovery changed obstructing directory: %v", err)
	}
}
