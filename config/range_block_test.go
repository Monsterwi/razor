package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestRangeBlockConfiguration(t *testing.T) {
	for _, value := range []int{65536, 1048576, 4194304, 8388608, 0, -1, 65535, 1048577, 16777216} {
		t.Run(strconv.Itoa(value), func(t *testing.T) {
			dir := t.TempDir()
			for name, data := range map[string]string{"records.config": "CONFIG proxy.config.cache.range_block_bytes INT " + strconv.Itoa(value) + "\n", "storage.config": ""} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := Load(dir)
			valid := value == 1048576
			if valid && err != nil {
				t.Fatal(err)
			}
			if !valid && (err == nil || !strings.Contains(err.Error(), "range_block_bytes")) {
				t.Fatalf("invalid size %d accepted: %v cfg=%v", value, err, cfg)
			}
		})
	}
}
