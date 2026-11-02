package config

// Full-object followers wait for durable completion; the total deadline also
// bounds a private creator response and is independent of per-read idle limits.
func (c *Config) coalescingFields() []struct {
	name               string
	value              *int
	fallback, min, max int
} {
	return []struct {
		name               string
		value              *int
		fallback, min, max int
	}{
		{"cache.range_block_bytes", &c.RangeBlockBytes, 1 << 20, 1 << 20, 1 << 20},
		{"cache.shared_fill_public_static", &c.SharedFillPublicStatic, 0, 0, 1},
		{"cache.shared_fill_max_tasks", &c.SharedFillMaxTasks, 16, 1, 1000000},
		{"cache.shared_fill_max_waiters", &c.SharedFillMaxWaiters, 256, 1, 1000000},
		{"cache.shared_fill_timeout", &c.SharedFillTimeout, 300, 1, 86400},
	}
}
