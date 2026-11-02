package config

import (
	"fmt"
	"github.com/Monsterwi/razor/balancer"
	"net/netip"
	"net/url"
	"strconv"
)

// All timeouts are seconds. Payload reservations exclude the RAM cache, stripe
// buffers, directory indexes and Go runtime; they are not a process RSS limit.
func (c *Config) resourceFields() []struct {
	name               string
	value              *int
	fallback, min, max int
} {
	return append([]struct {
		name               string
		value              *int
		fallback, min, max int
	}{
		{"http.max_client_connections", &c.MaxClientConnections, 512, 1, 1000000},
		{"http.max_requests", &c.MaxRequests, 256, 1, 1000000},
		{"http.max_queued_requests", &c.MaxQueuedRequests, 64, 0, 1000000},
		{"http.max_origin_requests", &c.MaxOriginRequests, 64, 1, 1000000},
		{"http.max_origin_queued_requests", &c.MaxOriginQueuedRequests, 32, 0, 1000000},
		{"http.max_origin_connections", &c.MaxOriginConnections, 64, 1, 1000000},
		{"cache.max_writers", &c.MaxCacheWriters, 16, 1, 1000000},
		{"cache.max_queued_writers", &c.MaxQueuedCacheWriters, 16, 0, 1000000},
		{"cache.write_buffer_bytes", &c.CacheWriteBufferBytes, 128 << 20, 1, 1 << 40},
		{"cache.max_object_bytes", &c.MaxCacheObjectBytes, 64 << 20, 1, 1 << 40},
		{"http.request_header_timeout", &c.RequestHeaderTimeout, 10, 1, 86400},
		{"http.connect_timeout", &c.ConnectTimeout, 5, 1, 86400},
		{"http.tls_handshake_timeout", &c.TLSHandshakeTimeout, 5, 1, 86400},
		{"http.response_header_timeout", &c.ResponseHeaderTimeout, 30, 1, 86400},
		{"cache.write_timeout", &c.CacheWriteTimeout, 5, 1, 86400},
		{"http.queue_timeout", &c.QueueTimeout, 1, 1, 86400},
		{"http.shutdown_timeout", &c.ShutdownTimeout, 30, 1, 86400},
		{"http.keep_alive_no_activity_timeout_in", &c.KeepAliveTimeout, 120, 1, 86400},
		{"http.transaction_no_activity_timeout_in", &c.TransactionTimeoutIn, 30, 1, 86400},
		{"http.transaction_no_activity_timeout_out", &c.TransactionTimeoutOut, 30, 1, 86400},
	}, c.coalescingFields()...)
}

// WithDefaults supports embedders that historically passed an empty Config.
// Load validates explicit records before returning, including explicit zeros.
func (c *Config) WithDefaults() *Config {
	out := *c
	out.UpstreamTargets = append([]string(nil), c.UpstreamTargets...)
	out.TrustedProxies = append([]string(nil), c.TrustedProxies...)
	if out.resourceDefaults {
		return &out
	}
	out.resourceDefaults = true
	out.networkDefaults()
	if out.RAMCacheAlgorithm == "" {
		out.RAMCacheAlgorithm = "otter"
		if out.RAMCacheBytes == 0 {
			out.RAMCacheBytes = 10 << 20
		}
	}
	for _, f := range out.resourceFields() {
		if *f.value == 0 {
			*f.value = f.fallback
		}
	}
	return &out
}
func (c *Config) Validate() error {
	if len(c.UpstreamTargets) == 0 {
		return fmt.Errorf("at least one upstream is required")
	}
	for _, raw := range c.UpstreamTargets {
		target, err := url.Parse(raw)
		if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" || target.User != nil || target.Fragment != "" {
			return fmt.Errorf("invalid upstream URL")
		}
		if port := target.Port(); port != "" {
			if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
				return fmt.Errorf("upstream TCP port must be between 1 and 65535")
			}
		}
	}
	if _, err := balancer.Build(c.UpstreamAlgo, c.UpstreamTargets); err != nil {
		return err
	}
	for _, network := range c.TrustedProxies {
		if _, err := netip.ParsePrefix(network); err != nil {
			return fmt.Errorf("invalid trusted proxy network")
		}
	}

	if err := c.validateNetwork(); err != nil {
		return err
	}
	if err := c.validateRAM(); err != nil {
		return err
	}
	if c.ServerPort < 0 || c.ServerPort > 65535 {
		return fmt.Errorf("invalid server port")
	}
	for _, f := range c.resourceFields() {
		if *f.value < f.min || *f.value > f.max {
			return fmt.Errorf("proxy.config.%s must be between %d and %d", f.name, f.min, f.max)
		}
	}
	if c.RangeBlockBytes != 1<<20 {
		return fmt.Errorf("proxy.config.cache.range_block_bytes must be 1048576")
	}
	if c.CacheWriteBufferBytes < c.CacheWriterReservation() {
		return fmt.Errorf("cache write budget cannot hold one writer (%d bytes)", c.CacheWriterReservation())
	}
	return nil
}

// CacheWriterReservation covers the retained head, growing 1 MiB fragment,
// serialized document, bounded HTTP metadata and published-fragment records,
// plus one producer staging block and an independent committed-block reader.
// The HTTP path caps its metadata and key at 64 KiB, and writes in 32 KiB pieces.
func (c *Config) CacheWriterReservation() int {
	// One slot covers a producer staging block plus an independent sending reader.
	return 6<<20 + (c.MaxCacheObjectBytes/(1<<20)+2)*128 + c.RangeBlockBytes + c.RangeReaderReservation()
}
func (c *Config) RangeReaderReservation() int { return c.RangeBlockBytes + 2<<20 + 64<<10 }
func (c *Config) loadLimits(rc *RecordsConfig) error {
	for _, f := range c.resourceFields() {
		name := "proxy.config." + f.name
		if rc.Has(name) {
			value, err := strconv.Atoi(rc.GetString(name, ""))
			if err != nil {
				return fmt.Errorf("invalid integer %s: %w", name, err)
			}
			*f.value = value
		}
	}
	return c.Validate()
}
