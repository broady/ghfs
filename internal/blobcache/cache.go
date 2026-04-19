// Package blobcache is the content-addressed blob store shared across
// all repos a ghfs process is serving.
//
// Layout: <Dir>/<oid[:2]>/<oid>. The 2-hex shard gives 256 sub-dirs;
// with ~50 KB average blob size and a 1 GB budget that's ~80 files per
// shard — small enough that readdir stays cheap.
//
// Eviction is sampled LRU (power-of-K-choices, Redis-style) using the
// filesystem's atime as the LRU signal. See evict.go for details.
package blobcache

import (
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Cache is a shared content-addressed store. All methods are safe for
// concurrent use.
type Cache struct {
	// Dir is the root directory (e.g., ~/.cache/ghfs/blobs).
	Dir string
	// Budget is the soft byte cap. Evictions fire when current exceeds it.
	Budget int64
	// Logger receives eviction + error telemetry.
	Logger *slog.Logger
	// SampleK is the number of candidates sampled per eviction cycle
	// (Redis default is 5). Higher K → closer to true LRU, more stat calls.
	SampleK int

	mu      sync.Mutex
	current int64
	rng     *rand.Rand
}

// New constructs a Cache rooted at dir with the given byte budget. The
// directory is created if missing; the on-disk current-bytes counter
// is populated by a single walk.
func New(dir string, budget int64, logger *slog.Logger) (*Cache, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create blob cache dir: %w", err)
	}
	c := &Cache{
		Dir:     dir,
		Budget:  budget,
		Logger:  logger,
		SampleK: 5,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if err := c.recomputeCurrent(); err != nil {
		// Non-fatal: the counter is self-correcting as Put() ticks run.
		logger.Warn("blob cache: recompute current bytes failed, proceeding with 0", "error", err)
		c.current = 0
	}
	return c, nil
}

// Path returns the expected absolute path for the given OID. The file
// may or may not exist — callers should stat before reading.
func (c *Cache) Path(oid string) string {
	if len(oid) >= 2 {
		return filepath.Join(c.Dir, oid[:2], oid)
	}
	return filepath.Join(c.Dir, oid)
}

// Get returns the file path for oid if it is cached, and whether it exists.
func (c *Cache) Get(oid string) (path string, ok bool) {
	p := c.Path(oid)
	if _, err := os.Stat(p); err == nil {
		return p, true
	}
	return p, false
}

// Added tells the cache that a blob of the given size was just added
// (by the hydrator worker that called gitstore.BlobToCache with
// c.Path(oid) as the destination). Triggers eviction if we're over
// budget.
func (c *Cache) Added(oid string, size int64) {
	c.mu.Lock()
	c.current += size
	overBy := c.current - c.Budget
	c.mu.Unlock()
	if c.Budget <= 0 || overBy <= 0 {
		return
	}
	c.evictUntilUnderBudget()
}

// Current returns the (approximate) number of bytes currently held.
// Self-healing: drifts from the true value are corrected over time as
// eviction runs.
func (c *Cache) Current() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// recomputeCurrent walks the cache and sums file sizes. Cheap on a
// fresh mount; run once at startup. The shard scheme caps directory
// fanout so readdir stays quick.
func (c *Cache) recomputeCurrent() error {
	var total int64
	err := filepath.WalkDir(c.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil // skip racing removals
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.current = total
	c.mu.Unlock()
	return nil
}
