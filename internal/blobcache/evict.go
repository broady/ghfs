package blobcache

import (
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// evictUntilUnderBudget runs sampled-LRU evictions until current_bytes
// drops below the budget (or the cache is empty).
func (c *Cache) evictUntilUnderBudget() {
	// A simple guard: never loop more than some bounded amount per call,
	// to keep a single Put from turning into a GC marathon.
	const maxEvictionsPerCall = 64
	for i := 0; i < maxEvictionsPerCall; i++ {
		c.mu.Lock()
		if c.current <= c.Budget {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		freed, err := c.evictOne()
		if err != nil {
			c.Logger.Warn("blob cache: eviction error", "error", err)
			return
		}
		if freed == 0 {
			// Cache is empty or sampling found nothing — stop.
			return
		}
	}
}

// candidate is a sampled cache file: its path, size, and access time.
type candidate struct {
	path  string
	size  int64
	atime time.Time
}

// evictOne samples SampleK candidates across the cache, removes the
// one with the oldest access time, and returns the number of bytes
// freed. Returns (0, nil) if no candidates could be sampled.
func (c *Cache) evictOne() (int64, error) {
	k := c.SampleK
	if k < 1 {
		k = 1
	}

	candidates := make([]candidate, 0, k)
	for i := 0; i < k*3 && len(candidates) < k; i++ {
		// Attempt up to 3x K times to handle empty shards.
		cand, err := c.sampleOne()
		if err != nil {
			if errors.Is(err, errEmptyCache) {
				if len(candidates) == 0 {
					return 0, nil
				}
				break
			}
			// Skip transient errors from racing unlinks.
			continue
		}
		candidates = append(candidates, cand)
	}
	if len(candidates) == 0 {
		return 0, nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].atime.Before(candidates[j].atime)
	})
	victim := candidates[0]

	if err := os.Remove(victim.path); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	c.mu.Lock()
	c.current -= victim.size
	if c.current < 0 {
		c.current = 0
	}
	c.mu.Unlock()
	c.Logger.Debug("blob cache: evicted", "path", victim.path, "size", victim.size, "atime", victim.atime)
	return victim.size, nil
}

var errEmptyCache = errors.New("blob cache is empty")

// sampleOne picks a random shard dir, then a random file within it,
// and returns its metadata. Returns errEmptyCache if no populated
// shards could be found after a few tries.
func (c *Cache) sampleOne() (candidate, error) {
	shards, err := readDirNames(c.Dir)
	if err != nil {
		return candidate{}, err
	}
	if len(shards) == 0 {
		return candidate{}, errEmptyCache
	}
	// Try up to 5 random shards before giving up (handles a cache
	// where most shards are empty).
	for i := 0; i < 5; i++ {
		shard := shards[c.randInt(len(shards))]
		shardPath := filepath.Join(c.Dir, shard)
		entries, err := readDirNames(shardPath)
		if err != nil {
			continue
		}
		if len(entries) == 0 {
			continue
		}
		entry := entries[c.randInt(len(entries))]
		entryPath := filepath.Join(shardPath, entry)
		info, err := os.Stat(entryPath)
		if err != nil {
			continue
		}
		return candidate{
			path:  entryPath,
			size:  info.Size(),
			atime: fileAtime(info),
		}, nil
	}
	return candidate{}, errEmptyCache
}

// randInt returns a random integer in [0, n) using the cache's own
// rand source (guarded by its mutex).
func (c *Cache) randInt(n int) int {
	if n <= 1 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rng == nil {
		c.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return c.rng.Intn(n)
}

// readDirNames is a tiny wrapper around os.ReadDir that returns just
// the names (avoids allocating a DirEntry slice we don't need).
func readDirNames(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ents, err := f.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		// Skip any non-regular entries (e.g., stray tmp files / dotfiles).
		if e.Type().IsDir() || e.Type().IsRegular() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// Ensure we don't import io/fs only for the side effect of a type
// reference; keep the import for clarity.
var _ = fs.ModePerm
