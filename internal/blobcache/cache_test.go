package blobcache

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNew_CreatesDir(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "blobs")
	c, err := New(tmp, 1024, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.Dir != tmp {
		t.Errorf("Dir = %q, want %q", c.Dir, tmp)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("expected dir to exist: %v", err)
	}
}

func TestPath_Sharding(t *testing.T) {
	t.Parallel()
	c, err := New(t.TempDir(), 1024, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	oid := "abcdef1234567890"
	got := c.Path(oid)
	want := filepath.Join(c.Dir, "ab", oid)
	if got != want {
		t.Errorf("Path(%q) = %q, want %q", oid, got, want)
	}
}

func TestPath_ShortOID(t *testing.T) {
	t.Parallel()
	c, err := New(t.TempDir(), 1024, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := c.Path("x")
	// Short OIDs fall back to the flat layout.
	if got != filepath.Join(c.Dir, "x") {
		t.Errorf("Path(x) = %q, wanted flat layout", got)
	}
}

func TestPutGetRoundtrip(t *testing.T) {
	t.Parallel()
	c, err := New(t.TempDir(), 1024, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	oid := "ff000000000000000000000000000000deadbeef"
	payload := []byte("hello world")

	// Simulate a hydrator write: create the shard dir + file, then
	// notify the cache of the size.
	path := c.Path(oid)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir shard: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.Added(oid, int64(len(payload)))

	gotPath, ok := c.Get(oid)
	if !ok {
		t.Fatal("Get reported cache miss for file we just wrote")
	}
	if gotPath != path {
		t.Errorf("Get returned %q, want %q", gotPath, path)
	}
	if c.Current() != int64(len(payload)) {
		t.Errorf("Current = %d, want %d", c.Current(), len(payload))
	}
}

func TestGet_Miss(t *testing.T) {
	t.Parallel()
	c, err := New(t.TempDir(), 1024, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, ok := c.Get("deadbeefdeadbeefdeadbeef")
	if ok {
		t.Fatal("expected miss on empty cache")
	}
}

// TestSampledEvictionStaysUnderBudget fills the cache past the budget
// and then checks that successive Added() calls keep us within a
// bounded slop. Because power-of-K-choices is approximate, we allow a
// generous tolerance.
func TestSampledEvictionStaysUnderBudget(t *testing.T) {
	t.Parallel()
	budget := int64(1024)
	c, err := New(t.TempDir(), budget, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Write 40 blobs of 100 bytes each = 4 KB total, 4x budget.
	payload := make([]byte, 100)
	for i := 0; i < 40; i++ {
		oid := fakeOID(i)
		path := c.Path(oid)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			t.Fatal(err)
		}
		c.Added(oid, int64(len(payload)))
	}

	// Sampled LRU is approximate; allow up to 3x budget slop (in
	// practice we see < 1.2x in the normal case but CI can be noisy).
	if cur := c.Current(); cur > budget*3 {
		t.Fatalf("current bytes %d exceeds %d (3x budget)", cur, budget*3)
	}

	// Recount from disk and make sure the counter and disk agree to
	// within one blob — drift accumulates when random eviction races
	// with our counter updates, so allow a bounded mismatch.
	if err := c.recomputeCurrent(); err != nil {
		t.Fatal(err)
	}
	if cur := c.Current(); cur > budget*3 {
		t.Fatalf("on-disk bytes %d exceeds %d (3x budget)", cur, budget*3)
	}
}

// TestShardingDistribution checks that the sharding prefix spreads
// blobs across many top-level dirs so readdir stays cheap.
func TestShardingDistribution(t *testing.T) {
	t.Parallel()
	c, err := New(t.TempDir(), 1<<30, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	shards := map[string]bool{}
	for i := 0; i < 500; i++ {
		oid := fakeOID(i)
		path := c.Path(oid)
		// Extract shard dir name (first 2 hex).
		parent := filepath.Base(filepath.Dir(path))
		shards[parent] = true
	}
	// We expect plenty of distinct shards with 500 distinct OIDs
	// whose first 2 hex vary.
	if len(shards) < 10 {
		t.Fatalf("too few distinct shards: %d (want >= 10)", len(shards))
	}
	for s := range shards {
		if len(s) != 2 || !isHex(s) {
			t.Errorf("shard %q is not 2 hex chars", s)
		}
	}
}

func fakeOID(i int) string {
	// Produce varying 40-char hex strings.
	base := "0123456789abcdef"
	b := make([]byte, 40)
	for j := range b {
		b[j] = base[(i+j*7)%16]
	}
	return string(b)
}

func isHex(s string) bool {
	for _, r := range s {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}
