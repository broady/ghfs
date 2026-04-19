package reposrc

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/broady/ghfs/internal/blobcache"
	"github.com/broady/ghfs/internal/gitstore"
	"github.com/broady/ghfs/internal/model"
)

// TestBuildTree_PathsAndChildren verifies the flat BaseNode slice is
// correctly folded into Nodes + Children maps.
func TestBuildTree_PathsAndChildren(t *testing.T) {
	t.Parallel()

	// Build a toy repo so gitstore.BuildTreeIndex returns real nodes.
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	run(t, "git", "init", "-q", repo)
	mustWrite(t, filepath.Join(repo, "README.md"), []byte("hi"))
	mustWrite(t, filepath.Join(repo, "a", "b.txt"), []byte("nested"))
	run(t, "git", "-C", repo, "add", "-A")
	run(t, "git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "init")

	store := gitstore.New(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the repo's own .git as the blobless clone stand-in — it has
	// the same shape for our purposes (BuildTreeIndex only cares
	// about tree reachability).
	cfg := gitConfig(t, filepath.Join(repo, ".git"), tmp)
	oid, err := store.ResolveRef(ctx, cfg, "HEAD")
	if err != nil {
		t.Fatalf("ResolveRef: %v", err)
	}
	nodes, err := store.BuildTreeIndex(ctx, cfg, oid)
	if err != nil {
		t.Fatalf("BuildTreeIndex: %v", err)
	}
	tree := buildTree("", oid, nodes)

	if _, ok := tree.Nodes["README.md"]; !ok {
		t.Errorf("expected README.md in Nodes; got keys %v", keys(tree.Nodes))
	}
	if _, ok := tree.Nodes["a/b.txt"]; !ok {
		t.Errorf("expected a/b.txt in Nodes; got keys %v", keys(tree.Nodes))
	}
	// README.md is a child of "."; a is a child of "."; b.txt is a child of "a".
	rootKids := tree.Children["."]
	if !contains(rootKids, "README.md") || !contains(rootKids, "a") {
		t.Errorf("root children = %v, want contains README.md and a", rootKids)
	}
	aKids := tree.Children["a"]
	if !contains(aKids, "b.txt") {
		t.Errorf("a children = %v, want contains b.txt", aKids)
	}
}

// TestEnsureCloned_Idempotent verifies that EnsureCloned is safe to
// call many times — only the first call clones, the rest are no-ops.
// Uses a local bare repo as the "remote" so the test has no network
// dependency.
func TestEnsureCloned_Idempotent(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	// Create a source repo with one commit.
	source := filepath.Join(tmp, "src")
	run(t, "git", "init", "-q", source)
	mustWrite(t, filepath.Join(source, "f.txt"), []byte("x"))
	run(t, "git", "-C", source, "add", "-A")
	run(t, "git", "-C", source, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "init")

	// Manager wired to use `source` as the clone URL.
	blobs, err := blobcache.New(filepath.Join(tmp, "blobs"), 1<<20, nil)
	if err != nil {
		t.Fatalf("blobcache.New: %v", err)
	}
	store := gitstore.New(nil)
	m := NewManager(store, blobs, filepath.Join(tmp, "repos"), "" /* token */, nil)
	// Intercept the clone URL: since buildCloneURL hardcodes github.com,
	// we override it by constructing a Repo manually.
	r := m.Get("localowner", "localrepo")
	r.Config.RemoteURL = source

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := r.EnsureCloned(ctx); err != nil {
		t.Fatalf("EnsureCloned (first): %v", err)
	}
	// Stat a known file inside the clone to verify it worked.
	if _, err := os.Stat(r.Config.GitDir); err != nil {
		t.Fatalf("expected git dir at %s: %v", r.Config.GitDir, err)
	}
	if err := r.EnsureCloned(ctx); err != nil {
		t.Fatalf("EnsureCloned (second): %v", err)
	}
	if !r.cloned {
		t.Fatal("cloned flag not set")
	}
}

// TestResolveTree_HEAD verifies that resolving the empty ref returns a
// tree with the expected root entry.
func TestResolveTree_HEAD(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	source := filepath.Join(tmp, "src")
	run(t, "git", "init", "-q", source)
	mustWrite(t, filepath.Join(source, "hello.txt"), []byte("world"))
	run(t, "git", "-C", source, "add", "-A")
	run(t, "git", "-C", source, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "init")

	blobs, err := blobcache.New(filepath.Join(tmp, "blobs"), 1<<20, nil)
	if err != nil {
		t.Fatalf("blobcache.New: %v", err)
	}
	store := gitstore.New(nil)
	m := NewManager(store, blobs, filepath.Join(tmp, "repos"), "", nil)
	r := m.Get("o", "r")
	r.Config.RemoteURL = source

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tree, err := r.ResolveTree(ctx, "")
	if err != nil {
		t.Fatalf("ResolveTree: %v", err)
	}
	if _, ok := tree.Nodes["hello.txt"]; !ok {
		t.Fatalf("expected hello.txt in tree; got %v", keys(tree.Nodes))
	}
}

// TestClassifyCloneError covers the error-classification table used by
// EnsureCloned to decide between *NotFoundError (→ ENOENT in the FUSE
// layer) and a plain wrapped error (→ EIO).
//
// Concrete strings drawn from real git-clone stderr output observed
// against github.com.
func TestClassifyCloneError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		stderr     string
		wantNotFnd bool
	}{
		{"true 404", "remote: Repository not found.\nfatal: repository 'https://github.com/nope/nope.git/' not found", true},
		{"private / auth", "remote: Repository not found.\nfatal: Authentication failed for 'https://github.com/...'", true},
		{"no creds prompt", "fatal: could not read Username for 'https://github.com': terminal prompts disabled", true},
		{"dns failure", "fatal: unable to access 'https://github.com/x/y.git/': Could not resolve host: github.com", false},
		{"tls timeout", "fatal: unable to access 'https://github.com/x/y.git/': SSL connection timeout", false},
		{"http 503", "fatal: unable to access 'https://github.com/x/y.git/': The requested URL returned error: 503", false},
		{"generic", "fatal: something went wrong", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := classifyCloneError("owner/repo", errTest(c.stderr))
			if got := IsNotFound(err); got != c.wantNotFnd {
				t.Errorf("IsNotFound(%q) = %v, want %v (err=%v)", c.stderr, got, c.wantNotFnd, err)
			}
		})
	}
}

// TestEnsureCloned_FailureCached verifies the cloneFailureBackoff
// negative cache: a second EnsureCloned within the window returns the
// cached error without re-invoking Store.CloneBlobless.
//
// We don't drive a real git clone — instead we point at a bogus gitDir
// under a non-existent parent so CloneBlobless fails in mktemp with a
// permission-style error, then assert the cache returns the same
// error on the second call.
func TestEnsureCloned_FailureCached(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	// Make the parent unwritable so the mktemp-based clone fails fast
	// without touching the network.
	parent := filepath.Join(tmp, "readonly")
	if err := os.MkdirAll(parent, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(parent, 0o755) })

	store := gitstore.New(nil)
	cache, err := blobcache.New(filepath.Join(tmp, "blobs"), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(store, cache, filepath.Join(parent, "repos"), "", nil)

	r := m.Get("owner", "repo")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err1 := r.EnsureCloned(ctx)
	if err1 == nil {
		t.Fatal("expected first EnsureCloned to fail")
	}
	err2 := r.EnsureCloned(ctx)
	if err2 == nil {
		t.Fatal("expected cached failure on second call")
	}
	// Cached error should be identical to the first call (no fresh
	// clone attempt).
	if err1.Error() != err2.Error() {
		t.Errorf("cached err mismatch:\n first:  %v\n second: %v", err1, err2)
	}
}

// errTest is a trivial error type that lets tests feed arbitrary
// strings into classifyCloneError without constructing fake exec
// failures.
type errTest string

func (e errTest) Error() string { return string(e) }

// TestRepoBlobSize covers the oid->size backfill used by the FUSE
// layer when a blobless clone left tree entries at SizeState="unknown".
func TestRepoBlobSize(t *testing.T) {
	t.Parallel()
	r := &Repo{}

	if _, ok := r.BlobSize("abc"); ok {
		t.Fatal("BlobSize on empty repo should miss")
	}
	// Defensive: empty oid must not allocate the map or record.
	r.SetBlobSize("", 42)
	if _, ok := r.BlobSize(""); ok {
		t.Fatal("BlobSize('') should always miss")
	}
	if r.blobSizes != nil {
		t.Fatal("SetBlobSize('') should not allocate the map")
	}

	r.SetBlobSize("abc", 12)
	if sz, ok := r.BlobSize("abc"); !ok || sz != 12 {
		t.Fatalf("BlobSize(abc) = (%d, %v), want (12, true)", sz, ok)
	}
	// Overwrite: the second write wins (matches artifact-fs
	// snapshot.UpdateSize: last-write-wins is fine because the blob
	// OID is content-addressed, so a second hydrate that reports a
	// different size would itself be a bug).
	r.SetBlobSize("abc", 99)
	if sz, _ := r.BlobSize("abc"); sz != 99 {
		t.Fatalf("BlobSize(abc) after overwrite = %d, want 99", sz)
	}
}

// helpers

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// gitConfig returns a minimal model.RepoConfig for tests that want to
// poke gitstore against an existing .git directory.
func gitConfig(t *testing.T, gitDir, blobDir string) model.RepoConfig {
	t.Helper()
	return model.RepoConfig{
		ID:           "test",
		GitDir:       gitDir,
		BlobCacheDir: blobDir,
	}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if x == y {
			return true
		}
	}
	return false
}
