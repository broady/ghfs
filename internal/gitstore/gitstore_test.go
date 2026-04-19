package gitstore

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/broady/ghfs/internal/model"
)

func TestResolveRefAndBuildTreeIndex(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	run(t, "git", "init", "-q", repo)
	os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello"), 0o644)
	run(t, "git", "-C", repo, "add", "README.md")
	run(t, "git", "-C", repo, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "-q", "-m", "init")

	cfg := model.RepoConfig{ID: "x", GitDir: filepath.Join(repo, ".git")}
	store := New(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oid, err := store.ResolveRef(ctx, cfg, "HEAD")
	if err != nil {
		t.Fatalf("ResolveRef: %v", err)
	}
	if oid == "" {
		t.Fatal("empty OID")
	}
	nodes, err := store.BuildTreeIndex(ctx, cfg, oid)
	if err != nil {
		t.Fatalf("BuildTreeIndex: %v", err)
	}
	found := false
	for _, n := range nodes {
		if n.Path == "README.md" {
			found = true
			if n.Type != "file" {
				t.Fatalf("expected type file, got %q", n.Type)
			}
		}
	}
	if !found {
		t.Fatal("expected README.md in tree")
	}
}

func TestBlobToCacheBinarySafe(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	run(t, "git", "init", "-q", repo)
	// File with binary content including NULs and a trailing newline.
	payload := []byte{0x00, 0x01, 0x02, 0xff, 'l', 'i', 'n', 'e', '\n'}
	os.WriteFile(filepath.Join(repo, "bin"), payload, 0o644)
	run(t, "git", "-C", repo, "add", "bin")
	run(t, "git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "init")

	cfg := model.RepoConfig{ID: "x", GitDir: filepath.Join(repo, ".git"), BlobCacheDir: filepath.Join(tmp, "cache")}
	store := New(nil)
	ctx := context.Background()
	oid, _ := store.ResolveRef(ctx, cfg, "HEAD")
	nodes, _ := store.BuildTreeIndex(ctx, cfg, oid)
	var blobOID string
	for _, n := range nodes {
		if n.Path == "bin" {
			blobOID = n.ObjectOID
		}
	}
	if blobOID == "" {
		t.Fatal("no blob OID for bin")
	}
	dst := filepath.Join(tmp, "cache", blobOID)
	size, err := store.BlobToCache(ctx, cfg, blobOID, dst)
	if err != nil {
		t.Fatalf("BlobToCache: %v", err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", size, len(payload))
	}
	got, _ := os.ReadFile(dst)
	if string(got) != string(payload) {
		t.Fatalf("content mismatch; got %v, want %v", got, payload)
	}
}

func TestBuildTreeIndexNonASCIIPaths(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	run(t, "git", "init", "-q", repo)
	os.WriteFile(filepath.Join(repo, "café.txt"), []byte("latte"), 0o644)
	os.WriteFile(filepath.Join(repo, "日本語.md"), []byte("hello"), 0o644)
	run(t, "git", "-C", repo, "add", ".")
	run(t, "git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "non-ascii")

	cfg := model.RepoConfig{ID: "x", GitDir: filepath.Join(repo, ".git")}
	store := New(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oid, err := store.ResolveRef(ctx, cfg, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := store.BuildTreeIndex(ctx, cfg, oid)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	for _, n := range nodes {
		paths[n.Path] = true
	}
	if !paths["café.txt"] {
		t.Fatalf("expected café.txt; got %v", paths)
	}
	if !paths["日本語.md"] {
		t.Fatalf("expected 日本語.md; got %v", paths)
	}
}

func TestResolveRefTag(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	run(t, "git", "init", "-q", repo)
	os.WriteFile(filepath.Join(repo, "f.txt"), []byte("x"), 0o644)
	run(t, "git", "-C", repo, "add", ".")
	run(t, "git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "init")
	run(t, "git", "-C", repo, "tag", "v1.0")

	cfg := model.RepoConfig{ID: "x", GitDir: filepath.Join(repo, ".git")}
	store := New(nil)
	ctx := context.Background()
	oid, err := store.ResolveRef(ctx, cfg, "v1.0")
	if err != nil {
		t.Fatalf("ResolveRef(v1.0): %v", err)
	}
	if len(oid) < 40 {
		t.Fatalf("short OID: %q", oid)
	}
}

func TestResolveRefUnknown(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	run(t, "git", "init", "-q", repo)
	os.WriteFile(filepath.Join(repo, "f.txt"), []byte("x"), 0o644)
	run(t, "git", "-C", repo, "add", ".")
	run(t, "git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "init")

	cfg := model.RepoConfig{ID: "x", GitDir: filepath.Join(repo, ".git"), RemoteURL: ""}
	store := New(nil)
	ctx := context.Background()
	_, err := store.ResolveRef(ctx, cfg, "this-ref-definitely-does-not-exist")
	if err == nil {
		t.Fatal("expected error for unknown ref")
	}
}

func TestCredentialEnvEscapesSingleQuotes(t *testing.T) {
	t.Parallel()
	safeURL, env := credentialEnv("https://user:p@ss'word@github.com/org/repo.git")
	if safeURL == "" {
		t.Fatal("expected non-empty safe URL")
	}
	if strings.Contains(safeURL, "p@ss") {
		t.Fatalf("safe URL should not contain password: %s", safeURL)
	}
	found := false
	for _, e := range env {
		if strings.HasPrefix(e, "GIT_CONFIG_VALUE_0=") {
			found = true
			val := strings.TrimPrefix(e, "GIT_CONFIG_VALUE_0=")
			if strings.Contains(val, "p@ss'word") {
				t.Fatalf("unescaped password in helper: %s", val)
			}
			if !strings.Contains(val, `'\''`) {
				t.Fatalf("expected escaped single quote in helper, got: %s", val)
			}
		}
	}
	if !found {
		t.Fatal("expected GIT_CONFIG_VALUE_0 in env")
	}
}

func TestCredentialEnvNoCredentials(t *testing.T) {
	t.Parallel()
	safeURL, env := credentialEnv("https://github.com/org/repo.git")
	if safeURL != "https://github.com/org/repo.git" {
		t.Fatalf("expected unchanged URL, got %s", safeURL)
	}
	if len(env) != 0 {
		t.Fatalf("expected no env vars, got %v", env)
	}
}

func TestCredentialEnvTokenAsUsername(t *testing.T) {
	t.Parallel()
	safeURL, env := credentialEnv("https://ghp_abc123@github.com/org/repo.git")
	if strings.Contains(safeURL, "ghp_abc123") {
		t.Fatalf("token should be stripped from safe URL: %s", safeURL)
	}
	if len(env) == 0 {
		t.Fatal("expected credential helper env vars")
	}
}

func TestSetBatchPoolSizeUpdatesExistingAndNewPools(t *testing.T) {
	t.Parallel()
	store := New(nil)
	first := store.getPool("/tmp/repo-a.git", nil)
	if first.maxSize != 4 {
		t.Fatalf("initial pool maxSize = %d, want 4", first.maxSize)
	}
	store.SetBatchPoolSize(12)
	if first.maxSize != 12 {
		t.Fatalf("updated existing pool maxSize = %d, want 12", first.maxSize)
	}
	second := store.getPool("/tmp/repo-b.git", nil)
	if second.maxSize != 12 {
		t.Fatalf("new pool maxSize = %d, want 12", second.maxSize)
	}
}

// TestCloneBlobless_Local verifies CloneBlobless against a local
// source repo — no network dependency, but covers the --no-single-branch
// path.
func TestCloneBlobless_Local(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	source := filepath.Join(tmp, "src")
	run(t, "git", "init", "-q", source)
	os.WriteFile(filepath.Join(source, "f.txt"), []byte("x"), 0o644)
	run(t, "git", "-C", source, "add", ".")
	run(t, "git", "-C", source, "-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "init")

	gitDir := filepath.Join(tmp, "dest", "git")
	cfg := model.RepoConfig{ID: "x", Name: "x", RemoteURL: source, GitDir: gitDir}
	store := New(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.CloneBlobless(ctx, cfg); err != nil {
		t.Fatalf("CloneBlobless: %v", err)
	}
	if _, err := os.Stat(gitDir); err != nil {
		t.Fatalf("clone dir missing: %v", err)
	}
	// Second call is a no-op.
	if err := store.CloneBlobless(ctx, cfg); err != nil {
		t.Fatalf("CloneBlobless second call: %v", err)
	}
}

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}
}
