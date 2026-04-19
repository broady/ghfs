// Package reposrc owns per-repository state: a blobless clone, its
// hydrator Service, and one in-memory tree per resolved ref.
//
// A FUSE Repository node obtains its *Repo from a Manager keyed by
// (owner, name). Different "<repo>@<ref>" views of the same repository
// share the same clone directory and hydrator, but carry separate
// trees.
package reposrc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/broady/ghfs/internal/blobcache"
	"github.com/broady/ghfs/internal/gitstore"
	"github.com/broady/ghfs/internal/hydrator"
	"github.com/broady/ghfs/internal/model"
)

// NotFoundError indicates a repo or ref does not exist.
type NotFoundError struct {
	Msg string
}

func (e *NotFoundError) Error() string { return e.Msg }

// IsNotFound reports whether err (or any in its chain) is a NotFoundError.
func IsNotFound(err error) bool {
	var nf *NotFoundError
	return errors.As(err, &nf)
}

// Manager holds the set of repositories ghfs has touched this session.
// One Manager per ghfs process.
type Manager struct {
	Store    *gitstore.Store
	Cache    *blobcache.Cache
	Logger   *slog.Logger
	ReposDir string // parent directory for per-repo clones, e.g. ~/.cache/ghfs/repos
	Token    string // GitHub PAT, or empty for anonymous

	// HydratorWorkers is the number of worker goroutines per Repo.
	HydratorWorkers int

	mu    sync.Mutex
	repos map[string]*Repo // "<owner>/<repo>" -> Repo
}

// NewManager constructs a Manager. Callers should set fields after
// construction (e.g., HydratorWorkers) before first use.
func NewManager(store *gitstore.Store, cache *blobcache.Cache, reposDir, token string, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		Store:           store,
		Cache:           cache,
		Logger:          logger,
		ReposDir:        reposDir,
		Token:           token,
		HydratorWorkers: 4,
		repos:           map[string]*Repo{},
	}
}

// Get returns the *Repo for (owner, name), creating its in-memory
// bookkeeping lazily. The on-disk clone is NOT created until
// EnsureCloned is called.
func (m *Manager) Get(owner, name string) *Repo {
	key := owner + "/" + name
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.repos[key]; ok {
		return r
	}
	gitDir := filepath.Join(m.ReposDir, owner, name, "git")
	cfg := model.RepoConfig{
		ID:           model.RepoID(key),
		Name:         key,
		RemoteURL:    buildCloneURL(owner, name, m.Token),
		GitDir:       gitDir,
		BlobCacheDir: m.Cache.Dir,
	}
	r := &Repo{
		Owner:   owner,
		Name:    name,
		Manager: m,
		Config:  cfg,
		trees:   map[string]*Tree{},
	}
	m.repos[key] = r
	return r
}

// ListClones returns a snapshot of all repos whose clones have been
// materialised this session. Used by the SIGUSR1 refresh handler.
func (m *Manager) ListClones() []*Repo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Repo, 0, len(m.repos))
	for _, r := range m.repos {
		r.mu.Lock()
		cloned := r.cloned
		r.mu.Unlock()
		if cloned {
			out = append(out, r)
		}
	}
	return out
}

// Close tears down hydrators for every managed repo.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.repos {
		r.mu.Lock()
		if r.hyd != nil {
			r.hyd.Stop()
		}
		r.mu.Unlock()
	}
	m.repos = nil
}

// Repo is the per-repository state. Concurrent-safe.
type Repo struct {
	Owner   string
	Name    string
	Manager *Manager
	Config  model.RepoConfig

	// mu guards cloned, hyd, trees — short critical sections only.
	mu     sync.Mutex
	cloned bool              // true once EnsureCloned succeeded
	hyd    *hydrator.Service // built lazily, first EnsureCloned
	trees  map[string]*Tree  // ref -> tree

	// cloneMu serialises long-running clone operations so mu isn't
	// held for minutes. Only EnsureCloned takes this.
	cloneMu sync.Mutex

	// cloneFailUntil and cloneFailErr implement a short-lived negative
	// cache so a repeatedly-failing clone doesn't re-run `git clone` on
	// every FUSE op. Guarded by mu. See EnsureCloned for the retry /
	// backoff policy.
	cloneFailUntil time.Time
	cloneFailErr   error

	// blobSizesMu guards blobSizes.
	blobSizesMu sync.RWMutex
	// blobSizes backfills real byte sizes for blobs whose tree-index
	// entry was left SizeState="unknown" by gitstore.batchResolveSizes
	// (blobless clones run batch-check with GIT_NO_LAZY_FETCH=1, so
	// every blob comes back "missing" and keeps SizeBytes=0). The
	// hydrator learns the true size as a side effect of materialising
	// the blob; File.Open / Symlink.Readlink write it here so later
	// File.Getattr calls — including the one the kernel issues right
	// after open when the file's AttrTimeout is zero — see the real
	// size and stop clipping reads at the stale zero from the tree.
	blobSizes map[string]int64 // ObjectOID -> size
}

// SetBlobSize records size as the true byte length of the blob with
// oid. Called by the FUSE layer after the hydrator materialises a
// blob, so subsequent stats on any path that references the same blob
// see the real size without a full tree re-index.
//
// No-op on empty oid (defensive: the tree index leaves ObjectOID=""
// for implicit directories).
func (r *Repo) SetBlobSize(oid string, size int64) {
	if oid == "" {
		return
	}
	r.blobSizesMu.Lock()
	if r.blobSizes == nil {
		r.blobSizes = map[string]int64{}
	}
	r.blobSizes[oid] = size
	r.blobSizesMu.Unlock()
}

// BlobSize returns a previously-recorded size for oid. ok is false
// if SetBlobSize was never called for this oid (either never hydrated,
// or the tree already knows the size and the FUSE layer didn't bother
// recording it).
func (r *Repo) BlobSize(oid string) (size int64, ok bool) {
	if oid == "" {
		return 0, false
	}
	r.blobSizesMu.RLock()
	size, ok = r.blobSizes[oid]
	r.blobSizesMu.RUnlock()
	return size, ok
}

// Tree is an in-memory view of the repository at a specific commit.
type Tree struct {
	// RefName is the user-supplied ref or "" for default HEAD.
	RefName string
	// OID is the resolved commit OID.
	OID string
	// Nodes is path -> BaseNode for every path present in the tree.
	// Root is stored under ".".
	Nodes map[string]model.BaseNode
	// Children is path -> sorted child names (basenames, not paths).
	Children map[string][]string
}

// ResolveTree ensures a tree is built for ref and returns it. ref may
// be "" (HEAD), a branch name, tag name, or commit SHA.
func (r *Repo) ResolveTree(ctx context.Context, ref string) (*Tree, error) {
	r.mu.Lock()
	if t, ok := r.trees[ref]; ok {
		r.mu.Unlock()
		return t, nil
	}
	r.mu.Unlock()

	if err := r.EnsureCloned(ctx); err != nil {
		return nil, err
	}

	oid, err := r.Manager.Store.ResolveRef(ctx, r.Config, ref)
	if err != nil {
		return nil, &NotFoundError{Msg: fmt.Sprintf("resolve ref %q for %s: %s", ref, r.Config.Name, err)}
	}
	baseNodes, err := r.Manager.Store.BuildTreeIndex(ctx, r.Config, oid)
	if err != nil {
		return nil, fmt.Errorf("build tree index %s@%s: %w", r.Config.Name, oid, err)
	}
	t := buildTree(ref, oid, baseNodes)

	r.mu.Lock()
	// Someone may have raced us — prefer the existing entry.
	if existing, ok := r.trees[ref]; ok {
		r.mu.Unlock()
		return existing, nil
	}
	r.trees[ref] = t
	r.mu.Unlock()
	return t, nil
}

// Hydrator returns the hydrator service. EnsureCloned must have been
// called first; otherwise this returns nil.
func (r *Repo) Hydrator() *hydrator.Service {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hyd
}

// IsCloned reports whether the blobless clone has been successfully
// materialised. Callers use this to choose between tree-backed access
// (free, already paid) and the API fallback (cheap per-call, avoids
// the 500ms+ clone cost for shells that probe every subdirectory).
func (r *Repo) IsCloned() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cloned
}

// buildTree folds the flat []BaseNode slice into path- and
// children-indexed maps.
func buildTree(ref, oid string, baseNodes []model.BaseNode) *Tree {
	t := &Tree{
		RefName:  ref,
		OID:      oid,
		Nodes:    make(map[string]model.BaseNode, len(baseNodes)),
		Children: map[string][]string{},
	}
	for _, n := range baseNodes {
		t.Nodes[n.Path] = n
	}
	for _, n := range baseNodes {
		if n.Path == "." {
			continue
		}
		parent := filepath.Dir(n.Path)
		if parent == "/" {
			parent = "."
		}
		base := filepath.Base(n.Path)
		t.Children[parent] = append(t.Children[parent], base)
	}
	return t
}

// buildCloneURL constructs an HTTPS clone URL. The token (if any) is
// embedded as the user component so gitstore.credentialEnv picks it up
// and passes it via a credential helper rather than leaking to `ps`.
func buildCloneURL(owner, name, token string) string {
	if token != "" {
		return fmt.Sprintf("https://%s@github.com/%s/%s.git", token, owner, name)
	}
	return fmt.Sprintf("https://github.com/%s/%s.git", owner, name)
}

