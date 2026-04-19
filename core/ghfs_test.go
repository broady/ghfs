package core

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-github/v60/github"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/broady/ghfs/internal/model"
	"github.com/broady/ghfs/internal/reposrc"
)

func TestParseRepoAndRef_PlainName(t *testing.T) {
	t.Parallel()
	repo, ref, err := ParseRepoAndRef("ghfs")
	if err != nil {
		t.Fatalf("ParseRepoAndRef: %v", err)
	}
	if repo != "ghfs" {
		t.Errorf("repo = %q, want %q", repo, "ghfs")
	}
	if ref != "" {
		t.Errorf("ref = %q, want empty (HEAD)", ref)
	}
}

func TestParseRepoAndRef_WithBranch(t *testing.T) {
	t.Parallel()
	repo, ref, err := ParseRepoAndRef("ghfs@main")
	if err != nil {
		t.Fatalf("ParseRepoAndRef: %v", err)
	}
	if repo != "ghfs" || ref != "main" {
		t.Errorf("got (%q, %q), want (%q, %q)", repo, ref, "ghfs", "main")
	}
}

func TestParseRepoAndRef_WithTag(t *testing.T) {
	t.Parallel()
	repo, ref, err := ParseRepoAndRef("linux@v6.1")
	if err != nil {
		t.Fatalf("ParseRepoAndRef: %v", err)
	}
	if repo != "linux" || ref != "v6.1" {
		t.Errorf("got (%q, %q), want (%q, %q)", repo, ref, "linux", "v6.1")
	}
}

func TestParseRepoAndRef_WithShortSHA(t *testing.T) {
	t.Parallel()
	repo, ref, err := ParseRepoAndRef("go@abc1234")
	if err != nil {
		t.Fatalf("ParseRepoAndRef: %v", err)
	}
	if repo != "go" || ref != "abc1234" {
		t.Errorf("got (%q, %q), want (%q, %q)", repo, ref, "go", "abc1234")
	}
}

func TestParseRepoAndRef_EmptyName(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoAndRef("")
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestParseRepoAndRef_EmptyRef(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoAndRef("ghfs@")
	if err == nil {
		t.Fatal("expected error for empty ref after '@'")
	}
}

func TestParseRepoAndRef_EmptyRepo(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoAndRef("@main")
	if err == nil {
		t.Fatal("expected error for empty repo")
	}
}

func TestParseRepoAndRef_SlashInRef(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoAndRef("ghfs@feature/x")
	if err == nil {
		t.Fatal("expected error for '/' in ref (FUSE can't carry slashes in entry names)")
	}
}

func TestJoinRepoPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		dir, base, want string
	}{
		{".", "file", "file"},
		{"", "file", "file"},
		{"a", "b", "a/b"},
		{"a/b", "c", "a/b/c"},
	}
	for _, c := range cases {
		got := joinRepoPath(c.dir, c.base)
		if got != c.want {
			t.Errorf("joinRepoPath(%q, %q) = %q, want %q", c.dir, c.base, got, c.want)
		}
	}
}

func TestBlockedPathsCoverVCS(t *testing.T) {
	t.Parallel()
	for _, name := range []string{".git", ".svn", ".cvs", ".hg", ".bzr", ".jj"} {
		if !blockedInRepo[name] {
			t.Errorf("blockedInRepo[%q] should be true (VCS metadata must block in-repo)", name)
		}
		if !blockedTopLevel[name] {
			t.Errorf("blockedTopLevel[%q] should be true (VCS metadata must block at owner level too)", name)
		}
	}
}

func TestBlockedTopLevelCoversDesktopProbes(t *testing.T) {
	t.Parallel()
	// Regression: desktop-stack probes fired at mount (see ghfs.log)
	// should not reach the GitHub API.
	for _, name := range []string{
		"autorun.inf", ".xdg-volume-info", ".Trash",
		"AACS", "BDMV", "bdmv", "VIDEO_TS",
		".DS_Store", "System Volume Information",
	} {
		if !isBlockedTopLevel(name) {
			t.Errorf("isBlockedTopLevel(%q) should be true", name)
		}
	}
	// .Trash-<uid> is matched by prefix, not exact membership.
	for _, name := range []string{".Trash-1000", ".Trash-1000-files", ".Trash-0"} {
		if !isBlockedTopLevel(name) {
			t.Errorf("isBlockedTopLevel(%q) should be true (per-uid trash)", name)
		}
	}
	// Must NOT block things a repo could legitimately contain.
	for _, name := range []string{"autorun.inf", "BDMV", "AACS"} {
		if blockedInRepo[name] {
			t.Errorf("blockedInRepo[%q] should be false (legit repo contents must pass through)", name)
		}
	}
}

// Regression: go-fuse's rawBridge only applies NegativeTimeout to
// ENOENT returns when out.EntryTimeout() == 0 (bridge.go:362). If any
// Lookup handler pre-sets EntryTimeout before the existence check, the
// bridge treats the result as a zero-lifetime negative entry and the
// kernel re-enters our handler on every probe — which is what made
// fish_prompt re-hit GitHub for broady/HEAD every render. This guards
// the four Lookup handlers that short-circuit on blocked names: the
// shared path must leave EntryTimeout untouched so the bridge can fill
// in NegativeTimeout from mount options.
func TestLookupENOENTLeavesEntryTimeoutZero(t *testing.T) {
	t.Parallel()

	cases := []struct {
		what string
		call func(out *fuse.EntryOut) syscall.Errno
	}{
		{
			what: "FS.Lookup blocked top-level",
			call: func(out *fuse.EntryOut) syscall.Errno {
				var f FS
				_, errno := f.Lookup(context.Background(), ".jj", out)
				return errno
			},
		},
		{
			what: "User.Lookup blocked top-level",
			call: func(out *fuse.EntryOut) syscall.Errno {
				var u User
				_, errno := u.Lookup(context.Background(), ".jj", out)
				return errno
			},
		},
		{
			what: "Repository.Lookup blocked in-repo",
			call: func(out *fuse.EntryOut) syscall.Errno {
				var r Repository
				_, errno := r.Lookup(context.Background(), ".git", out)
				return errno
			},
		},
		{
			what: "Dir.Lookup blocked in-repo",
			call: func(out *fuse.EntryOut) syscall.Errno {
				var d Dir
				_, errno := d.Lookup(context.Background(), ".git", out)
				return errno
			},
		},
	}
	for _, c := range cases {
		var out fuse.EntryOut
		errno := c.call(&out)
		if errno != syscall.ENOENT {
			t.Errorf("%s: errno = %v, want ENOENT", c.what, errno)
		}
		if out.EntryTimeout() != 0 {
			t.Errorf("%s: EntryTimeout = %v, want 0 (so bridge applies NegativeTimeout)", c.what, out.EntryTimeout())
		}
	}
}

// cachedRootContents must serve pre-populated cache entries without
// touching r.Client — that's the whole point of the cache. Concretely,
// this test uses a nil Client which would panic if cachedRootContents
// fell through to contentsAt. Guards against a future refactor that
// accidentally bypasses the cache check.
func TestRepositoryCachedRootContents_ServesFromCache(t *testing.T) {
	t.Parallel()

	name1, name2 := "foo", "bar"
	kindFile, kindDir := "file", "dir"
	want := []*github.RepositoryContent{
		{Name: &name1, Type: &kindFile},
		{Name: &name2, Type: &kindDir},
	}

	r := &Repository{
		apiRootItems:    want,
		apiRootCachedAt: time.Now(),
	}
	got, errno := r.cachedRootContents(context.Background())
	if errno != 0 {
		t.Fatalf("errno = %v, want 0", errno)
	}
	if len(got) != 2 {
		t.Fatalf("got %d items, want 2", len(got))
	}
	if *got[0].Name != "foo" || *got[1].Name != "bar" {
		t.Errorf("got names (%q, %q), want (foo, bar)", *got[0].Name, *got[1].Name)
	}
}

// An expired cache must re-fetch, not silently hand back stale items.
// We verify the fall-through happens by setting cachedAt to the past
// and observing that contentsAt is invoked — a nil Client makes that
// call panic.
func TestRepositoryCachedRootContents_ExpiredRefetches(t *testing.T) {
	t.Parallel()

	stale := "stale"
	r := &Repository{
		apiRootItems:    []*github.RepositoryContent{{Name: &stale}},
		apiRootCachedAt: time.Now().Add(-repositoryAPITTL - time.Second),
	}
	defer func() {
		if recover() == nil {
			t.Error("expected fall-through to contentsAt (nil Client panic) after cache expiry")
		}
	}()
	_, _ = r.cachedRootContents(context.Background())
}

// TestRepositoryGetattr_StampsPushedAtAsMtime verifies the main
// user-visible effect of the GraphQL metadata prefetch: Repository dir
// inodes report pushedAt as mtime/ctime so `ls -lt ~/github.com/<owner>/`
// orders by activity. Without this the kernel serves uniform zero and
// sort-by-modified is a no-op.
func TestRepositoryGetattr_StampsPushedAtAsMtime(t *testing.T) {
	t.Parallel()

	pushed := time.Date(2026, 4, 19, 3, 16, 17, 0, time.UTC)
	r := &Repository{
		Owner:    "broady",
		Name:     "ghfs",
		pushedAt: pushed,
	}

	var out fuse.AttrOut
	if errno := r.Getattr(context.Background(), nil, &out); errno != 0 {
		t.Fatalf("Getattr: %v", errno)
	}
	if got, want := out.Mtime, uint64(pushed.Unix()); got != want {
		t.Errorf("Mtime = %d, want %d", got, want)
	}
	if got, want := out.Ctime, uint64(pushed.Unix()); got != want {
		t.Errorf("Ctime = %d, want %d", got, want)
	}
	if out.Mode&fuse.S_IFDIR == 0 {
		t.Errorf("Mode = %o, want S_IFDIR bit set", out.Mode)
	}
}

// TestRepositoryGetattr_ZeroPushedAtLeavesMtime checks the honest-
// ignorance case: if we haven't fetched metadata yet (pre-GraphQL-batch
// or on a token that can't hit GraphQL), pushedAt is zero and Getattr
// must leave Mtime at 0 rather than fabricating a time.Now() — otherwise
// `find -mtime -1` would match every untouched repo.
func TestRepositoryGetattr_ZeroPushedAtLeavesMtime(t *testing.T) {
	t.Parallel()

	r := &Repository{Owner: "broady", Name: "never-warmed"}
	var out fuse.AttrOut
	if errno := r.Getattr(context.Background(), nil, &out); errno != 0 {
		t.Fatalf("Getattr: %v", errno)
	}
	if out.Mtime != 0 {
		t.Errorf("Mtime = %d, want 0 (honest unknown)", out.Mtime)
	}
	if out.Ctime != 0 {
		t.Errorf("Ctime = %d, want 0", out.Ctime)
	}
}

// TestRepositoryGetattr_AttrTimeoutTracksReaddirTTL pins the
// short attr-timeout fix: Repository mtime reflects pushedAt, which
// only refreshes when User.Readdir re-paginates the owner's repos
// (at most every userReaddirTTL). If Getattr returned the kernel's
// entryTimeout (30m), `ls -lt` would stay stuck on a stale pushedAt
// for half an hour after a new push landed. Regression guard for
// anyone copy-pasting the Dir/File/User.Getattr template which all
// legitimately use entryTimeout.
func TestRepositoryGetattr_AttrTimeoutTracksReaddirTTL(t *testing.T) {
	t.Parallel()

	r := &Repository{Owner: "broady", Name: "ghfs"}
	var out fuse.AttrOut
	if errno := r.Getattr(context.Background(), nil, &out); errno != 0 {
		t.Fatalf("Getattr: %v", errno)
	}
	if got, want := out.Timeout(), userReaddirTTL; got != want {
		t.Errorf("AttrTimeout = %v, want %v (pushedAt only refreshes at this cadence)", got, want)
	}
	if out.Timeout() >= entryTimeout {
		t.Errorf("AttrTimeout = %v, must be well under entryTimeout=%v to track pushedAt freshness", out.Timeout(), entryTimeout)
	}
}

// TestSetPushedAt_IgnoresZero guards the nil-check discipline in
// setPushedAt: a subsequent call with zero time must not clobber a
// real timestamp written earlier. This matters because the REST
// listing path (buildEntries) and the GraphQL prefetch path
// (applyGraphQLEntry) both write pushedAt — and if one of them runs
// with a missing value, the inode must keep the last good one.
func TestSetPushedAt_IgnoresZero(t *testing.T) {
	t.Parallel()

	existing := time.Date(2026, 4, 19, 0, 0, 0, 0, time.UTC)
	r := &Repository{Owner: "broady", Name: "ghfs", pushedAt: existing}
	r.setPushedAt(time.Time{}) // zero input — must be a no-op
	if !r.pushedAt.Equal(existing) {
		t.Errorf("pushedAt = %v, want %v preserved (zero input must not clobber)", r.pushedAt, existing)
	}

	newer := existing.Add(24 * time.Hour)
	r.setPushedAt(newer)
	if !r.pushedAt.Equal(newer) {
		t.Errorf("pushedAt = %v, want %v (non-zero input must overwrite)", r.pushedAt, newer)
	}
}

// Regression: blobless clones run batch-check with GIT_NO_LAZY_FETCH
// so every blob is reported "missing" and the tree index keeps
// SizeState="unknown", SizeBytes=0. File.Getattr used to report
// size=0 with the full entryTimeout (30min), which page-cache-clipped
// reads to zero bytes — `cat README.md` returned empty despite the
// hydrator fetching the blob successfully.
//
// The fix: when size is unknown, consult the Repo's blob-size backfill
// (written by File.Open after EnsureHydrated) before giving up, and
// set a 1ns attr timeout on miss so the kernel re-Getattr's instead
// of serving the stale zero for half an hour.
func TestFileGetattr_UnknownSizeConsultsRepoBackfillThenShortTimeout(t *testing.T) {
	t.Parallel()

	repo := &reposrc.Repo{}
	f := &File{
		Repo: repo,
		Node: model.BaseNode{
			Path:      "README.md",
			Type:      "file",
			Mode:      0o100644,
			ObjectOID: "deadbeef",
			SizeState: "unknown",
		},
	}

	// Before backfill: size=0, attr timeout 1ns (force kernel re-Getattr).
	var a fuse.AttrOut
	if errno := f.Getattr(context.Background(), nil, &a); errno != 0 {
		t.Fatalf("Getattr: %v", errno)
	}
	if a.Size != 0 {
		t.Errorf("pre-backfill Size = %d, want 0", a.Size)
	}
	if a.Timeout() == 0 {
		t.Error("pre-backfill Timeout = 0, bridge would apply 30min default — reads would clip to zero for 30min")
	}
	if a.Timeout() >= entryTimeout {
		t.Errorf("pre-backfill Timeout = %v, want tiny so kernel re-calls Getattr", a.Timeout())
	}

	// After backfill: size is real, timeout is entryTimeout.
	repo.SetBlobSize("deadbeef", 1234)
	var b fuse.AttrOut
	if errno := f.Getattr(context.Background(), nil, &b); errno != 0 {
		t.Fatalf("Getattr (post-backfill): %v", errno)
	}
	if b.Size != 1234 {
		t.Errorf("post-backfill Size = %d, want 1234", b.Size)
	}
	if b.Timeout() != entryTimeout {
		t.Errorf("post-backfill Timeout = %v, want %v", b.Timeout(), entryTimeout)
	}
}

// Symlink.Getattr mirrors File.Getattr for the unknown-size backfill
// path. Symlinks are less critical (Readlink is idempotent regardless
// of cached size) but `ls -l` shows the target length from Getattr
// so we still want it to converge on the real size.
func TestSymlinkGetattr_UnknownSizeUsesBackfillAndShortTimeout(t *testing.T) {
	t.Parallel()

	repo := &reposrc.Repo{}
	s := &Symlink{
		Repo: repo,
		Node: model.BaseNode{
			Path:      "link",
			Type:      "symlink",
			Mode:      0o120000,
			ObjectOID: "beef",
			SizeState: "unknown",
		},
	}

	var a fuse.AttrOut
	if errno := s.Getattr(context.Background(), nil, &a); errno != 0 {
		t.Fatalf("Getattr: %v", errno)
	}
	if a.Size != 0 {
		t.Errorf("pre-backfill Size = %d, want 0", a.Size)
	}
	if a.Timeout() == 0 || a.Timeout() >= entryTimeout {
		t.Errorf("pre-backfill Timeout = %v, want tiny non-zero", a.Timeout())
	}

	repo.SetBlobSize("beef", 11)
	var b fuse.AttrOut
	if errno := s.Getattr(context.Background(), nil, &b); errno != 0 {
		t.Fatalf("Getattr (post-backfill): %v", errno)
	}
	if b.Size != 11 {
		t.Errorf("post-backfill Size = %d, want 11", b.Size)
	}
	if b.Timeout() != entryTimeout {
		t.Errorf("post-backfill Timeout = %v, want %v", b.Timeout(), entryTimeout)
	}
}

// Regression for the `cat README.md → empty` bug: every Lookup return
// path must populate out.Attr via fillLookupAttr. The go-fuse bridge
// does NOT auto-fill attrs when the parent has a NodeLookuper (see
// bridge.go:lookup — it calls the parent's Lookup and trusts it); if
// we leave out.Attr at the zero value, the bridge's setEntryOutTimeout
// stamps the 30-minute mount-option AttrTimeout onto a size=0, and the
// kernel caches that for half an hour and serves empty reads out of
// the page cache without ever calling Read.
//
// This test exercises the helper directly; that's enough to pin the
// contract, because every Lookup path funnels through it.
func TestFillLookupAttr_PopulatesSizeAndTimeout(t *testing.T) {
	t.Parallel()

	repo := &reposrc.Repo{}
	repo.SetBlobSize("oid-known", 1024)

	cases := []struct {
		name        string
		embed       fs.InodeEmbedder
		wantSize    uint64
		wantTimeout time.Duration
	}{
		{
			name: "file with tree-known size",
			embed: &File{Repo: &reposrc.Repo{}, Node: model.BaseNode{
				Path: "a.go", Type: "file", Mode: 0o100644,
				ObjectOID: "x", SizeState: "known", SizeBytes: 42,
			}},
			wantSize: 42, wantTimeout: entryTimeout,
		},
		{
			name: "file with backfilled size",
			embed: &File{Repo: repo, Node: model.BaseNode{
				Path: "b.go", Type: "file", Mode: 0o100644,
				ObjectOID: "oid-known", SizeState: "unknown",
			}},
			wantSize: 1024, wantTimeout: entryTimeout,
		},
		{
			name: "file with still-unknown size gets short timeout",
			embed: &File{Repo: &reposrc.Repo{}, Node: model.BaseNode{
				Path: "c.go", Type: "file", Mode: 0o100644,
				ObjectOID: "missing", SizeState: "unknown",
			}},
			wantSize: 0, wantTimeout: unknownSizeAttrTimeout,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var out fuse.EntryOut
			fillLookupAttr(context.Background(), c.embed, &out)
			if out.Size != c.wantSize {
				t.Errorf("out.Size = %d, want %d — kernel would cache wrong size and clip reads", out.Size, c.wantSize)
			}
			if got := out.AttrTimeout(); got != c.wantTimeout {
				t.Errorf("out.AttrTimeout = %v, want %v", got, c.wantTimeout)
			}
		})
	}
}

// Safety check for the embedder that has no Getattr: fillLookupAttr
// must be a no-op, leaving out.Attr at its zero value for the bridge
// to fill in via setEntryOutTimeout defaults. Matters because we call
// this from FS.Lookup / User.Lookup on User / Repository inodes, and
// those do have Getattr — but a future embedder that doesn't
// shouldn't crash the lookup path.
func TestFillLookupAttr_SkipsWhenNoGetattr(t *testing.T) {
	t.Parallel()
	var out fuse.EntryOut
	// naked struct implementing neither Getattr nor the full embedder
	// interface — pass a bare InodeEmbedder by wrapping fs.Inode.
	type bare struct{ fs.Inode }
	fillLookupAttr(context.Background(), &bare{}, &out)
	if out.Size != 0 {
		t.Errorf("out.Size = %d, want 0 (no-op expected)", out.Size)
	}
	if out.AttrTimeout() != 0 {
		t.Errorf("out.AttrTimeout = %v, want 0 (bridge default should take over)", out.AttrTimeout())
	}
}

// File.Getattr for known-size nodes (the contents-API pre-clone path)
// must still report the real size with the full entryTimeout — the
// backfill path is purely additive and must not regress the happy path.
func TestFileGetattr_KnownSizeUnchanged(t *testing.T) {
	t.Parallel()

	f := &File{
		Repo: &reposrc.Repo{},
		Node: model.BaseNode{
			Path:      "x.txt",
			Type:      "file",
			Mode:      0o100644,
			ObjectOID: "abc",
			SizeState: "known",
			SizeBytes: 42,
		},
	}
	var a fuse.AttrOut
	if errno := f.Getattr(context.Background(), nil, &a); errno != 0 {
		t.Fatalf("Getattr: %v", errno)
	}
	if a.Size != 42 {
		t.Errorf("Size = %d, want 42", a.Size)
	}
	if a.Timeout() != entryTimeout {
		t.Errorf("Timeout = %v, want %v", a.Timeout(), entryTimeout)
	}
}

// Negative-cache must short-circuit too. GitHub's /contents/ returns
// 404 for repos with no commits; previously we re-probed every `ls`,
// costing ~110ms/empty repo serialised by the kernel's opendir walk.
// Now ENOENT is cached for repositoryAPITTL like a success.
func TestRepositoryCachedRootContents_CachesENOENT(t *testing.T) {
	t.Parallel()

	// Client intentionally nil — would panic if fall-through happened.
	r := &Repository{
		apiRootErrno:    syscall.ENOENT,
		apiRootCachedAt: time.Now(),
	}
	items, errno := r.cachedRootContents(context.Background())
	if errno != syscall.ENOENT {
		t.Fatalf("errno = %v, want ENOENT", errno)
	}
	if items != nil {
		t.Errorf("items = %v, want nil on cached ENOENT", items)
	}
}

// TestRepositoryCachedRootContents_Singleflight: N concurrent callers
// on the same cold cache must fire exactly one /contents/ request —
// the rest coalesce via apiRootInflight and share the leader's result.
//
// Regression guard for holding apiMu across the network call: if
// someone reverts to the old design, N callers will serialise on
// apiMu and each observe a fresh cache miss... actually no, they'd
// still coalesce (the second caller locks after the first stored),
// so this test proves the singleflight specifically via the inflight
// channel. The "apiMu not held across I/O" property is covered by
// TestRepositoryCachedRootContents_GetattrNotBlocked below.
func TestRepositoryCachedRootContents_Singleflight(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		// Block until the test releases us — gives all N callers a
		// chance to queue before the leader's request completes.
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"README.md","type":"file","path":"README.md"}]`)
	}))
	t.Cleanup(srv.Close)

	client := github.NewClient(srv.Client())
	base, _ := url.Parse(srv.URL + "/")
	client.BaseURL = base

	r := &Repository{
		Owner:  "broady",
		Name:   "ghfs",
		Client: client,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	const N = 8
	var wg sync.WaitGroup
	results := make([][]*github.RepositoryContent, N)
	errnos := make([]syscall.Errno, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errnos[i] = r.cachedRootContents(context.Background())
		}(i)
	}

	// Give goroutines a moment to all queue up on the inflight channel,
	// then release the leader.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := requests.Load(); got != 1 {
		t.Errorf("server saw %d requests, want 1 (singleflight should coalesce)", got)
	}
	for i, items := range results {
		if errnos[i] != 0 {
			t.Errorf("call %d: errno = %v, want 0", i, errnos[i])
			continue
		}
		if len(items) != 1 || items[0].Name == nil || *items[0].Name != "README.md" {
			t.Errorf("call %d: got %+v, want one item named README.md", i, items)
		}
	}
}

// TestRepositoryCachedRootContents_ApiMuNotHeldAcrossIO: while a
// contentsAt call is in flight for a Repository, a sibling
// apiMu.Lock() elsewhere must complete promptly. If apiMu were held
// across the /contents/ call (the old behaviour), this test would
// time out. Pinning the invariant at this commit avoids regressions
// when later commits add more apiMu-protected fields (Getattr's
// pushedAt snapshot, etc.).
func TestRepositoryCachedRootContents_ApiMuNotHeldAcrossIO(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"README.md","type":"file","path":"README.md"}]`)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		// If a test assertion aborts before we release, unblock the
		// server so httptest.Close doesn't hang on an in-flight handler.
		select {
		case <-release:
		default:
			close(release)
		}
	})

	client := github.NewClient(srv.Client())
	base, _ := url.Parse(srv.URL + "/")
	client.BaseURL = base

	r := &Repository{
		Owner:  "broady",
		Name:   "ghfs",
		Client: client,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	// Start a contentsAt that will block inside the server.
	leaderDone := make(chan struct{})
	go func() {
		_, _ = r.cachedRootContents(context.Background())
		close(leaderDone)
	}()
	// Let the leader claim apiRootInflight and drop apiMu.
	time.Sleep(50 * time.Millisecond)

	// Sibling lock: must acquire immediately because apiMu is NOT
	// held across the /contents/ call.
	lockDone := make(chan struct{})
	go func() {
		r.apiMu.Lock()
		r.apiMu.Unlock()
		close(lockDone)
	}()
	select {
	case <-lockDone:
		// Good: apiMu was free while the leader was mid-flight.
	case <-time.After(2 * time.Second):
		t.Fatal("sibling apiMu.Lock blocked behind in-flight contentsAt — apiMu is being held across network I/O")
	}
	close(release)
	<-leaderDone
}

// TestRepositoryCachedRootContents_CtxCancelDuringWait: a waiter
// whose context is cancelled while the leader is still in flight
// must return EINTR promptly rather than hanging until the leader
// finishes.
func TestRepositoryCachedRootContents_CtxCancelDuringWait(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	client := github.NewClient(srv.Client())
	base, _ := url.Parse(srv.URL + "/")
	client.BaseURL = base

	r := &Repository{
		Owner:  "broady",
		Name:   "ghfs",
		Client: client,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	// Leader: blocks inside the server.
	go r.cachedRootContents(context.Background())
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan syscall.Errno, 1)
	go func() {
		_, errno := r.cachedRootContents(ctx)
		done <- errno
	}()
	// Give the waiter time to reach its <-inflight select.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case errno := <-done:
		if errno != syscall.EINTR {
			t.Errorf("errno = %v, want EINTR", errno)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ctx cancel did not unblock waiter — singleflight wait must select on ctx.Done")
	}
}
