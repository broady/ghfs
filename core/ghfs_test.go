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
	"github.com/hanwen/go-fuse/v2/fuse"
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
