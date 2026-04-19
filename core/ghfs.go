// Package core implements the ghfs FUSE filesystem as a tree of go-fuse
// nodes backed by a git blobless clone.
//
// Hierarchy:
//
//	FS          ← mountpoint root (e.g. ~/github.com)
//	  User      ← one dir per GitHub user/org; populated via REST Users/Orgs/Repos.List
//	    Repository ← one dir per "<repo>" or "<repo>@<ref>". First access triggers
//	                  a blobless clone + in-memory tree build.
//	      Dir   ← subdirectories within the repo (tree-backed, no network)
//	        File ← regular files; Open() streams the blob via the hydrator
//
// Directory listings within repos are served entirely from the in-memory
// tree index. File reads trigger a cat-file fetch (first access) and
// then stream from the shared content-addressed blob cache.
package core

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/go-github/v60/github"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/broady/ghfs/internal/hydrator"
	"github.com/broady/ghfs/internal/model"
	"github.com/broady/ghfs/internal/reposrc"
)

// blockedInRepo are names we refuse to look up inside a repo tree.
// GitHub never serves these, and letting them through wastes a
// Contents-API call or forces a clone. Scoped narrowly to VCS
// metadata — a repo could legitimately contain a file named
// "autorun.inf" or a directory named "BDMV", so those are only
// blocked at the mount/owner level (see blockedTopLevel).
var blockedInRepo = map[string]bool{
	".git": true,
	".svn": true,
	".cvs": true,
	".hg":  true,
	".bzr": true,
	".jj":  true, // Jujutsu — probed by `jj root`/`jj status` on every shell prompt in jj-aware setups.
}

// blockedTopLevel are names we refuse to resolve as owners or repos.
// These get probed relentlessly by the desktop stack the moment we
// mount — gvfs/udisks/tracker scan for autorun + removable-media
// markers, file managers look for .Trash and macOS metadata dirs,
// and if the user ever inserts a Blu-ray the kernel probes for its
// filesystem structure. Each probe would otherwise cost a
// Users.Get / Repositories.Get round trip per mount.
//
// blockedInRepo is a strict subset — VCS metadata is blocked
// everywhere; desktop noise is blocked only at the top two levels.
var blockedTopLevel = map[string]bool{
	// VCS metadata (inherited — these never appear as GitHub owners
	// or repo names either).
	".git": true,
	".svn": true,
	".cvs": true,
	".hg":  true,
	".bzr": true,
	".jj":  true,

	// Removable-media / autorun probes.
	"autorun.inf":               true,
	".xdg-volume-info":          true,
	".Trash":                    true,
	"lost+found":                true,
	".fseventsd":                true,
	".Spotlight-V100":           true,
	".DocumentRevisions-V100":   true,
	".TemporaryItems":           true,
	".DS_Store":                 true,
	"System Volume Information": true,

	// Blu-ray / DVD filesystem structure probes.
	"AACS":     true,
	"BDMV":     true,
	"bdmv":     true,
	"VIDEO_TS": true,
	"AUDIO_TS": true,
}

// isBlockedTopLevel also catches dynamic names like .Trash-1000
// (per-uid trash dirs) without enumerating every possible uid.
func isBlockedTopLevel(name string) bool {
	if blockedTopLevel[name] {
		return true
	}
	// .Trash-<uid> and .Trash-<uid>-files/info variants.
	if strings.HasPrefix(name, ".Trash-") {
		return true
	}
	return false
}

// entryTimeout is how long the kernel should cache a successful
// lookup/dirent. 30 minutes matches the previous ghfs behaviour; the
// SIGUSR1 refresh handler (see main.go) doesn't invalidate these but
// a subsequent natural Lookup after SIGUSR1 will re-read the tree.
const entryTimeout = 30 * time.Minute

// userReaddirTTL is how long User.Readdir reuses its last successful
// repo listing before re-paginating Repositories.List. FUSE invokes
// Readdir on every shell enumeration (the kernel dcache only caches
// Lookup results, not dirents), so without this every `ls
// ~/github.com/<owner>/` pays ceil(N/100) sequential round trips.
// Tradeoff: for 60s after pushing a new repo, `ls` won't show it —
// but `cd <newrepo>` still works because Lookup falls through to
// Repositories.Get on cache miss.
const userReaddirTTL = 60 * time.Second

// repositoryAPITTL is how long Repository caches its root Contents-API
// listing (the /repos/<owner>/<repo>/contents/ call). eza --icons
// opens every sibling repo to pick project-type icons, so without a
// per-inode cache each `ls ~/github.com/<owner>/` costs one round
// trip per repo — N serial calls because the kernel dispatches each
// child opendir separately. 60s matches userReaddirTTL and GitHub's
// typical max-age=60 on Contents responses.
const repositoryAPITTL = 60 * time.Second

// userPrefetchConcurrency bounds how many child repos User.Readdir
// warms in parallel. Deliberately smaller than GitHub's concurrent-
// request ceiling so a single ghfs process doesn't monopolise the
// token's rate budget.
const userPrefetchConcurrency = 16

// userPrefetchSuppressDur debounces prefetches. Tab completion, fish
// autosuggest, and rapid-fire `ls` all trigger User.Readdir; we only
// want to kick the concurrent prefetch once per window.
const userPrefetchSuppressDur = 30 * time.Second

// userPrefetchTimeout caps a single prefetch run. The run can't
// inherit the Readdir context (that cancels as soon as Readdir
// returns, killing every in-flight prefetch) so we use a detached
// context bounded by this.
const userPrefetchTimeout = 30 * time.Second

// FS is the filesystem root — one dir per GitHub user/org. It owns no
// git state; that lives in the reposrc.Manager.
type FS struct {
	fs.Inode

	// Client is used for the metadata listing (Users.Get,
	// Organizations.List, Repositories.List). All content reads go
	// through Repos.
	Client *github.Client

	// Repos materialises + tracks per-repo state (clones, trees,
	// hydrators).
	Repos *reposrc.Manager

	Logger *slog.Logger

	// authedLogin is the GitHub login of the token owner, resolved
	// lazily via Users.Get(ctx, ""). Used by User.Readdir to pick the
	// right endpoint: the authed user's own dir needs
	// ListByAuthenticatedUser (so private repos appear); everyone else
	// falls through to ListByOrg or the public-only Users.Repositories
	// listing. Empty string means anonymous or resolution failed.
	authedOnce  sync.Once
	authedLogin string
}

// authLogin returns the token owner's GitHub login, resolving it on
// first call. Safe to call concurrently; error cases return "".
func (f *FS) authLogin(ctx context.Context) string {
	f.authedOnce.Do(func() {
		u, _, err := f.Client.Users.Get(ctx, "")
		if err != nil {
			f.Logger.Debug("fs.authLogin: Users.Get failed (anonymous?)", "error", err)
			return
		}
		if u.Login != nil {
			f.authedLogin = *u.Login
		}
	})
	return f.authedLogin
}

var _ = (fs.NodeLookuper)((*FS)(nil))

// Lookup resolves a top-level owner/org name via Users.Get.
//
// Do NOT pre-set out.SetEntryTimeout here. The rawBridge in go-fuse
// only applies NegativeTimeout on ENOENT if out.EntryTimeout() == 0
// (bridge.go:362). If we pre-set it, every failed lookup leaks a
// zero-lifetime negative dentry and the kernel re-enters this handler
// on every probe — which is exactly what was making every fish_prompt
// render re-hit Users.Get for names like "HEAD". Successful returns
// still get entryTimeout applied by the bridge via setEntryOutTimeout
// (mount options set EntryTimeout=30min in main.go).
func (f *FS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if isBlockedTopLevel(name) {
		return nil, syscall.ENOENT
	}

	if existing := f.GetChild(name); existing != nil {
		return existing, 0
	}

	u, _, err := f.Client.Users.Get(ctx, name)
	if err != nil {
		f.Logger.Error("fs.lookup: Users.Get failed", "user", name, "error", err)
		return nil, syscall.ENOENT
	}

	ownerType := ""
	if u.Type != nil {
		ownerType = *u.Type
	}
	login := ""
	if u.Login != nil {
		login = *u.Login
	}

	user := &User{
		Login:        login,
		OwnerType:    ownerType,
		IsAuthedUser: ownerType == "User" && login != "" && login == f.authLogin(ctx),
		Client:       f.Client,
		Repos:        f.Repos,
		Logger:       f.Logger,
	}
	return f.NewInode(ctx, user, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
}

var _ = (fs.NodeReaddirer)((*FS)(nil))

// Readdir lists the authenticated user plus their orgs. Kept identical
// in shape to the pre-refactor behaviour.
func (f *FS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	var entries []fuse.DirEntry

	if login := f.authLogin(ctx); login != "" {
		entries = append(entries, fuse.DirEntry{Name: login, Mode: fuse.S_IFDIR})
	}

	opts := &github.ListOptions{PerPage: 100}
	for {
		orgs, resp, err := f.Client.Organizations.List(ctx, "", opts)
		if err != nil {
			f.Logger.Error("fs.readdir: Organizations.List failed", "error", err)
			break
		}
		for _, org := range orgs {
			if org.Login != nil {
				entries = append(entries, fuse.DirEntry{Name: *org.Login, Mode: fuse.S_IFDIR})
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return fs.NewListDirStream(entries), 0
}

var _ = (fs.NodeGetattrer)((*FS)(nil))

func (f *FS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.SetTimeout(entryTimeout)
	now := uint64(time.Now().Unix())
	out.Atime, out.Mtime, out.Ctime = now, now, now
	return 0
}

// User is a GitHub user or organisation — one directory containing
// their repos.
type User struct {
	fs.Inode

	Login string
	// OwnerType is "User" or "Organization" (from the Users.Get
	// response). Empty if we couldn't determine it — Readdir falls
	// back to the public Repositories.List in that case.
	OwnerType string
	// IsAuthedUser is true iff Login matches the token owner. Drives
	// Readdir to use ListByAuthenticatedUser so private repos appear.
	IsAuthedUser bool
	Client       *github.Client
	Repos        *reposrc.Manager
	Logger       *slog.Logger

	// knownMu guards knownRepos + readdirDone.
	knownMu sync.RWMutex
	// knownRepos is the set of repo names confirmed to exist for this
	// owner, populated by Readdir and on-demand Lookup (Repositories.Get).
	knownRepos map[string]bool
	// readdirDone is true once Readdir fully paginated. After that,
	// absence from knownRepos is authoritative ENOENT without an API hit.
	readdirDone bool

	// dirMu guards dirNames + dirCachedAt. Separate from knownMu
	// because Readdir's cache hit path only needs the ordered names
	// slice, not the membership map.
	dirMu       sync.Mutex
	dirNames    []string  // last successful listing, in API order
	dirCachedAt time.Time // zero value = no cache yet

	// prefetchMu guards prefetchStartedAt — debounces
	// startPrefetchRepoContents so a rapid sequence of User.Readdir
	// calls doesn't fan out the same 148 concurrent API requests
	// over and over.
	prefetchMu        sync.Mutex
	prefetchStartedAt time.Time
}

var _ = (fs.NodeGetattrer)((*User)(nil))

func (u *User) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.SetTimeout(entryTimeout)
	out.Nlink = 2
	return 0
}

var _ = (fs.NodeReaddirer)((*User)(nil))

// Readdir enumerates the owner's repos. Picks the right endpoint
// based on what we know about the owner:
//
//   - Authenticated user's own dir → ListByAuthenticatedUser
//     (GET /user/repos) with Affiliation="owner,collaborator" so
//     private repos + collab repos appear.
//   - Organization → ListByOrg (GET /orgs/{org}/repos) which returns
//     everything the authed user can see in that org (including
//     private).
//   - Any other user → Repositories.List (GET /users/{login}/repos)
//     which returns public repos only — the GitHub API doesn't
//     expose third-party users' private repos anyway.
//
// Successful listings are cached on the User inode for userReaddirTTL
// so repeated `ls` / shell tab-completion don't re-paginate. Pre-
// populates child inodes to avoid subsequent Lookup calls and records
// known names so Lookup can answer ENOENT locally.
func (u *User) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	started := time.Now()

	// Fast path: recent cached listing.
	u.dirMu.Lock()
	if u.dirNames != nil && time.Since(u.dirCachedAt) < userReaddirTTL {
		names := u.dirNames
		age := time.Since(u.dirCachedAt)
		u.dirMu.Unlock()
		u.Logger.Debug("user.readdir: cache hit",
			"user", u.Login, "count", len(names), "age", age.Round(time.Millisecond))
		entries := u.buildEntries(ctx, names)
		// Kick off the concurrent per-repo Contents warmup. Debounced
		// internally so this is ~free on a hot cache.
		u.startPrefetchRepoContents(names)
		return fs.NewListDirStream(entries), 0
	}
	u.dirMu.Unlock()

	names, pages, errno := u.listRepos(ctx)
	if errno != 0 {
		return nil, errno
	}

	for _, name := range names {
		u.rememberRepo(name)
	}
	u.knownMu.Lock()
	u.readdirDone = true
	u.knownMu.Unlock()

	u.dirMu.Lock()
	u.dirNames = names
	u.dirCachedAt = time.Now()
	u.dirMu.Unlock()

	u.Logger.Debug("user.readdir: listed",
		"user", u.Login,
		"owner_type", u.OwnerType,
		"authed", u.IsAuthedUser,
		"count", len(names),
		"pages", pages,
		"elapsed", time.Since(started).Round(time.Millisecond))

	entries := u.buildEntries(ctx, names)
	// Fire the per-repo Contents warmup *after* child inodes exist so
	// the prefetch can find them via u.GetChild.
	u.startPrefetchRepoContents(names)
	return fs.NewListDirStream(entries), 0
}

// startPrefetchRepoContents kicks off a background warmup of every
// child Repository's root Contents cache. git-aware ls tools (eza
// --icons, lsd, lsd --classic, vscode file explorer) open every
// sibling repo to pick icons or compute project type — which in our
// FS means Repository.Readdir per child and one API round trip per
// child. Kernel serialises those opendirs, so without concurrency
// the user pays N * (network RTT + server latency) on the cold path.
//
// Debounced via prefetchStartedAt: repeated Readdir within
// userPrefetchSuppressDur (tab completion, fish autosuggest) re-use
// the in-flight or recently-completed prefetch. Cache hits inside
// cachedRootContents make repeated warmups idempotent anyway — the
// debouncer just avoids spawning pointless goroutines.
func (u *User) startPrefetchRepoContents(names []string) {
	u.prefetchMu.Lock()
	if time.Since(u.prefetchStartedAt) < userPrefetchSuppressDur {
		u.prefetchMu.Unlock()
		return
	}
	u.prefetchStartedAt = time.Now()
	u.prefetchMu.Unlock()

	// Snapshot names to insulate the goroutine from caller-side mutation.
	snapshot := append([]string(nil), names...)
	go u.prefetchRepoContents(snapshot)
}

// prefetchRepoContents is the body of the debounced background
// prefetch. Populates every child Repository's root-Contents cache
// via a single batched GraphQL query (chunked), falling back to the
// REST fan-out if GraphQL fails wholesale. Errors past the fallback
// are swallowed — a real Readdir will retry on demand.
//
// The GraphQL path replaces what used to be ceil(N/16) serial rounds
// of REST /contents/ calls with 1–ceil(N/50) batched POSTs, shaving
// ~1.3s off a cold `ls ~/github.com/<owner>/` for a ~150-repo user.
// On a warm httpcache the REST path would 304 near-instantly anyway,
// so GraphQL is a pure win on cold paths and a wash on warm ones
// (single POST ≈ 16 parallel conditional GETs). REST fallback keeps
// us at worst status-quo on GraphQL outages / token-scoping issues
// (fine-grained tokens without GraphQL access, etc).
func (u *User) prefetchRepoContents(names []string) {
	started := time.Now()

	// Collect non-cloned Repository inodes. Cloned repos serve
	// Readdir from the local tree, so warming the API cache is
	// pointless — and would poison a Readdir that's about to start
	// returning tree-accurate mode bits.
	repos := make([]*Repository, 0, len(names))
	for _, name := range names {
		child := u.GetChild(name)
		if child == nil {
			continue
		}
		repo, ok := child.Operations().(*Repository)
		if !ok {
			continue
		}
		if repo.Repo.IsCloned() {
			continue
		}
		repos = append(repos, repo)
	}

	if len(repos) == 0 {
		u.Logger.Debug("user.prefetch: nothing to warm (all cloned / empty)",
			"user", u.Login, "total", len(names))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), userPrefetchTimeout)
	defer cancel()

	written, err := u.batchRepoRootContents(ctx, repos)
	if err != nil {
		// Transport-level failure on at least one chunk — log and
		// fall back to the per-repo REST fan-out. Any chunks that
		// DID succeed already wrote their caches, so the fallback
		// only incurs REST calls for the remaining (un-warmed) repos.
		u.Logger.Warn("user.prefetch: graphql batch failed, falling back to REST",
			"user", u.Login,
			"warmed_before_fallback", written,
			"total", len(repos),
			"error", err)
		u.prefetchRepoContentsREST(ctx, repos)
	}

	u.Logger.Debug("user.prefetch: done",
		"user", u.Login,
		"warmed", written,
		"total", len(names),
		"candidates", len(repos),
		"strategy", "graphql",
		"elapsed", time.Since(started).Round(time.Millisecond))
}

// prefetchRepoContentsREST is the legacy per-repo concurrent REST path,
// used only as a fallback when the GraphQL batch call fails. Matches
// the historical concurrency cap (userPrefetchConcurrency) and relies
// on cachedRootContents's mutex to singleflight per-inode. Skips repos
// whose cache was already populated by a partial GraphQL success.
func (u *User) prefetchRepoContentsREST(ctx context.Context, repos []*Repository) {
	sem := make(chan struct{}, userPrefetchConcurrency)
	var wg sync.WaitGroup
	for _, r := range repos {
		// Cheap check: if a prior partial GraphQL success already
		// wrote this inode's cache, skip the REST round trip.
		r.apiMu.Lock()
		fresh := !r.apiRootCachedAt.IsZero() && time.Since(r.apiRootCachedAt) < repositoryAPITTL
		r.apiMu.Unlock()
		if fresh {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(r *Repository) {
			defer wg.Done()
			defer func() { <-sem }()
			// Errors intentionally ignored — on-demand Readdir retries.
			_, _ = r.cachedRootContents(ctx)
		}(r)
	}
	wg.Wait()
}

// buildEntries converts the cached name slice into FUSE dirents and
// pre-creates missing child inodes.
func (u *User) buildEntries(ctx context.Context, names []string) []fuse.DirEntry {
	entries := make([]fuse.DirEntry, 0, len(names))
	for _, name := range names {
		entries = append(entries, fuse.DirEntry{Name: name, Mode: fuse.S_IFDIR})
		if u.GetChild(name) == nil {
			child := u.NewInode(ctx, u.newRepository(name, ""), fs.StableAttr{Mode: fuse.S_IFDIR})
			u.AddChild(name, child, false)
		}
	}
	return entries
}

// listRepos paginates the appropriate GitHub endpoint for this owner
// and returns the ordered repo names plus the number of pages fetched.
// Returns ENOENT on API error (consistent with prior behaviour).
func (u *User) listRepos(ctx context.Context) ([]string, int, syscall.Errno) {
	switch {
	case u.OwnerType == "Organization":
		return u.listOrgRepos(ctx)
	case u.IsAuthedUser:
		return u.listAuthedRepos(ctx)
	default:
		return u.listUserRepos(ctx)
	}
}

// listAuthedRepos hits GET /user/repos with affiliation=owner,collaborator.
// This is the only way to see the authed user's private repos.
func (u *User) listAuthedRepos(ctx context.Context) ([]string, int, syscall.Errno) {
	opts := &github.RepositoryListByAuthenticatedUserOptions{
		Affiliation: "owner,collaborator",
		ListOptions: github.ListOptions{PerPage: 100},
	}
	var names []string
	pages := 0
	for {
		repos, resp, err := u.Client.Repositories.ListByAuthenticatedUser(ctx, opts)
		if err != nil {
			u.Logger.Error("user.readdir: ListByAuthenticatedUser failed", "user", u.Login, "error", err)
			return nil, pages, syscall.ENOENT
		}
		pages++
		for _, repo := range repos {
			// The owner field distinguishes repos owned by the user
			// vs repos they collaborate on. We only surface repos
			// the user actually owns under their own dir — collab
			// repos appear under their real owner's dir.
			if repo.Name == nil || repo.Owner == nil || repo.Owner.Login == nil {
				continue
			}
			if *repo.Owner.Login != u.Login {
				continue
			}
			names = append(names, *repo.Name)
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return names, pages, 0
}

// listOrgRepos hits GET /orgs/{org}/repos which returns all repos
// visible to the authed user (including private).
func (u *User) listOrgRepos(ctx context.Context) ([]string, int, syscall.Errno) {
	opts := &github.RepositoryListByOrgOptions{ListOptions: github.ListOptions{PerPage: 100}}
	var names []string
	pages := 0
	for {
		repos, resp, err := u.Client.Repositories.ListByOrg(ctx, u.Login, opts)
		if err != nil {
			u.Logger.Error("user.readdir: ListByOrg failed", "org", u.Login, "error", err)
			return nil, pages, syscall.ENOENT
		}
		pages++
		for _, repo := range repos {
			if repo.Name != nil {
				names = append(names, *repo.Name)
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return names, pages, 0
}

// listUserRepos hits GET /users/{login}/repos. Returns public repos
// only — the API never exposes a third-party user's private repos.
func (u *User) listUserRepos(ctx context.Context) ([]string, int, syscall.Errno) {
	opts := &github.RepositoryListOptions{ListOptions: github.ListOptions{PerPage: 100}}
	var names []string
	pages := 0
	for {
		repos, resp, err := u.Client.Repositories.List(ctx, u.Login, opts)
		if err != nil {
			u.Logger.Error("user.readdir: Repositories.List failed", "user", u.Login, "error", err)
			return nil, pages, syscall.ENOENT
		}
		pages++
		for _, repo := range repos {
			if repo.Name != nil {
				names = append(names, *repo.Name)
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return names, pages, 0
}

// rememberRepo records that owner/name exists.
func (u *User) rememberRepo(name string) {
	u.knownMu.Lock()
	if u.knownRepos == nil {
		u.knownRepos = map[string]bool{}
	}
	u.knownRepos[name] = true
	u.knownMu.Unlock()
}

// repoExists reports whether name is a real repository under this
// owner. It uses the Readdir-populated set when possible; otherwise
// (first access before any listing) it issues a single Repositories.Get.
// Any API error is treated as "does not exist" — consistent with how
// FS.Lookup handles Users.Get failures.
func (u *User) repoExists(ctx context.Context, name string) bool {
	u.knownMu.RLock()
	_, known := u.knownRepos[name]
	done := u.readdirDone
	u.knownMu.RUnlock()
	if known {
		return true
	}
	if done {
		return false
	}
	_, _, err := u.Client.Repositories.Get(ctx, u.Login, name)
	if err != nil {
		u.Logger.Debug("user.lookup: Repositories.Get miss", "repo", u.Login+"/"+name, "error", err)
		return false
	}
	u.rememberRepo(name)
	return true
}

var _ = (fs.NodeLookuper)((*User)(nil))

// Lookup handles plain "<repo>" and "<repo>@<ref>" forms. Verifies the
// repo exists (via the Readdir-populated set, or a one-shot
// Repositories.Get) before returning a positive result. Returning
// ENOENT here lets the kernel negatively cache bogus probes (e.g. jj
// walking up looking for ".jj") so they don't cascade into clone
// attempts for non-existent repos.
func (u *User) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if isBlockedTopLevel(name) {
		return nil, syscall.ENOENT
	}
	// Intentionally no out.SetEntryTimeout here — see FS.Lookup doc
	// comment. Negative returns must leave EntryTimeout at 0 so the
	// bridge applies NegativeTimeout (30min kernel dcache); positive
	// returns get EntryTimeout from mount options automatically.

	if existing := u.GetChild(name); existing != nil {
		return existing, 0
	}

	repoName, ref, err := ParseRepoAndRef(name)
	if err != nil {
		u.Logger.Debug("user.lookup: parse failed", "name", name, "error", err)
		return nil, syscall.ENOENT
	}

	if !u.repoExists(ctx, repoName) {
		return nil, syscall.ENOENT
	}

	return u.NewInode(ctx, u.newRepository(repoName, ref), fs.StableAttr{Mode: fuse.S_IFDIR}), 0
}

// newRepository builds a Repository inode for a (repo, ref) pair.
// Does no network I/O.
func (u *User) newRepository(repoName, ref string) *Repository {
	return &Repository{
		Owner:  u.Login,
		Name:   repoName,
		Ref:    ref,
		Repo:   u.Repos.Get(u.Login, repoName),
		Client: u.Client,
		Logger: u.Logger,
	}
}

// Repository is one directory: "<repo>" (ref="") or "<repo>@<ref>".
// Holds a reference to the shared reposrc.Repo (clone + hydrator) and
// defers tree building to first use.
type Repository struct {
	fs.Inode

	Owner string
	Name  string
	Ref   string // "" means default HEAD

	Repo   *reposrc.Repo
	Client *github.Client // for the pre-clone Contents-API listing path
	Logger *slog.Logger

	// apiMu guards apiRootItems + apiRootErrno + apiRootCachedAt +
	// apiRootInflight — per-inode cache of the root Contents-API
	// listing. eza --icons opens every sibling repo to pick
	// project-type icons; without this each such probe is an HTTP
	// round trip. Callers for different inodes proceed in parallel,
	// which is what lets User.prefetchRepoContents warm N repos
	// concurrently.
	//
	// apiRootErrno captures the last outcome so we can negative-cache
	// ENOENT (empty repo — GitHub's /contents/ returns 404 for repos
	// with no commits). Transient failures (EIO) are deliberately NOT
	// cached; apiRootCachedAt.IsZero() means "no result recorded".
	//
	// Concurrency discipline: apiMu is only ever held across short
	// critical sections (cache read/write) — never across a network
	// call. Concurrent callers for the same inode singleflight via
	// apiRootInflight: the first miss creates the channel, drops
	// apiMu, fires the network call, then re-acquires apiMu to
	// store + close the channel. Subsequent callers observe the
	// channel, drop apiMu, and wait on it, then re-acquire apiMu to
	// read the cached result.
	apiMu           sync.Mutex
	apiRootItems    []*github.RepositoryContent
	apiRootErrno    syscall.Errno
	apiRootCachedAt time.Time
	apiRootInflight chan struct{} // non-nil while a contentsAt call is in flight; closed on completion
}

// cachedRootContents returns the root Contents listing for this
// repo, serving from the per-inode cache when fresh. Concurrent
// callers singleflight via apiRootInflight: only one contentsAt
// call runs; the rest wait and read the shared result. apiMu is
// never held across the network call, so Getattr's pushedAt
// snapshot (and any other short apiMu-critical section) never
// blocks on a slow /contents/ fetch.
//
// Caches BOTH successful results and ENOENT — an empty repo's 404
// is a stable answer (user has to push a first commit for it to
// change), so re-probing it every `ls` wastes ~110ms per empty repo
// serialised by the kernel's opendir walk. EIO is not cached: the
// caller can retry without hammering a broken cache entry.
//
// Subdir (path != "") fetches bypass this and go straight to
// contentsAt — they're rare (only hit when the shell descends into
// an uncloned repo without triggering a full clone) and the root
// listing is what eza/lsd hammer.
func (r *Repository) cachedRootContents(ctx context.Context) ([]*github.RepositoryContent, syscall.Errno) {
	for {
		r.apiMu.Lock()
		if !r.apiRootCachedAt.IsZero() && time.Since(r.apiRootCachedAt) < repositoryAPITTL {
			items, errno := r.apiRootItems, r.apiRootErrno
			r.apiMu.Unlock()
			return items, errno
		}
		// A sibling call is in flight — drop apiMu and wait for it,
		// then loop to re-check the cache. Respects ctx so a client
		// disconnect doesn't hang us on a slow leader.
		if inflight := r.apiRootInflight; inflight != nil {
			r.apiMu.Unlock()
			select {
			case <-inflight:
				continue
			case <-ctx.Done():
				return nil, syscall.EINTR
			}
		}
		// Claim leadership: install the inflight channel, then
		// release apiMu before the network call.
		done := make(chan struct{})
		r.apiRootInflight = done
		r.apiMu.Unlock()

		items, errno := r.contentsAt(ctx, "")

		r.apiMu.Lock()
		// Only record stable outcomes. EIO means transient (network,
		// 5xx) and should retry on the next call.
		if errno == 0 || errno == syscall.ENOENT {
			r.apiRootItems = items
			r.apiRootErrno = errno
			r.apiRootCachedAt = time.Now()
		}
		r.apiRootInflight = nil
		close(done)
		r.apiMu.Unlock()
		return items, errno
	}
}

var _ = (fs.NodeGetattrer)((*Repository)(nil))

func (r *Repository) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.SetTimeout(entryTimeout)
	out.Nlink = 2
	return 0
}

// tree returns the tree for this (repo, ref), lazily cloning and
// indexing on first call.
func (r *Repository) tree(ctx context.Context) (*reposrc.Tree, syscall.Errno) {
	t, err := r.Repo.ResolveTree(ctx, r.Ref)
	if err != nil {
		r.Logger.Error("repository.tree: failed", "repo", r.Owner+"/"+r.Name, "ref", r.Ref, "error", err)
		if reposrc.IsNotFound(err) {
			return nil, syscall.ENOENT
		}
		return nil, syscall.EIO
	}
	return t, 0
}

var _ = (fs.NodeReaddirer)((*Repository)(nil))

// Readdir lists the repo root. If the repo has already been cloned, we
// serve from the in-memory tree; otherwise we fall back to the GitHub
// Contents API so shells that probe one level deeper than the user's
// cursor (fish tab-completion, eza --git, etc.) don't force a clone
// stampede across every sibling repo. Cloning is deferred to the first
// real traversal (Dir.Readdir, File.Open, Symlink.Readlink).
func (r *Repository) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if r.Repo.IsCloned() {
		t, errno := r.tree(ctx)
		if errno != 0 {
			return nil, errno
		}
		return readdirAt(ctx, &r.Inode, t, r.Repo, r.Ref, r.Logger, ".")
	}
	return r.readdirViaAPI(ctx)
}

var _ = (fs.NodeLookuper)((*Repository)(nil))

func (r *Repository) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if blockedInRepo[name] {
		return nil, syscall.ENOENT
	}
	// See FS.Lookup: no pre-set EntryTimeout so the bridge can
	// negative-cache ENOENT returns (e.g. from r.tree() or lookupViaAPI).
	if existing := r.GetChild(name); existing != nil {
		return existing, 0
	}
	if r.Repo.IsCloned() {
		t, errno := r.tree(ctx)
		if errno != 0 {
			return nil, errno
		}
		return lookupAt(ctx, &r.Inode, t, r.Repo, r.Ref, r.Logger, ".", name)
	}
	return r.lookupViaAPI(ctx, name)
}

// readdirViaAPI lists the repo root using Repositories.GetContents.
// Pre-populates child inodes so subsequent Lookups are kernel-cached.
// The API doesn't report git mode bits, so file executability defaults
// to non-executable until the clone lands; we rebuild those details
// tree-accurately on the first real traversal.
func (r *Repository) readdirViaAPI(ctx context.Context) (fs.DirStream, syscall.Errno) {
	items, errno := r.cachedRootContents(ctx)
	if errno != 0 {
		return nil, errno
	}
	entries := make([]fuse.DirEntry, 0, len(items))
	for _, item := range items {
		if item.Name == nil || item.Type == nil {
			continue
		}
		name := *item.Name
		embed := r.newInodeFromContent(item)
		if embed == nil {
			continue
		}
		mode := contentMode(*item.Type)
		entries = append(entries, fuse.DirEntry{Name: name, Mode: mode})
		if r.GetChild(name) == nil {
			inode := r.NewInode(ctx, embed, fs.StableAttr{Mode: mode})
			r.AddChild(name, inode, false)
		}
	}
	return fs.NewListDirStream(entries), 0
}

// lookupViaAPI resolves a single child name via the Contents API.
func (r *Repository) lookupViaAPI(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	// Fetch the full root listing (not just `name`) so we can pre-populate
	// siblings — cheap insurance against the shell following up with
	// lookups for neighbours. Shares the per-inode cache with readdirViaAPI.
	items, errno := r.cachedRootContents(ctx)
	if errno != 0 {
		return nil, errno
	}
	var match *github.RepositoryContent
	for _, item := range items {
		if item.Name == nil {
			continue
		}
		if *item.Name == name {
			match = item
		}
		if r.GetChild(*item.Name) == nil {
			if embed := r.newInodeFromContent(item); embed != nil {
				inode := r.NewInode(ctx, embed, fs.StableAttr{Mode: contentMode(*item.Type)})
				r.AddChild(*item.Name, inode, false)
			}
		}
	}
	if match == nil {
		return nil, syscall.ENOENT
	}
	if child := r.GetChild(name); child != nil {
		return child, 0
	}
	// Shouldn't happen (we just added it) but don't error on a race.
	embed := r.newInodeFromContent(match)
	if embed == nil {
		return nil, syscall.ENOENT
	}
	return r.NewInode(ctx, embed, fs.StableAttr{Mode: contentMode(*match.Type)}), 0
}

// contentsAt fetches the directory listing for a repo-relative path via
// the Contents API. Returns ENOENT on 404, EIO on anything else.
func (r *Repository) contentsAt(ctx context.Context, path string) ([]*github.RepositoryContent, syscall.Errno) {
	opts := &github.RepositoryContentGetOptions{Ref: r.Ref}
	_, dir, resp, err := r.Client.Repositories.GetContents(ctx, r.Owner, r.Name, path, opts)
	if err != nil {
		if resp != nil && resp.StatusCode == 404 {
			return nil, syscall.ENOENT
		}
		r.Logger.Error("repository.contents: failed", "repo", r.Owner+"/"+r.Name, "ref", r.Ref, "path", path, "error", err)
		return nil, syscall.EIO
	}
	return dir, 0
}

// newInodeFromContent builds the right inode embedder from a Contents
// API entry. Metadata (size, OID) is populated from the response so
// Getattr/Open can work without having the tree in hand.
func (r *Repository) newInodeFromContent(item *github.RepositoryContent) fs.InodeEmbedder {
	if item == nil || item.Name == nil || item.Type == nil {
		return nil
	}
	path := *item.Name
	if item.Path != nil {
		path = *item.Path
	}
	size := int64(0)
	if item.Size != nil {
		size = int64(*item.Size)
	}
	oid := ""
	if item.SHA != nil {
		oid = *item.SHA
	}
	switch *item.Type {
	case "dir":
		return &Dir{Repo: r.Repo, Ref: r.Ref, Path: path, Logger: r.Logger}
	case "symlink":
		node := model.BaseNode{
			Path:      path,
			Type:      "symlink",
			Mode:      0o120000,
			ObjectOID: oid,
			SizeBytes: size,
			SizeState: "known",
		}
		return &Symlink{Repo: r.Repo, Ref: r.Ref, Node: node, Logger: r.Logger}
	case "file":
		node := model.BaseNode{
			Path:      path,
			Type:      "file",
			Mode:      0o100644,
			ObjectOID: oid,
			SizeBytes: size,
			SizeState: "known",
		}
		return &File{Repo: r.Repo, Ref: r.Ref, Node: node, Logger: r.Logger}
	default:
		// "submodule" and anything else: skip.
		return nil
	}
}

// contentMode maps a Contents-API type string to FUSE mode bits.
func contentMode(t string) uint32 {
	switch t {
	case "dir":
		return fuse.S_IFDIR
	case "symlink":
		return fuse.S_IFLNK
	default:
		return fuse.S_IFREG
	}
}

// Dir is any subdirectory within a repo. Resolves the tree on demand
// so Dir inodes created during the pre-clone Contents-API listing can
// still answer readdir/lookup once someone actually descends — that
// descent is the signal we use to trigger the (still-lazy) clone.
type Dir struct {
	fs.Inode

	Repo *reposrc.Repo
	Ref  string

	Path   string // path relative to repo root, "." for root
	Logger *slog.Logger
}

var _ = (fs.NodeGetattrer)((*Dir)(nil))

func (d *Dir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.SetTimeout(entryTimeout)
	return 0
}

// tree fetches the tree for this Dir's ref. ResolveTree is a cheap map
// lookup after the first call per (repo, ref), so repeated Dir traversals
// inside a cloned repo don't re-clone.
func (d *Dir) tree(ctx context.Context) (*reposrc.Tree, syscall.Errno) {
	t, err := d.Repo.ResolveTree(ctx, d.Ref)
	if err != nil {
		d.Logger.Error("dir.tree: failed", "path", d.Path, "ref", d.Ref, "error", err)
		if reposrc.IsNotFound(err) {
			return nil, syscall.ENOENT
		}
		return nil, syscall.EIO
	}
	return t, 0
}

var _ = (fs.NodeReaddirer)((*Dir)(nil))

func (d *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	t, errno := d.tree(ctx)
	if errno != 0 {
		return nil, errno
	}
	return readdirAt(ctx, &d.Inode, t, d.Repo, d.Ref, d.Logger, d.Path)
}

var _ = (fs.NodeLookuper)((*Dir)(nil))

func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if blockedInRepo[name] {
		return nil, syscall.ENOENT
	}
	// See FS.Lookup: no pre-set EntryTimeout so ENOENT returns from
	// lookupAt (tree miss) get absorbed by the kernel negative cache.
	if existing := d.GetChild(name); existing != nil {
		return existing, 0
	}
	t, errno := d.tree(ctx)
	if errno != 0 {
		return nil, errno
	}
	return lookupAt(ctx, &d.Inode, t, d.Repo, d.Ref, d.Logger, d.Path, name)
}

// readdirAt is shared by Repository.Readdir and Dir.Readdir. It builds
// DirEntries from the tree's children index and pre-populates child
// inodes so the kernel skips the subsequent Lookup.
func readdirAt(ctx context.Context, parent *fs.Inode, t *reposrc.Tree, repo *reposrc.Repo, ref string, logger *slog.Logger, dirPath string) (fs.DirStream, syscall.Errno) {
	kids := t.Children[dirPath]
	sort.Strings(kids)
	entries := make([]fuse.DirEntry, 0, len(kids))
	for _, child := range kids {
		childPath := joinRepoPath(dirPath, child)
		node, ok := t.Nodes[childPath]
		if !ok {
			continue
		}
		mode := nodeMode(node)
		entries = append(entries, fuse.DirEntry{Name: child, Mode: mode})

		if parent.GetChild(child) == nil {
			inode := parent.NewInode(ctx, newNodeInode(repo, ref, logger, node), fs.StableAttr{Mode: mode})
			parent.AddChild(child, inode, false)
		}
	}
	return fs.NewListDirStream(entries), 0
}

// lookupAt resolves one child name in dirPath against the tree.
func lookupAt(ctx context.Context, parent *fs.Inode, t *reposrc.Tree, repo *reposrc.Repo, ref string, logger *slog.Logger, dirPath, name string) (*fs.Inode, syscall.Errno) {
	childPath := joinRepoPath(dirPath, name)
	node, ok := t.Nodes[childPath]
	if !ok {
		return nil, syscall.ENOENT
	}
	return parent.NewInode(ctx, newNodeInode(repo, ref, logger, node), fs.StableAttr{Mode: nodeMode(node)}), 0
}

// joinRepoPath joins dir + base, producing a clean path relative to
// the repo root. "." + "x" becomes "x", "a" + "b" becomes "a/b".
func joinRepoPath(dir, base string) string {
	if dir == "." || dir == "" {
		return base
	}
	return dir + "/" + base
}

// nodeMode maps a BaseNode to the FUSE mode bits we report.
func nodeMode(n model.BaseNode) uint32 {
	switch n.Type {
	case "dir":
		return fuse.S_IFDIR
	case "symlink":
		return fuse.S_IFLNK
	default:
		return fuse.S_IFREG
	}
}

// newNodeInode constructs the right FUSE node embedding for a tree entry.
func newNodeInode(repo *reposrc.Repo, ref string, logger *slog.Logger, node model.BaseNode) fs.InodeEmbedder {
	switch node.Type {
	case "dir":
		return &Dir{Repo: repo, Ref: ref, Path: node.Path, Logger: logger}
	case "symlink":
		return &Symlink{Repo: repo, Ref: ref, Node: node, Logger: logger}
	default:
		return &File{Repo: repo, Ref: ref, Node: node, Logger: logger}
	}
}

// ensureHydrator returns the repo's hydrator, triggering a clone if the
// repo wasn't materialised yet (pre-clone API-listing path). op and
// path are used only for the error log.
func ensureHydrator(ctx context.Context, repo *reposrc.Repo, ref string, logger *slog.Logger, op, path string) (*hydrator.Service, syscall.Errno) {
	if hyd := repo.Hydrator(); hyd != nil {
		return hyd, 0
	}
	if _, err := repo.ResolveTree(ctx, ref); err != nil {
		logger.Error(op+": clone failed", "path", path, "ref", ref, "error", err)
		if reposrc.IsNotFound(err) {
			return nil, syscall.ENOENT
		}
		return nil, syscall.EIO
	}
	hyd := repo.Hydrator()
	if hyd == nil {
		logger.Error(op+": no hydrator after clone", "path", path)
		return nil, syscall.EIO
	}
	return hyd, 0
}

// File is a regular git blob. Open() forces a hydrate into the shared
// cache and returns a FileHandle that reads from the cache file.
type File struct {
	fs.Inode

	Repo *reposrc.Repo
	Ref  string
	Node model.BaseNode

	Logger *slog.Logger
}

var _ = (fs.NodeGetattrer)((*File)(nil))

func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(f.Node.SizeBytes)
	// 0o100644 / 0o100755 preserved via Mode; fall back to 0644 if git
	// mode isn't something we recognise.
	mode := uint32(0o644)
	if f.Node.Mode&0o100755 == 0o100755 {
		mode = 0o755
	}
	out.Mode = fuse.S_IFREG | mode
	out.SetTimeout(entryTimeout)
	return 0
}

var _ = (fs.NodeOpener)((*File)(nil))

// Open hydrates the blob into the shared cache (if not already there)
// and opens the cache file for streaming reads. Binary-safe — the
// kernel-visible bytes are exactly the git blob's contents. If the
// inode was built from the pre-clone Contents-API listing, this is
// where the clone (and its hydrator) finally gets materialised.
func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Reject writes up-front; v1 is read-only.
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_TRUNC|syscall.O_CREAT) != 0 {
		return nil, 0, syscall.EROFS
	}
	hyd, errno := ensureHydrator(ctx, f.Repo, f.Ref, f.Logger, "file.open", f.Node.Path)
	if errno != 0 {
		return nil, 0, errno
	}
	cachePath, size, err := hyd.EnsureHydrated(ctx, f.Repo.Config, f.Node)
	if err != nil {
		f.Logger.Error("file.open: EnsureHydrated failed", "path", f.Node.Path, "oid", f.Node.ObjectOID, "error", err)
		return nil, 0, syscall.EIO
	}
	file, err := os.Open(cachePath)
	if err != nil {
		f.Logger.Error("file.open: cache open failed", "path", cachePath, "error", err)
		return nil, 0, syscall.EIO
	}
	return &FileHandle{file: file, size: size, logger: f.Logger}, fuse.FOPEN_KEEP_CACHE, 0
}

// FileHandle streams from a backing cache file at an arbitrary offset.
type FileHandle struct {
	file   *os.File
	size   int64
	logger *slog.Logger
}

var _ = (fs.FileReader)((*FileHandle)(nil))
var _ = (fs.FileReleaser)((*FileHandle)(nil))

func (fh *FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= fh.size {
		return fuse.ReadResultData(nil), 0
	}
	// go-fuse's ReadResultFd efficiently splices the cache file into
	// the reply buffer without a user-space copy.
	//
	// (*os.File).Fd() returns a bare uintptr — it does NOT transfer
	// ownership of the descriptor, and the runtime is free to run
	// *os.File's close-on-finalize finalizer as soon as it decides fh
	// is unreachable. The ReadResultFd value only carries the uintptr,
	// so without an explicit KeepAlive the compiler / runtime may
	// consider fh.file dead the instant Fd() returns, GC it, and close
	// the fd before (or during) go-fuse's splice / pread on the reply
	// path. Symptom: sporadic EBADF / empty reads under concurrent load.
	// KeepAlive pins fh (and transitively fh.file) for the duration of
	// Read; the caller's own reference to the handle keeps it alive
	// for the splice that follows.
	res := fuse.ReadResultFd(fh.file.Fd(), off, len(dest))
	runtime.KeepAlive(fh)
	return res, 0
}

// Release closes the backing file when the kernel releases the handle.
func (fh *FileHandle) Release(ctx context.Context) syscall.Errno {
	if err := fh.file.Close(); err != nil {
		fh.logger.Debug("filehandle.release: close error", "error", err)
	}
	return 0
}

// Symlink is a git symlink blob. We hydrate it once (they're small) to
// read the target.
type Symlink struct {
	fs.Inode

	Repo *reposrc.Repo
	Ref  string
	Node model.BaseNode

	Logger *slog.Logger
}

var _ = (fs.NodeGetattrer)((*Symlink)(nil))

func (s *Symlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFLNK | 0o777
	out.Size = uint64(s.Node.SizeBytes)
	out.SetTimeout(entryTimeout)
	return 0
}

var _ = (fs.NodeReadlinker)((*Symlink)(nil))

func (s *Symlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	hyd, errno := ensureHydrator(ctx, s.Repo, s.Ref, s.Logger, "symlink.readlink", s.Node.Path)
	if errno != 0 {
		return nil, errno
	}
	cachePath, _, err := hyd.EnsureHydrated(ctx, s.Repo.Config, s.Node)
	if err != nil {
		s.Logger.Error("symlink.readlink: EnsureHydrated failed", "path", s.Node.Path, "error", err)
		return nil, syscall.EIO
	}
	f, err := os.Open(cachePath)
	if err != nil {
		return nil, syscall.EIO
	}
	defer f.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return nil, syscall.EIO
	}
	return buf.Bytes(), 0
}
