package reposrc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/broady/ghfs/internal/hydrator"
	"github.com/broady/ghfs/internal/model"
)

// cloneFailureBackoff is how long EnsureCloned returns the cached
// failure error without re-running `git clone`. Short enough that a
// user who just created/granted access to a repo sees it appear
// quickly; long enough to stop a shell that retries `ls` on ENOENT
// from launching clone subprocesses in a tight loop.
//
// Same window for transient errors (network / 5xx) and NotFoundError
// (404 / auth failure): tuning them separately is premature. If we
// ever want a longer cache for 404s specifically, split here.
const cloneFailureBackoff = 30 * time.Second

// EnsureCloned makes sure the blobless clone exists on disk and the
// hydrator Service is running. Safe to call repeatedly from many
// goroutines — only the first caller pays the clone cost; subsequent
// callers return immediately.
//
// The actual clone runs outside r.mu (it can take seconds-to-minutes)
// under a dedicated cloneMu so we don't block readers that only need
// to check the cloned flag.
//
// Error handling: a failed clone is cached for cloneFailureBackoff so
// a shell that retries `ls` on ENOENT doesn't launch a clone per call.
// The cached error preserves its type — NotFoundError for true 404 /
// auth failures (mapped to ENOENT by the FUSE layer), plain errors
// for transient network failures (mapped to EIO) so the user sees
// "I/O error" rather than a misleading "No such file or directory".
func (r *Repo) EnsureCloned(ctx context.Context) error {
	r.mu.Lock()
	if r.cloned {
		r.mu.Unlock()
		return nil
	}
	if err := r.cachedCloneFailureLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()

	r.cloneMu.Lock()
	defer r.cloneMu.Unlock()

	// Recheck under r.mu: a racing goroutine may have cloned or cached
	// a failure while we were waiting on cloneMu.
	r.mu.Lock()
	if r.cloned {
		r.mu.Unlock()
		return nil
	}
	if err := r.cachedCloneFailureLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()

	r.Manager.Logger.Info("reposrc: cloning", "repo", r.Config.Name)
	cloneErr := r.Manager.Store.CloneBlobless(ctx, r.Config)
	if cloneErr != nil {
		// Don't cache context cancellation — the caller went away
		// but the repo itself is probably fine; next attempt should
		// re-run the clone rather than echoing back a stale ctx err.
		if ctx.Err() != nil {
			return cloneErr
		}
		err := classifyCloneError(r.Config.Name, cloneErr)
		r.mu.Lock()
		r.cloneFailUntil = time.Now().Add(cloneFailureBackoff)
		r.cloneFailErr = err
		r.mu.Unlock()
		return err
	}

	hyd := hydrator.New(r.Manager.Store)
	// Wire blobcache accounting: every successful hydrate adds size to
	// the shared budget; the cache evicts as needed.
	cache := r.Manager.Cache
	hyd.SetOnHydrated(func(_ model.RepoID, oid string, size int64) {
		cache.Added(oid, size)
	})
	hyd.Start(r.Manager.HydratorWorkers, r.Config)

	r.mu.Lock()
	r.hyd = hyd
	r.cloned = true
	// Clear any stale negative cache now that the clone succeeded.
	r.cloneFailUntil = time.Time{}
	r.cloneFailErr = nil
	r.mu.Unlock()
	return nil
}

// cachedCloneFailureLocked returns the cached clone error if we're
// still inside the backoff window, else nil. Must be called with r.mu
// held.
func (r *Repo) cachedCloneFailureLocked() error {
	if r.cloneFailErr == nil {
		return nil
	}
	if time.Now().Before(r.cloneFailUntil) {
		return r.cloneFailErr
	}
	return nil
}

// classifyCloneError wraps err as *NotFoundError iff git's stderr
// indicates the repo genuinely doesn't exist / isn't accessible.
// Network and transport errors are returned as-is so the FUSE layer
// maps them to EIO rather than ENOENT.
//
// GitHub returns 404 for both truly-missing and private-no-access
// repos (it refuses to confirm existence without auth), so auth
// failures are treated as NotFound too: from the user's perspective
// "I can't see this repo" is indistinguishable from "it doesn't
// exist", and presenting it as ENOENT matches `ls`'s natural output.
func classifyCloneError(name string, err error) error {
	// Match set is narrow on purpose: reposrc only clones via HTTPS
	// (buildCloneURL always returns https://…), so ssh-specific strings
	// ("ERROR: Permission to x/y denied") aren't reachable. Keeping the
	// list tight avoids mis-classifying transient errors that happen to
	// contain the word "denied" in an unrelated context.
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "repository not found"),
		strings.Contains(msg, "does not exist"),
		strings.Contains(msg, "authentication failed"),
		strings.Contains(msg, "could not read username"):
		return &NotFoundError{Msg: fmt.Sprintf("clone %s: %s", name, err)}
	}
	return fmt.Errorf("clone %s: %w", name, err)
}
