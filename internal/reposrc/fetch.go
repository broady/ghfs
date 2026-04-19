package reposrc

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Fetch refreshes all remote-tracking refs from origin and clears the
// in-memory tree cache so the next ResolveTree call rebuilds against
// post-fetch state. The SIGUSR1 handler in main.go calls this on every
// cloned repo.
//
// Note: this does NOT invalidate already-materialized FUSE inodes — the
// kernel caches them for entryTimeout. New lookups/readdirs will see
// fresh data; existing cached directory listings may be stale.
//
// No-op if the repo hasn't been cloned yet this session.
func (r *Repo) Fetch(ctx context.Context) error {
	r.mu.Lock()
	cloned := r.cloned
	r.mu.Unlock()
	if !cloned {
		return nil
	}
	if err := r.Manager.Store.Fetch(ctx, r.Config); err != nil {
		return fmt.Errorf("fetch %s: %w", r.Config.Name, err)
	}
	r.mu.Lock()
	r.trees = map[string]*Tree{}
	r.mu.Unlock()
	return nil
}

// FetchAll runs Fetch across every cloned repo with at most concurrency
// goroutines in flight, each bounded by perRepoTimeout. Best-effort:
// per-repo errors are logged and swallowed (a 404 on one repo must not
// block refreshing the rest). Blocks until all workers complete or ctx
// is cancelled.
//
// A sequential loop (the v0 SIGUSR1 path) held every subsequent repo
// hostage behind the slowest git fetch — a cold fetch against a large
// repo on a flaky link could stall the whole refresh for multiple
// minutes. Fanning out turns total time into roughly max(perRepoTimeout,
// ceil(N/concurrency) * typical_fetch_time) at the cost of more parallel
// git processes; concurrency=4 matches the default hydrator / cat-file
// pool sizes elsewhere in the tree.
func (m *Manager) FetchAll(ctx context.Context, concurrency int, perRepoTimeout time.Duration) {
	fanOutRepos(ctx, m.ListClones(), concurrency, func(ctx context.Context, r *Repo) {
		fetchCtx := ctx
		var cancel context.CancelFunc
		if perRepoTimeout > 0 {
			fetchCtx, cancel = context.WithTimeout(ctx, perRepoTimeout)
			defer cancel()
		}
		if err := r.Fetch(fetchCtx); err != nil {
			m.Logger.Warn("reposrc: fetch failed", "repo", r.Config.Name, "error", err)
			return
		}
		m.Logger.Info("reposrc: fetched", "repo", r.Config.Name)
	})
}

// fanOutRepos runs fn on each repo with at most concurrency goroutines
// in flight. Blocks until every fn returns or ctx is cancelled (a
// cancelled ctx skips not-yet-started workers; already-running workers
// receive the cancelled ctx as their argument and must respect it
// themselves).
//
// Factored out so FetchAll's concurrency invariants are unit-testable
// against a synthetic fn with a counter — the real Fetch path touches
// git and is awkward to probe for bound violations.
func fanOutRepos(ctx context.Context, repos []*Repo, concurrency int, fn func(context.Context, *Repo)) {
	if len(repos) == 0 {
		return
	}
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, r := range repos {
		wg.Add(1)
		go func(r *Repo) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			fn(ctx, r)
		}(r)
	}
	wg.Wait()
}
