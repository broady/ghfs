// Package hydrator turns a priority-queued stream of HydrationTasks into
// materialised blobs in a content-addressed cache. Multiple concurrent
// readers requesting the same OID are coalesced onto a single fetch.
//
// Lifted near-verbatim from github.com/cloudflare/artifact-fs/internal/hydrator.
// Differences from the upstream:
//   - cachePathFor shards by oid[:2] so the shared ghfs cache stays
//     navigable (256 top-level dirs).
//   - The upstream sibling-based overlay hooks are absent; ghfs is
//     read-only.
package hydrator

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/broady/ghfs/internal/model"
)

// BlobFetcher is the interface the hydrator calls to materialise a blob.
// gitstore.Store implements it.
type BlobFetcher interface {
	BlobToCache(ctx context.Context, repo model.RepoConfig, objectOID string, dstPath string) (size int64, err error)
	VerifyBlob(ctx context.Context, repo model.RepoConfig, objectOID string, cachePath string) (ok bool, err error)
}

// OnHydratedFunc is invoked after every successful blob fetch. ghfs
// uses it to account bytes added to the shared blob cache for eviction.
type OnHydratedFunc func(repoID model.RepoID, objectOID string, size int64)

// Service owns the hydrator's priority queue, worker pool, and
// inflight-dedup maps for one repo's clone.
type Service struct {
	fetcher    BlobFetcher
	mu         sync.Mutex
	pq         priorityQueue
	wait       inflight[result]
	verifying  inflight[verifyResult]
	started    bool
	stopOnce   sync.Once
	stopCh     chan struct{}
	workReady  chan struct{} // signalled when new work is enqueued
	onHydrated OnHydratedFunc
	verified   map[string]struct{}
}

type result struct {
	cachePath string
	size      int64
	err       error
}

type verifyResult struct {
	ok  bool
	err error
}

// New constructs a Service. Call Start before Enqueue / EnsureHydrated.
func New(fetcher BlobFetcher) *Service {
	return &Service{
		fetcher:   fetcher,
		wait:      newInflight[result](),
		verifying: newInflight[verifyResult](),
		stopCh:    make(chan struct{}),
		workReady: make(chan struct{}, 1),
		verified:  map[string]struct{}{},
	}
}

// SetOnHydrated registers a callback invoked after each successful
// blob fetch.
func (s *Service) SetOnHydrated(fn OnHydratedFunc) {
	s.mu.Lock()
	s.onHydrated = fn
	s.mu.Unlock()
}

// signalWork performs a non-blocking send on workReady to wake a worker.
func (s *Service) signalWork() {
	select {
	case s.workReady <- struct{}{}:
	default: // already signalled, workers will drain the queue
	}
}

// Start spins up `workers` goroutines pinned to repo.
func (s *Service) Start(workers int, repo model.RepoConfig) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	for i := 0; i < workers; i++ {
		go s.worker(repo)
	}
}

// Stop terminates workers and fails any waiters that were mid-flight.
func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.wait.closeAll(result{err: errors.New("hydrator stopped")})
		s.verifying.closeAll(verifyResult{err: errors.New("hydrator stopped")})
	})
}

// Enqueue adds a task without waiting for it to complete. Useful for
// speculative pre-fetching (not used in ghfs v1).
func (s *Service) Enqueue(task model.HydrationTask) {
	s.mu.Lock()
	heap.Push(&s.pq, &taskItem{task: task})
	s.mu.Unlock()
	s.signalWork()
}

// EnsureHydrated blocks until node's blob is present in the cache. If
// the cache already has a valid file (size matches, or hash verifies),
// returns immediately. Otherwise enqueues an explicit-read priority
// task and waits for the worker.
func (s *Service) EnsureHydrated(ctx context.Context, repo model.RepoConfig, node model.BaseNode) (cachePath string, size int64, err error) {
	cachePath = cachePathFor(repo, node.ObjectOID)
	if size, ok, err := s.validateCachedBlob(ctx, repo, cachePath, node); err != nil {
		return "", 0, err
	} else if ok {
		return cachePath, size, nil
	}
	key := taskKey(repo.ID, node.ObjectOID)
	ch := make(chan result, 1)
	s.mu.Lock()
	first := s.wait.add(key, ch)
	if first {
		heap.Push(&s.pq, &taskItem{task: explicitReadTask(repo.ID, node.Path, node.ObjectOID)})
	}
	s.mu.Unlock()
	if first {
		s.signalWork()
	}

	select {
	case <-ctx.Done():
		// Drop our channel so the worker doesn't send to an abandoned
		// channel nobody reads.
		s.mu.Lock()
		s.wait.remove(key, ch)
		s.mu.Unlock()
		return "", 0, ctx.Err()
	case r := <-ch:
		return r.cachePath, r.size, r.err
	}
}

func (s *Service) validateCachedBlob(ctx context.Context, repo model.RepoConfig, cachePath string, node model.BaseNode) (size int64, ok bool, err error) {
	st, err := os.Stat(cachePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if node.SizeState == "known" && st.Size() != node.SizeBytes {
		if err := s.removeInvalidCacheFile(cachePath, repo.ID, node.ObjectOID); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}
	key := taskKey(repo.ID, node.ObjectOID)
	if s.isVerified(key) {
		return st.Size(), true, nil
	}
	ok, err = s.verifyBlobOnce(ctx, key, func(verifyCtx context.Context) (bool, error) {
		return s.fetcher.VerifyBlob(verifyCtx, repo, node.ObjectOID, cachePath)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, false, err
		}
		return 0, false, nil
	}
	if !ok {
		if err := s.removeInvalidCacheFile(cachePath, repo.ID, node.ObjectOID); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}
	return st.Size(), true, nil
}

func (s *Service) isVerified(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.verified[key]
	return ok
}

func (s *Service) markVerified(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verified[key] = struct{}{}
}

func (s *Service) clearVerified(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.verified, key)
}

func (s *Service) removeInvalidCacheFile(cachePath string, repoID model.RepoID, objectOID string) error {
	if err := os.Remove(cachePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.clearVerified(taskKey(repoID, objectOID))
	return nil
}

func (s *Service) verifyBlobOnce(ctx context.Context, key string, verify func(context.Context) (bool, error)) (bool, error) {
	s.mu.Lock()
	if _, ok := s.verified[key]; ok {
		s.mu.Unlock()
		return true, nil
	}
	ch := make(chan verifyResult, 1)
	first := s.verifying.add(key, ch)
	s.mu.Unlock()

	if first {
		go s.runVerification(key, verify)
	}
	return s.awaitVerification(ctx, key, ch)
}

func (s *Service) awaitVerification(ctx context.Context, key string, ch chan verifyResult) (bool, error) {
	select {
	case <-ctx.Done():
		s.mu.Lock()
		s.verifying.remove(key, ch)
		s.mu.Unlock()
		return false, ctx.Err()
	case r := <-ch:
		return r.ok, r.err
	}
}

func (s *Service) runVerification(key string, verify func(context.Context) (bool, error)) {
	verifyCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-s.stopCh:
			cancel()
		case <-verifyCtx.Done():
		}
	}()

	ok, err := verify(verifyCtx)
	r := verifyResult{ok: ok, err: err}

	s.mu.Lock()
	if err == nil && ok {
		s.verified[key] = struct{}{}
	}
	waiters := s.verifying.take(key)
	s.mu.Unlock()

	notifyWaiters(waiters, r)
}

// QueueDepth returns the number of queued tasks for a given repo.
func (s *Service) QueueDepth(repoID model.RepoID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := 0
	for _, item := range s.pq {
		if item.task.RepoID == repoID {
			c++
		}
	}
	return c
}

func (s *Service) worker(repo model.RepoConfig) {
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.workReady:
			// Drain the queue: process all available items before
			// waiting for the next signal. Re-signal if items remain
			// so other workers can help.
			for {
				if !s.step(repo) {
					break
				}
				s.signalWork()
			}
		}
	}
}

// step pops and processes one item from the queue. Returns true if an
// item was processed, false if the queue was empty.
func (s *Service) step(repo model.RepoConfig) bool {
	s.mu.Lock()
	if len(s.pq) == 0 {
		s.mu.Unlock()
		return false
	}
	item := heap.Pop(&s.pq).(*taskItem)
	key := taskKey(item.task.RepoID, item.task.ObjectOID)
	waits := s.wait.take(key)
	s.mu.Unlock()

	cachePath := cachePathFor(repo, item.task.ObjectOID)
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		notifyWaiters(waits, result{err: err})
		return true
	}
	// Timeout context derived from stopCh so stuck blob fetches don't
	// block a worker forever.
	fetchCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	go func() {
		select {
		case <-s.stopCh:
			cancel()
		case <-fetchCtx.Done():
		}
	}()
	size, err := s.fetcher.BlobToCache(fetchCtx, repo, item.task.ObjectOID, cachePath)
	if err != nil {
		notifyWaiters(waits, result{err: fmt.Errorf("hydrate %s: %w", item.task.Path, err)})
		return true
	}
	s.markVerified(taskKey(item.task.RepoID, item.task.ObjectOID))
	notifyWaiters(waits, result{cachePath: cachePath, size: size, err: nil})
	s.mu.Lock()
	fn := s.onHydrated
	s.mu.Unlock()
	if fn != nil {
		fn(item.task.RepoID, item.task.ObjectOID, size)
	}
	return true
}

func taskKey(repoID model.RepoID, oid string) string {
	return string(repoID) + ":" + oid
}

// cachePathFor is ghfs-specific: blobs are stored in a shared
// content-addressed cache sharded by the first two hex digits of the
// OID. With ~50 KB average blob size and a 1 GB budget that's ~80
// files per shard.
func cachePathFor(repo model.RepoConfig, oid string) string {
	if len(oid) >= 2 {
		return filepath.Join(repo.BlobCacheDir, oid[:2], oid)
	}
	return filepath.Join(repo.BlobCacheDir, oid)
}

func explicitReadTask(repoID model.RepoID, path string, oid string) model.HydrationTask {
	return model.HydrationTask{
		RepoID:     repoID,
		Path:       path,
		ObjectOID:  oid,
		Priority:   PriorityExplicitRead,
		Reason:     "explicit read",
		EnqueuedAt: time.Now(),
	}
}

// Priority levels — higher wins. An explicit user read beats any
// speculative pre-fetch.
const (
	PriorityExplicitRead = 1000
	PrioritySibling      = 800
	PriorityBootstrap    = 700
	PriorityLikelyText   = 500
	PriorityNearbyCode   = 400
	PriorityBinary       = 100
)

// ClassifyPriority picks a priority for speculative pre-fetching based
// on the file's path. (ghfs v1 doesn't pre-fetch, but this is retained
// for parity and future use.)
func ClassifyPriority(path string) int {
	base := filepath.Base(path)
	ext := filepath.Ext(path)
	switch {
	case base == "README" || base == "README.md" || base == "LICENSE" || base == "Makefile" || base == ".gitignore":
		return PriorityBootstrap
	case base == "go.mod" || base == "go.sum" || base == "Cargo.toml" || base == "package.json" || base == "pnpm-lock.yaml" || base == "pyproject.toml":
		return PriorityBootstrap
	case isCodeExtension(ext):
		return PriorityLikelyText
	case ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" || ext == ".zip" || ext == ".pdf" || ext == ".tar" || ext == ".gz" || ext == ".mp4" || ext == ".mov" || ext == ".avi":
		return PriorityBinary
	default:
		return PriorityNearbyCode
	}
}

func isCodeExtension(ext string) bool {
	switch ext {
	case ".go", ".rs", ".zig", ".py", ".ts", ".tsx", ".js", ".jsx",
		".java", ".c", ".cc", ".cpp", ".h", ".hpp",
		".json", ".yaml", ".yml", ".toml", ".md":
		return true
	}
	return false
}

type taskItem struct {
	task  model.HydrationTask
	index int
}

type priorityQueue []*taskItem

func (p priorityQueue) Len() int { return len(p) }
func (p priorityQueue) Less(i, j int) bool {
	if p[i].task.Priority == p[j].task.Priority {
		return p[i].task.EnqueuedAt.Before(p[j].task.EnqueuedAt)
	}
	return p[i].task.Priority > p[j].task.Priority
}
func (p priorityQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index, p[j].index = i, j
}
func (p *priorityQueue) Push(x any) {
	item := x.(*taskItem)
	item.index = len(*p)
	*p = append(*p, item)
}
func (p *priorityQueue) Pop() any {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*p = old[:n-1]
	return item
}
