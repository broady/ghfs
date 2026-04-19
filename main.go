// ghfs mounts GitHub as a FUSE filesystem. See README for details.
//
// Architecture (post-gitstore refactor):
//
//  1. REST (go-github) → enumerate owners/orgs + list their repos.
//  2. Per-repo blobless clone (git clone --filter=blob:none
//     --no-single-branch) into ~/.cache/ghfs/repos/<owner>/<repo>/git.
//  3. In-memory tree index (git ls-tree -r -t -z) per (repo, ref).
//  4. On file read: `git cat-file --batch` streams the blob into the
//     shared content-addressed cache (~/.cache/ghfs/blobs/<shard>/<oid>),
//     then FUSE reads from that cache file. Binary-safe; no 1 MB cap.
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/google/go-github/v60/github"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/broady/ghfs/core"
	"github.com/broady/ghfs/internal/blobcache"
	"github.com/broady/ghfs/internal/githubhttp"
	"github.com/broady/ghfs/internal/gitstore"
	"github.com/broady/ghfs/internal/reposrc"
)

var (
	token       = flag.String("token", "", "personal access token")
	anonymous   = flag.Bool("anonymous", false, "use anonymous (unauthenticated) API requests")
	cacheDir    = flag.String("cache-dir", "", "directory for disk cache (default: ~/.cache/ghfs)")
	diskCacheMB = flag.Int("cache-disk-mb", 1024, "shared blob cache size budget in MB")
	batchPoolSz = flag.Int("cat-file-pool", 4, "max persistent `git cat-file --batch` processes per repo")
	hydWorkers  = flag.Int("hydrate-workers", 4, "blob hydration worker goroutines per repo")
	apiMemMB    = flag.Int("api-cache-mem-mb", githubhttp.DefaultMemCacheMB, "in-memory budget (MB) for the GitHub API response cache")
	apiNoCache  = flag.Bool("no-api-cache", false, "disable the GitHub API response cache (always hits the network)")
)

func main() {
	// Logger setup.
	logLevelStr := os.Getenv("GHFS_LOG_LEVEL")
	if logLevelStr == "" {
		logLevelStr = "info"
	}
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(logLevelStr)); err != nil {
		slog.Error("invalid GHFS_LOG_LEVEL", "value", logLevelStr, "error", err)
		logLevel = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})
	slog.SetDefault(slog.New(handler))
	log.SetFlags(0)

	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("path required")
	}
	// Fall back to $GITHUB_TOKEN so callers (e.g. systemd units) can pass the
	// token via env instead of an argv flag, keeping it out of `ps` / cmdline.
	// Unset immediately so spawned child processes don't inherit it.
	if *token == "" {
		if env := os.Getenv("GITHUB_TOKEN"); env != "" {
			*token = env
			os.Unsetenv("GITHUB_TOKEN")
		}
	}
	if *token == "" && !*anonymous {
		log.Fatal("must provide either -token or -anonymous flag")
	}
	if *token != "" && *anonymous {
		log.Fatal("cannot specify both -token and -anonymous")
	}

	mountPath := flag.Arg(0)
	slog.Info("starting ghfs", "mount_path", mountPath)

	// Resolve cache directory.
	cachePath := *cacheDir
	if cachePath == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatal("failed to determine home directory:", err)
		}
		cachePath = filepath.Join(homeDir, ".cache", "ghfs")
	}
	if err := os.MkdirAll(cachePath, 0o755); err != nil {
		log.Fatal("failed to create cache directory:", err)
	}
	blobsDir := filepath.Join(cachePath, "blobs")
	reposDir := filepath.Join(cachePath, "repos")
	apiCacheDir := filepath.Join(cachePath, "api")

	// Blob cache + gitstore + repo manager.
	cache, err := blobcache.New(blobsDir, int64(*diskCacheMB)*1024*1024, slog.Default())
	if err != nil {
		log.Fatal("blob cache init:", err)
	}
	store := gitstore.New(slog.Default())
	store.SetBatchPoolSize(*batchPoolSz)
	repos := reposrc.NewManager(store, cache, reposDir, *token, slog.Default())
	repos.HydratorWorkers = *hydWorkers

	slog.Info("cache configured",
		"disk_mb", *diskCacheMB,
		"blobs_dir", blobsDir,
		"repos_dir", reposDir,
	)
	// GitHub API client: layered transport chain with ETag-aware
	// httpcache on a two-tier in-memory-LRU + disk store, fronted by
	// a 5-minute revalidation suppressor so repeated per-repo Contents
	// probes (e.g. by eza --icons) cost zero network after warming.
	// SIGUSR1 flushes the suppressor so the next request revalidates.
	apiClient, err := githubhttp.New(githubhttp.Config{
		Token:      *token,
		CacheDir:   apiCacheDir,
		MemCacheMB: *apiMemMB,
		Disabled:   *apiNoCache,
		Logger:     slog.Default(),
	})
	if err != nil {
		log.Fatal("github http client init:", err)
	}
	if *apiNoCache {
		slog.Info("GitHub API cache disabled")
	} else {
		slog.Info("GitHub API cache configured",
			"mem_mb", *apiMemMB,
			"dir", apiCacheDir,
			"suppress_window", githubhttp.DefaultSuppressDur,
		)
	}
	githubClient := github.NewClient(apiClient.HTTP)

	if *token == "" {
		slog.Warn("using anonymous mode - REST API rate limits apply (60 requests/hour)")
	}

	slog.Info("send SIGUSR1 to force-refresh: fetches all cloned repos + clears API cache suppression",
		"command", "kill -USR1 "+strconv.Itoa(os.Getpid()))

	// SIGUSR1 → (1) clear the suppression cache so subsequent API
	// requests revalidate through httpcache and pick up any changes,
	// (2) fetch all cloned repos. Already-materialized FUSE child
	// inodes (cached by the kernel for entryTimeout) are NOT
	// invalidated — the kernel will continue serving them until they
	// expire naturally. This is a known limitation of the
	// single-process FUSE model.
	//
	// Fetches fan out with bounded concurrency (reposrc.Manager.FetchAll)
	// so a user with dozens of clones isn't held hostage by a serial
	// walk: the v0 loop took ~N * per-repo-fetch on a 50+ clone tree,
	// multi-minute latency before the first refreshed repo was visible.
	// Workers match the hydrate / cat-file pool defaults (4) — enough
	// parallelism to saturate a typical network pipe without spawning
	// a thundering herd of git processes.
	refreshWorkers := *hydWorkers
	if refreshWorkers < 1 {
		refreshWorkers = 4
	}
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGUSR1)
		for range sigChan {
			slog.Info("SIGUSR1 received")
			if apiClient.Suppressor != nil {
				apiClient.Suppressor.ClearSuppressionCache()
			}
			repos.FetchAll(context.Background(), refreshWorkers, 2*time.Minute)
		}
	}()

	root := &core.FS{
		Client: githubClient,
		Repos:  repos,
		Logger: slog.Default(),
	}

	// Negatively cache ENOENT at the kernel dcache. Every jj/git/shell
	// prompt renders fires stat() for a bunch of metadata files
	// (.jj, .git, .envrc, .gitignore, package.json, Cargo.toml, ...).
	// Without a negative TTL, each of those stats re-enters our
	// Lookup handler and turns into a Users.Get / Repositories.Get
	// probe — even though the answer never changes. 30 minutes
	// matches entryTimeout for positive entries.
	negTimeout := 30 * time.Minute
	fuseOpts := &fs.Options{
		EntryTimeout:    &negTimeout,
		AttrTimeout:     &negTimeout,
		NegativeTimeout: &negTimeout,
		MountOptions: fuse.MountOptions{
			Name:          "ghfs",
			FsName:        "ghfs",
			DisableXAttrs: true,
			Debug:         os.Getenv("GHFS_FUSE_DEBUG") != "",
			Options:       []string{"noatime"},
		},
	}

	server, err := fs.Mount(mountPath, root, fuseOpts)
	if err != nil {
		slog.Error("failed to mount FUSE", "path", mountPath, "error", err)
		log.Fatal(err)
	}
	slog.Info("serving FUSE filesystem")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		server.Wait()
		close(done)
	}()

	unmounting := false
	for {
		select {
		case sig := <-sigChan:
			if unmounting {
				slog.Info("received another shutdown signal, retrying unmount...", "signal", sig)
			} else {
				slog.Info("received shutdown signal", "signal", sig)
				unmounting = true
			}

			unmountDone := make(chan error, 1)
			go func() { unmountDone <- server.Unmount() }()

			select {
			case err := <-unmountDone:
				if err != nil {
					slog.Warn("unmount failed", "error", err)
					slog.Info("press Ctrl+C again to retry, or force unmount from another terminal", "command", "umount "+mountPath)
				} else {
					<-done
					slog.Info("filesystem unmounted")
					repos.Close()
					store.Close()
					return
				}
			case <-time.After(2 * time.Second):
				slog.Warn("unmount timed out (active operations?)")
				slog.Info("press Ctrl+C again to retry, or force unmount from another terminal", "command", "umount "+mountPath)
			}

		case <-done:
			slog.Info("filesystem unmounted")
			repos.Close()
			store.Close()
			return
		}
	}
}
