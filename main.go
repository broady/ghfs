package main

import (
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/die-net/lrucache"
	"github.com/die-net/lrucache/twotier"
	"github.com/google/go-github/v60/github"
	"github.com/gregjones/httpcache"
	"github.com/gregjones/httpcache/diskcache"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/broady/ghfs/core"
)

func main() {
	// Setup structured logging
	logLevelStr := os.Getenv("GHFS_LOG_LEVEL")
	if logLevelStr == "" {
		logLevelStr = "info"
	}

	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(logLevelStr)); err != nil {
		slog.Error("invalid GHFS_LOG_LEVEL", "value", logLevelStr, "error", err)
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}
	handler := slog.NewTextHandler(os.Stderr, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)

	log.SetFlags(0)

	// Parse arguments and require that we have the path.
	token := flag.String("token", "", "personal access token")
	anonymous := flag.Bool("anonymous", false, "use anonymous (unauthenticated) API requests")
	cacheDir := flag.String("cache-dir", "", "directory for disk cache (default: ~/.cache/ghfs)")
	memCacheMB := flag.Int("cache-mem-mb", 128, "in-memory cache size in MB")
	diskCacheMB := flag.Int("cache-disk-mb", 1024, "disk cache size in MB")
	noCacheFlag := flag.Bool("no-cache", false, "disable caching entirely")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("path required")
	}

	// Require explicit authentication choice
	if *token == "" && !*anonymous {
		log.Fatal("must provide either -token or -anonymous flag")
	}
	if *token != "" && *anonymous {
		log.Fatal("cannot specify both -token and -anonymous")
	}

	mountPath := flag.Arg(0)
	slog.Info("starting ghfs", "mount_path", mountPath)

	// Create FUSE connection.
	conn, err := fuse.Mount(mountPath)
	if err != nil {
		slog.Error("failed to mount FUSE", "path", mountPath, "error", err)
		log.Fatal(err)
	}
	defer conn.Close()

	// Create HTTP client with layered transports.
	//
	// Transport chain (outside to inside):
	// 1. httpcache.Transport - HTTP caching with ETag-based conditional requests
	// 2. GitHubHTTPTransport - authentication, header normalization, and logging
	// 3. http.DefaultTransport - actual HTTP requests

	githubTransport := &core.GitHubHTTPTransport{
		Token:  *token,
		Base:   http.DefaultTransport,
		Logger: slog.Default(),
	}

	var finalTransport http.RoundTripper
	// Configure caching
	if *noCacheFlag {
		// No caching - use GitHubHTTPTransport directly
		finalTransport = githubTransport
		slog.Info("caching disabled")
	} else {
		// Setup cache directory
		cachePath := *cacheDir
		if cachePath == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				log.Fatal("failed to determine home directory:", err)
			}
			cachePath = filepath.Join(homeDir, ".cache", "ghfs")
		}

		// Create cache directory if it doesn't exist
		if err := os.MkdirAll(cachePath, 0755); err != nil {
			log.Fatal("failed to create cache directory:", err)
		}

		// Create two-tier cache: LRU memory cache + disk cache
		// maxSize in bytes, maxAge in seconds (24 hours)
		memCache := lrucache.New(int64(*memCacheMB)*1024*1024, 24*60*60)
		diskCache := diskcache.New(cachePath)
		cache := twotier.New(memCache, diskCache)

		// Wrap with httpcache
		finalTransport = &httpcache.Transport{
			Cache:               cache,
			MarkCachedResponses: true,
			Transport:           githubTransport,
		}

		slog.Info("cache configured", "mem_mb", *memCacheMB, "disk_mb", *diskCacheMB, "cache_dir", cachePath)
	}

	c := &http.Client{Transport: finalTransport}

	if *token != "" {
		slog.Debug("github authentication enabled")
	} else {
		slog.Warn("using anonymous mode - API rate limits will be significantly lower (60 requests/hour)")
	}

	// Create filesystem.
	// HTTP cache: httpcache for all GitHub API responses (automatic via http.Client)
	filesys := &core.FS{
		Client: github.NewClient(c),
		Logger: slog.Default(),
	}
	slog.Info("serving FUSE filesystem")
	if err := fs.Serve(conn, filesys); err != nil {
		slog.Error("fs.Serve failed", "error", err)
		log.Fatal(err)
	}
}
