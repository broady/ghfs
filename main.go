package main

import (
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/google/go-github/v60/github"
	"github.com/gregjones/httpcache"
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
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("path required")
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

	// Create HTTP client with authentication, caching, and logging.
	// Chain: GitHubHTTPTransport (auth + cache headers + logging) -> httpcache.Transport (caching) -> http.DefaultTransport
	cacheTransport := &httpcache.Transport{
		Cache:               httpcache.NewMemoryCache(),
		MarkCachedResponses: true,
		Transport:           http.DefaultTransport,
	}
	transport := &core.GitHubHTTPTransport{
		Token:  *token,
		Base:   cacheTransport,
		Logger: slog.Default(),
	}
	c := &http.Client{Transport: transport}

	if *token != "" {
		slog.Debug("github authentication and caching enabled")
	} else {
		slog.Warn("no github token provided - will use unauthenticated API calls with caching")
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
