package main

import (
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/google/go-github/v60/github"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/broady/ghfs/core"
)

func main() {
	// Setup structured logging
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
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

	// Create HTTP client with authentication if token is provided.
	var c *http.Client
	if *token != "" {
		c = &http.Client{
			Transport: &core.TokenTransport{
				Token: *token,
				Base:  http.DefaultTransport,
			},
		}
		slog.Debug("github authentication enabled")
	} else {
		slog.Warn("no github token provided - will use unauthenticated API calls")
	}

	// Create filesystem with caches.
	// 404 cache: 1000 entries with 5 min TTL
	// File content cache: 128 MB with 2 min TTL
	filesys := &core.FS{
		Client:           github.NewClient(c),
		Logger:           slog.Default(),
		NotFoundCache:    core.NewNotFoundCache(1000),
		FileContentCache: core.NewFileContentCache(128 * 1024 * 1024),
	}
	slog.Info("serving FUSE filesystem")
	if err := fs.Serve(conn, filesys); err != nil {
		slog.Error("fs.Serve failed", "error", err)
		log.Fatal(err)
	}
}
