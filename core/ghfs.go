package core

import (
	"container/list"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/go-github/v60/github"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// revalidationEntry represents an entry in the LRU cache
type revalidationEntry struct {
	url       string
	timestamp time.Time
}

// RevalidationSuppressor wraps an http.RoundTripper and suppresses revalidation requests
// for a configured duration after receiving a 304 Not Modified response.
//
// TODO: Replace homegrown LRU with a more efficient implementation (e.g., hashicorp/golang-lru
// or github.com/karlseguin/ccache) to reduce allocations and lock contention.
type RevalidationSuppressor struct {
	Base             http.RoundTripper
	SuppressDuration time.Duration
	Logger           *slog.Logger
	MaxEntries       int // Maximum number of URLs to track (default 1000)

	cache map[string]*list.Element // URL -> list element
	lru   *list.List               // LRU list of *revalidationEntry
	mu    sync.Mutex
}

// ClearSuppressionCache clears the revalidation suppression cache, allowing immediate revalidation.
// This is useful when users want to force a refresh of cached data.
func (r *RevalidationSuppressor) ClearSuppressionCache() {
	r.mu.Lock()
	r.cache = make(map[string]*list.Element)
	r.lru = list.New()
	r.mu.Unlock()
	if r.Logger != nil {
		r.Logger.Info("revalidation suppression cache cleared - next requests will revalidate")
	}
}

func (r *RevalidationSuppressor) RoundTrip(req *http.Request) (*http.Response, error) {
	// Only suppress GET requests
	if req.Method != "GET" {
		return r.Base.RoundTrip(req)
	}

	cacheKey := req.URL.String()

	// Check if we recently revalidated this URL
	r.mu.Lock()
	if elem, exists := r.cache[cacheKey]; exists {
		entry := elem.Value.(*revalidationEntry)
		if time.Since(entry.timestamp) < r.SuppressDuration {
			// Move to front (most recently used)
			r.lru.MoveToFront(elem)
			r.mu.Unlock()

			// Force a cache hit by adding Cache-Control: only-if-cached header
			// This tells httpcache to only serve from cache, never revalidate
			// Clone the request to avoid modifying the original
			reqCopy := req.Clone(req.Context())
			reqCopy.Header.Set("Cache-Control", "only-if-cached")
			resp, err := r.Base.RoundTrip(reqCopy)
			// If we got a 504 (gateway timeout) from only-if-cached with no cache entry,
			// fall through to normal request
			if err == nil && resp.StatusCode == http.StatusGatewayTimeout {
				resp.Body.Close()
				// Fall through to normal request below
			} else {
				return resp, err
			}
		} else {
			// Expired, remove it
			delete(r.cache, cacheKey)
			r.lru.Remove(elem)
		}
	}
	r.mu.Unlock()

	resp, err := r.Base.RoundTrip(req)

	// Record revalidation time for any response from cache
	// httpcache converts 304s to 200s from cache, so we detect cache hits via X-From-Cache header
	if err == nil && resp != nil && resp.Header.Get("X-From-Cache") == "1" {
		r.mu.Lock()
		defer r.mu.Unlock()

		// Initialize if needed
		if r.cache == nil {
			r.cache = make(map[string]*list.Element)
			r.lru = list.New()
		}

		maxEntries := r.MaxEntries
		if maxEntries == 0 {
			maxEntries = 1000
		}

		// Update or add entry
		if elem, exists := r.cache[cacheKey]; exists {
			// Update timestamp and move to front
			entry := elem.Value.(*revalidationEntry)
			entry.timestamp = time.Now()
			r.lru.MoveToFront(elem)
		} else {
			// Add new entry
			entry := &revalidationEntry{
				url:       cacheKey,
				timestamp: time.Now(),
			}
			elem := r.lru.PushFront(entry)
			r.cache[cacheKey] = elem

			// Evict oldest if over capacity
			if r.lru.Len() > maxEntries {
				oldest := r.lru.Back()
				if oldest != nil {
					oldEntry := oldest.Value.(*revalidationEntry)
					delete(r.cache, oldEntry.url)
					r.lru.Remove(oldest)
				}
			}
		}
	}

	return resp, err
}

// Paths to block from being looked up in the GitHub API.
// These are version control directories that never exist in GitHub and waste API calls.
var blockedPaths = map[string]bool{
	".git": true, // Git directory - never exposed by GitHub API
	".svn": true, // Subversion directory
	".cvs": true, // CVS directory
}

// GitHubHTTPTransport handles authentication and logging.
type GitHubHTTPTransport struct {
	Token  string // Empty if unauthenticated
	Base   http.RoundTripper
	Logger *slog.Logger
}

// RoundTrip implements http.RoundTripper with authentication, header normalization, and logging.
func (t *GitHubHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Add authentication token if provided
	if t.Token != "" {
		req.Header.Set("Authorization", "token "+t.Token)
	}

	// Execute the request
	resp, err := t.Base.RoundTrip(req)
	if resp == nil || err != nil {
		if t.Logger != nil && err != nil {
			t.Logger.Debug("http.request", "method", req.Method, "url", req.URL.Path, "error", err.Error())
		}
		return resp, err
	}

	// For 304 responses, set Date header to current time
	// This ensures httpcache calculates freshness from now, not from original response
	if resp.StatusCode == http.StatusNotModified {
		resp.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}

	// Ensure Date header is set (required by httpcache)
	if resp.Header.Get("Date") == "" {
		resp.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}

	// Remove Vary header and related X-Varied-* headers to allow caching
	resp.Header.Del("Vary")
	for key := range resp.Header {
		if strings.HasPrefix(key, "X-Varied-") {
			resp.Header.Del(key)
		}
	}

	// Ensure 404s have ETag for caching
	if resp.StatusCode == http.StatusNotFound && resp.Header.Get("ETag") == "" {
		resp.Header.Set("ETag", "\"404-"+req.URL.String()+"\"")
	}

	// Log the request with cache status
	if t.Logger != nil {
		cacheStatus := "miss"
		if resp.Header.Get("X-From-Cache") == "1" {
			cacheStatus = "hit"
		} else if resp.StatusCode == http.StatusNotModified {
			cacheStatus = "revalidated"
		}
		t.Logger.Debug("http.request",
			"method", req.Method,
			"url", req.URL.Path,
			"status", resp.StatusCode,
			"cache", cacheStatus,
		)
	}

	return resp, err
}

// FS represents the FUSE filesystem root
type FS struct {
	fs.Inode
	Client *github.Client
	Logger *slog.Logger
}

var _ = (fs.NodeLookuper)((*FS)(nil))

// Lookup looks up a child node in the root directory
func (f *FS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if blockedPaths[name] {
		return nil, syscall.ENOENT
	}

	// Set entry cache timeout to 30 minutes to reduce lookups
	out.SetEntryTimeout(30 * time.Minute)

	// Check if we already have an inode for this user
	existing := f.GetChild(name)
	if existing != nil {
		return existing, 0
	}

	// Fetch from API (httpcache will handle caching)
	u, _, err := f.Client.Users.Get(ctx, name)
	if err != nil {
		f.Logger.Error("lookup: failed to get user", "user", name, "error", err)
		return nil, syscall.ENOENT
	}

	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
	}

	user := &User{
		User:   u,
		Client: f.Client,
		Logger: f.Logger,
	}

	return f.NewInode(ctx, user, stable), 0
}

var _ = (fs.NodeReaddirer)((*FS)(nil))

// Readdir reads the root directory
func (f *FS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	var entries []fuse.DirEntry

	// Get the authenticated user's information
	user, _, err := f.Client.Users.Get(ctx, "")
	if err != nil {
		f.Logger.Error("root.readdir: failed to get authenticated user", "error", err)
		// If no token is provided or user can't be authenticated, return empty list
		return fs.NewListDirStream(entries), 0
	}

	// Add the authenticated user as a directory entry
	if user.Login != nil {
		entries = append(entries, fuse.DirEntry{
			Name: *user.Login,
			Mode: fuse.S_IFDIR,
		})
	}

	// Get organizations the user has access to
	opts := &github.ListOptions{PerPage: 100}
	for {
		orgs, resp, err := f.Client.Organizations.List(ctx, "", opts)
		if err != nil {
			f.Logger.Error("root.readdir: failed to get organizations", "error", err)
			break
		}

		for _, org := range orgs {
			if org.Login != nil {
				entries = append(entries, fuse.DirEntry{
					Name: *org.Login,
					Mode: fuse.S_IFDIR,
				})
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

// Getattr returns attributes for the root directory
func (f *FS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.SetTimeout(30 * time.Minute)
	// Set reasonable timestamps to avoid unnecessary revalidation
	now := uint64(time.Now().Unix())
	out.Atime = now
	out.Mtime = now
	out.Ctime = now
	return 0
}

// User represents a GitHub user or organization directory
type User struct {
	fs.Inode
	*github.User
	Client *github.Client
	Logger *slog.Logger
}

var _ = (fs.NodeGetattrer)((*User)(nil))

func (u *User) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.SetTimeout(30 * time.Minute)
	out.Nlink = 2 // . and ..
	return 0
}

var _ = (fs.NodeReaddirer)((*User)(nil))

func (u *User) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	var entries []fuse.DirEntry

	// List all repositories for the user with pagination (httpcache will handle caching)
	opts := &github.RepositoryListOptions{
		ListOptions: github.ListOptions{PerPage: 100},
	}
	for {
		repos, resp, err := u.Client.Repositories.List(ctx, *u.Login, opts)
		if err != nil {
			u.Logger.Error("user.readdir: failed to get repositories", "user", *u.Login, "error", err)
			return nil, syscall.ENOENT
		}

		for _, repo := range repos {
			entries = append(entries, fuse.DirEntry{
				Name: *repo.Name,
				Mode: fuse.S_IFDIR,
			})

			// Pre-populate child inode to avoid subsequent Lookup() API calls
			if u.GetChild(*repo.Name) == nil {
				stable := fs.StableAttr{Mode: fuse.S_IFDIR}
				repository := &Repository{
					Repository: repo,
					Client:     u.Client,
					Logger:     u.Logger,
				}
				child := u.NewInode(ctx, repository, stable)
				u.AddChild(*repo.Name, child, false)
			}
		}

		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	return fs.NewListDirStream(entries), 0
}

var _ = (fs.NodeLookuper)((*User)(nil))

func (u *User) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if blockedPaths[name] {
		return nil, syscall.ENOENT
	}

	// Set entry cache timeout to 30 minutes to reduce lookups
	out.SetEntryTimeout(30 * time.Minute)

	// Check if we already have an inode for this repository
	existing := u.GetChild(name)
	if existing != nil {
		return existing, 0
	}

	// Fetch from API (httpcache will handle caching)
	r, _, err := u.Client.Repositories.Get(ctx, *u.Login, name)
	if err != nil {
		u.Logger.Error("user.lookup: failed to get repository", "user", *u.Login, "repo", name, "error", err)
		return nil, syscall.ENOENT
	}

	stable := fs.StableAttr{Mode: fuse.S_IFDIR}
	repo := &Repository{
		Repository: r,
		Client:     u.Client,
		Logger:     u.Logger,
	}
	return u.NewInode(ctx, repo, stable), 0
}

// Repository represents a GitHub repository directory
type Repository struct {
	fs.Inode
	*github.Repository
	Client *github.Client
	Logger *slog.Logger
}

var _ = (fs.NodeGetattrer)((*Repository)(nil))

func (r *Repository) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.SetTimeout(30 * time.Minute)
	out.Nlink = 2 // . and ..
	return 0
}

var _ = (fs.NodeLookuper)((*Repository)(nil))

func (r *Repository) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if blockedPaths[name] {
		return nil, syscall.ENOENT
	}

	// Set entry cache timeout to 30 minutes to reduce lookups
	out.SetEntryTimeout(30 * time.Minute)

	// Check if we already have an inode for this entry
	existing := r.GetChild(name)
	if existing != nil {
		return existing, 0
	}

	// Fetch from API (httpcache will handle caching)
	fileContent, _, _, err := r.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, name, nil)
	if err != nil {
		r.Logger.Error("repo.lookup: failed to get contents", "user", *r.Owner.Login, "repo", *r.Name, "path", name, "error", err)
		return nil, syscall.ENOENT
	}

	if fileContent != nil {
		stable := fs.StableAttr{Mode: fuse.S_IFREG}
		file := &File{
			Content: fileContent,
			Client:  r.Client,
			Logger:  r.Logger,
			Owner:   *r.Owner.Login,
			Repo:    *r.Name,
		}
		return r.NewInode(ctx, file, stable), 0
	}

	stable := fs.StableAttr{Mode: fuse.S_IFDIR}
	dir := &Dir{
		Client: r.Client,
		Logger: r.Logger,
		Owner:  *r.Owner.Login,
		Repo:   *r.Name,
		Path:   name,
	}
	return r.NewInode(ctx, dir, stable), 0
}

var _ = (fs.NodeReaddirer)((*Repository)(nil))

func (r *Repository) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Fetch from API (httpcache will handle caching)
	_, directoryContent, _, err := r.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, "", nil)
	if err != nil {
		return nil, syscall.ENOENT
	}

	var entries []fuse.DirEntry
	for _, f := range directoryContent {
		var mode uint32
		if *f.Type == "dir" {
			mode = fuse.S_IFDIR
		} else {
			mode = fuse.S_IFREG
		}
		entries = append(entries, fuse.DirEntry{
			Name: *f.Name,
			Mode: mode,
		})

		// Pre-populate child inode to avoid subsequent Lookup() API calls
		if r.GetChild(*f.Name) == nil {
			stable := fs.StableAttr{Mode: mode}
			var child *fs.Inode
			if *f.Type == "dir" {
				dir := &Dir{
					Client: r.Client,
					Logger: r.Logger,
					Owner:  *r.Owner.Login,
					Repo:   *r.Name,
					Path:   *f.Name,
				}
				child = r.NewInode(ctx, dir, stable)
			} else {
				file := &File{
					Content: f,
					Client:  r.Client,
					Logger:  r.Logger,
					Owner:   *r.Owner.Login,
					Repo:    *r.Name,
				}
				child = r.NewInode(ctx, file, stable)
			}
			r.AddChild(*f.Name, child, false)
		}
	}
	return fs.NewListDirStream(entries), 0
}

// File represents a file within a GitHub repository
type File struct {
	fs.Inode
	Content *github.RepositoryContent
	Client  *github.Client
	Logger  *slog.Logger
	Owner   string
	Repo    string
}

var _ = (fs.NodeOnForgetter)((*File)(nil))

// OnForget is called when the kernel forgets this inode
func (f *File) OnForget() {
	// Clear content to free memory (especially if it was populated from directory listing)
	if f.Content != nil && f.Content.Content != nil {
		f.Content.Content = nil
	}
}

var _ = (fs.NodeGetattrer)((*File)(nil))

func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(*f.Content.Size)
	out.Mode = fuse.S_IFREG | 0644
	out.SetTimeout(30 * time.Minute)
	return 0
}

var _ = (fs.NodeOpener)((*File)(nil))

func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// If Content is not populated (e.g., from directory listing), fetch it
	var content string
	if f.Content.Content == nil || *f.Content.Content == "" {
		fileContent, _, _, err := f.Client.Repositories.GetContents(ctx, f.Owner, f.Repo, *f.Content.Path, nil)
		if err != nil {
			f.Logger.Error("file.open: failed to get file content", "path", *f.Content.Path, "error", err)
			return nil, 0, syscall.EIO
		}
		if fileContent == nil {
			f.Logger.Error("file.open: got nil file content", "path", *f.Content.Path)
			return nil, 0, syscall.EIO
		}
		content = *fileContent.Content
	} else {
		content = *f.Content.Content
	}

	// Decode base64 from GitHub API
	decoded, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding, strings.NewReader(content)))
	if err != nil {
		f.Logger.Error("file.open: base64 decode error", "path", *f.Content.Path, "error", err)
		return nil, 0, syscall.EIO
	}

	fh := &FileHandle{
		data:   decoded,
		logger: f.Logger,
		path:   *f.Content.Path,
	}

	return fh, fuse.FOPEN_KEEP_CACHE, 0
}

// FileHandle represents an open file handle
type FileHandle struct {
	data   []byte
	logger *slog.Logger
	path   string
}

var _ = (fs.FileReader)((*FileHandle)(nil))

func (fh *FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	end := off + int64(len(dest))
	if end > int64(len(fh.data)) {
		end = int64(len(fh.data))
	}

	if off >= int64(len(fh.data)) {
		return fuse.ReadResultData(nil), 0
	}

	n := copy(dest, fh.data[off:end])
	return fuse.ReadResultData(dest[:n]), 0
}

// Dir represents a directory within a GitHub repository
type Dir struct {
	fs.Inode
	Client *github.Client
	Logger *slog.Logger
	Owner  string
	Repo   string
	Path   string // Path within the repository (e.g., "docker" or "src/main")
}

var _ = (fs.NodeGetattrer)((*Dir)(nil))

func (d *Dir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0755
	out.SetTimeout(30 * time.Minute)
	return 0
}

var _ = (fs.NodeReaddirer)((*Dir)(nil))

func (d *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Fetch from API (httpcache will handle caching)
	_, directoryContent, _, err := d.Client.Repositories.GetContents(ctx, d.Owner, d.Repo, d.Path, nil)
	if err != nil {
		d.Logger.Error("dir.readdir: failed to get contents", "owner", d.Owner, "repo", d.Repo, "path", d.Path, "error", err)
		return nil, syscall.ENOENT
	}

	var entries []fuse.DirEntry
	for _, content := range directoryContent {
		var mode uint32
		if *content.Type == "dir" {
			mode = fuse.S_IFDIR
		} else {
			mode = fuse.S_IFREG
		}
		entries = append(entries, fuse.DirEntry{
			Name: *content.Name,
			Mode: mode,
		})

		// Pre-populate child inode to avoid subsequent Lookup() API calls
		if d.GetChild(*content.Name) == nil {
			stable := fs.StableAttr{Mode: mode}
			var child *fs.Inode
			if *content.Type == "dir" {
				subdir := &Dir{
					Client: d.Client,
					Logger: d.Logger,
					Owner:  d.Owner,
					Repo:   d.Repo,
					Path:   d.Path + "/" + *content.Name,
				}
				child = d.NewInode(ctx, subdir, stable)
			} else {
				file := &File{
					Content: content,
					Client:  d.Client,
					Logger:  d.Logger,
					Owner:   d.Owner,
					Repo:    d.Repo,
				}
				child = d.NewInode(ctx, file, stable)
			}
			d.AddChild(*content.Name, child, false)
		}
	}
	return fs.NewListDirStream(entries), 0
}

var _ = (fs.NodeLookuper)((*Dir)(nil))

func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Set entry cache timeout to 30 minutes to reduce lookups
	out.SetEntryTimeout(30 * time.Minute)

	// Check if we already have an inode for this entry
	existing := d.GetChild(name)
	if existing != nil {
		return existing, 0
	}

	// Construct the full path for this lookup
	fullPath := d.Path + "/" + name

	// Fetch from API (httpcache will handle caching)
	fileContent, directoryContent, _, err := d.Client.Repositories.GetContents(ctx, d.Owner, d.Repo, fullPath, nil)
	if err != nil {
		d.Logger.Error("dir.lookup: failed to get contents", "owner", d.Owner, "repo", d.Repo, "path", fullPath, "error", err)
		return nil, syscall.ENOENT
	}

	if fileContent != nil {
		stable := fs.StableAttr{Mode: fuse.S_IFREG}
		file := &File{
			Content: fileContent,
			Client:  d.Client,
			Logger:  d.Logger,
			Owner:   d.Owner,
			Repo:    d.Repo,
		}
		return d.NewInode(ctx, file, stable), 0
	}

	if directoryContent != nil {
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		dir := &Dir{
			Client: d.Client,
			Logger: d.Logger,
			Owner:  d.Owner,
			Repo:   d.Repo,
			Path:   fullPath,
		}
		return d.NewInode(ctx, dir, stable), 0
	}

	return nil, syscall.ENOENT
}
