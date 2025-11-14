package core

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/google/go-github/v60/github"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

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

	// Override Cache-Control header to use longer cache durations
	// GitHub's default cache times are too short for a filesystem use case
	// where repository contents don't change frequently
	if resp.StatusCode == http.StatusNotFound {
		// Cache 404s for 1 hour - if something doesn't exist, it likely won't appear soon
		resp.Header.Set("Cache-Control", "public, max-age=3600")
	} else if resp.StatusCode == http.StatusOK {
		// Cache successful responses for 30 minutes
		// This significantly reduces API calls for repository browsing
		resp.Header.Set("Cache-Control", "public, max-age=1800")
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
		f.Logger.Debug("lookup: blocked path", "name", name)
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
		u.Logger.Debug("user.lookup: blocked path", "user", *u.Login, "name", name)
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
		r.Logger.Debug("repo.lookup: blocked path", "user", *r.Owner.Login, "repo", *r.Name, "name", name)
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
	fileContent, directoryContent, _, err := r.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, name, nil)
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
		r.Logger.Debug("repo.readdir: failed to get contents", "user", *r.Owner.Login, "repo", *r.Name, "error", err)
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

var _ = (fs.NodeGetattrer)((*File)(nil))

func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(*f.Content.Size)
	out.Mode = fuse.S_IFREG | 0644
	out.SetTimeout(30 * time.Minute)
	return 0
}

var _ = (fs.NodeOpener)((*File)(nil))

func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	f.Logger.Debug("file.open: opening file", "path", *f.Content.Path, "size", *f.Content.Size)

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
	if n > 0 {
		fh.logger.Debug("file.read: read bytes", "path", fh.path, "bytes", n, "offset", off)
	}

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
