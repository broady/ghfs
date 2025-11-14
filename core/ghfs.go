package core

import (
	"container/list"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/google/go-github/v60/github"
)

// Paths to block from being looked up in the GitHub API.
// These are version control directories that never exist in GitHub and waste API calls.
var blockedPaths = map[string]bool{
	".git": true, // Git directory - never exposed by GitHub API
	".svn": true, // Subversion directory
	".cvs": true, // CVS directory
}

// NotFoundCache is an LRU cache for 404 responses with TTL.
type NotFoundCache struct {
	mu    sync.RWMutex
	cache map[string]time.Time // path -> expiration time
	lru   *list.List            // for LRU eviction
	max   int
}

// NewNotFoundCache creates a new 404 cache with max entries and TTL.
func NewNotFoundCache(maxEntries int) *NotFoundCache {
	return &NotFoundCache{
		cache: make(map[string]time.Time),
		lru:   list.New(),
		max:   maxEntries,
	}
}

// Has checks if a path is in the cache and not expired.
func (c *NotFoundCache) Has(path string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expiration, exists := c.cache[path]
	if !exists {
		return false
	}

	// Check if expired
	if time.Now().After(expiration) {
		return false
	}

	return true
}

// Add adds a path to the cache with TTL.
func (c *NotFoundCache) Add(path string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiration := time.Now().Add(ttl)

	// If already in cache, just update expiration
	if _, exists := c.cache[path]; exists {
		c.cache[path] = expiration
		return
	}

	// If at capacity, remove oldest
	if len(c.cache) >= c.max {
		if c.lru.Len() > 0 {
			front := c.lru.Front()
			if front != nil {
				delete(c.cache, front.Value.(string))
				c.lru.Remove(front)
			}
		}
	}

	c.cache[path] = expiration
	c.lru.PushBack(path)
}

// FileContentCache is an LRU cache for file contents with memory bounds and TTL.
type FileContentCache struct {
	mu         sync.RWMutex
	cache      map[string]*CachedFile
	lru        *list.List // doubly-linked list of paths for LRU eviction
	maxBytes   int64      // max size in bytes
	usedBytes  int64      // currently used bytes
}

// CachedFile holds file content and metadata.
type CachedFile struct {
	content    []byte
	expiration time.Time
}

// NewFileContentCache creates a new file content cache with max size in bytes.
func NewFileContentCache(maxBytes int64) *FileContentCache {
	return &FileContentCache{
		cache:    make(map[string]*CachedFile),
		lru:      list.New(),
		maxBytes: maxBytes,
	}
}

// Get retrieves a file from cache if it exists and hasn't expired.
func (c *FileContentCache) Get(path string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	file, exists := c.cache[path]
	if !exists {
		return nil, false
	}

	// Check if expired
	if time.Now().After(file.expiration) {
		return nil, false
	}

	return file.content, true
}

// Add stores file content in cache with TTL, evicting if necessary to stay within memory bounds.
func (c *FileContentCache) Add(path string, content []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	contentSize := int64(len(content))

	// If already in cache, remove old entry first
	if existing, exists := c.cache[path]; exists {
		c.usedBytes -= int64(len(existing.content))
		delete(c.cache, path)
	}

	// Make room if needed
	for c.usedBytes+contentSize > c.maxBytes && c.lru.Len() > 0 {
		front := c.lru.Front()
		if front != nil {
			oldPath := front.Value.(string)
			if oldFile, ok := c.cache[oldPath]; ok {
				c.usedBytes -= int64(len(oldFile.content))
				delete(c.cache, oldPath)
			}
			c.lru.Remove(front)
		}
	}

	// Add new entry
	if contentSize <= c.maxBytes {
		c.cache[path] = &CachedFile{
			content:    content,
			expiration: time.Now().Add(ttl),
		}
		c.usedBytes += contentSize
		c.lru.PushBack(path)
	}
}

// tokenTransport is an http.RoundTripper that adds authentication token to requests.
type TokenTransport struct {
	Token string
	Base  http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t *TokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "token "+t.Token)
	return t.Base.RoundTrip(req)
}

// FS represents the FUSE filesystem
type FS struct {
	Client           *github.Client
	Logger           *slog.Logger
	NotFoundCache    *NotFoundCache
	FileContentCache *FileContentCache
}

// Root returns the root filesystem node.
func (f *FS) Root() (fs.Node, error) {
	return &Root{FS: f}, nil
}

type Root struct {
	FS *FS
}

func (r *Root) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0755
	return nil
}

func (r *Root) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	if blockedPaths[req.Name] {
		r.FS.Logger.Debug("lookup: blocked path", "name", req.Name)
		return nil, fuse.ENOENT
	}

	r.FS.Logger.Debug("lookup: getting user", "user", req.Name)
	u, _, err := r.FS.Client.Users.Get(ctx, req.Name)
	if err != nil {
		r.FS.Logger.Error("lookup: failed to get user", "user", req.Name, "error", err)
		return nil, fuse.ENOENT
	}
	r.FS.Logger.Debug("lookup: found user", "user", req.Name)
	return &User{FS: r.FS, User: u}, nil
}

type User struct {
	*github.User
	FS *FS
}

func (u *User) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0755
	return nil
}

func (u *User) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	if blockedPaths[req.Name] {
		u.FS.Logger.Debug("user.lookup: blocked path", "user", *u.Login, "name", req.Name)
		return nil, fuse.ENOENT
	}

	u.FS.Logger.Debug("user.lookup: getting repository", "user", *u.Login, "repo", req.Name)
	r, _, err := u.FS.Client.Repositories.Get(ctx, *u.Login, req.Name)
	if err != nil {
		u.FS.Logger.Error("user.lookup: failed to get repository", "user", *u.Login, "repo", req.Name, "error", err)
		return nil, fuse.ENOENT
	}
	u.FS.Logger.Debug("user.lookup: found repository", "user", *u.Login, "repo", req.Name)
	return &Repository{FS: u.FS, Repository: r}, nil
}

type Repository struct {
	*github.Repository
	FS *FS
}

var _ = fs.HandleReadDirAller(&Repository{})

func (r *Repository) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0755
	return nil
}

func (r *Repository) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	if blockedPaths[req.Name] {
		r.FS.Logger.Debug("repo.lookup: blocked path", "user", *r.Owner.Login, "repo", *r.Name, "name", req.Name)
		return nil, fuse.ENOENT
	}

	// Check 404 cache first
	cacheKey := *r.Owner.Login + "/" + *r.Name + "/" + req.Name
	if r.FS.NotFoundCache.Has(cacheKey) {
		r.FS.Logger.Debug("repo.lookup: cached 404", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
		return nil, fuse.ENOENT
	}

	r.FS.Logger.Debug("repo.lookup: getting contents", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
	fileContent, directoryContent, _, err := r.FS.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, req.Name, nil)
	if err != nil {
		r.FS.Logger.Error("repo.lookup: failed to get contents", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name, "error", err)
		// Cache the 404 for 5 minutes
		r.FS.NotFoundCache.Add(cacheKey, 5*time.Minute)
		return nil, fuse.ENOENT
	}
	if fileContent != nil {
		r.FS.Logger.Debug("repo.lookup: found file", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
		return &File{FS: r.FS, Content: fileContent}, nil
	}
	r.FS.Logger.Debug("repo.lookup: found directory", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
	return &Dir{FS: r.FS, Contents: directoryContent}, nil
}

func (r *Repository) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	r.FS.Logger.Debug("repo.readdir: getting root directory contents", "user", *r.Owner.Login, "repo", *r.Name)
	_, directoryContent, _, err := r.FS.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, "", nil)
	if err != nil {
		r.FS.Logger.Error("repo.readdir: failed to get contents", "user", *r.Owner.Login, "repo", *r.Name, "error", err)
		return nil, fuse.ENOENT
	}

	var entries []fuse.Dirent
	for _, f := range directoryContent {
		entries = append(entries, fuse.Dirent{Name: *f.Name})
	}
	r.FS.Logger.Debug("repo.readdir: returning entries", "user", *r.Owner.Login, "repo", *r.Name, "count", len(entries))
	return entries, nil
}

type File struct {
	Content *github.RepositoryContent
	FS      *FS
}

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Size = uint64(*f.Content.Size)
	attr.Mode = 0755
	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f.FS.Logger.Debug("file.open: opening file", "path", *f.Content.Path, "size", *f.Content.Size)
	resp.Flags |= fuse.OpenNonSeekable

	// Check cache first
	if cached, ok := f.FS.FileContentCache.Get(*f.Content.Path); ok {
		f.FS.Logger.Debug("file.open: using cached content", "path", *f.Content.Path)
		return &FileHandle{
			r:      strings.NewReader(string(cached)),
			fs:     f.FS,
			path:   *f.Content.Path,
		}, nil
	}

	// Decode base64 from GitHub API
	decoded, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding, strings.NewReader(*f.Content.Content)))
	if err != nil {
		f.FS.Logger.Error("file.open: base64 decode error", "path", *f.Content.Path, "error", err)
		return nil, err
	}

	// Cache for 2 minutes
	f.FS.FileContentCache.Add(*f.Content.Path, decoded, 2*time.Minute)

	return &FileHandle{
		r:      strings.NewReader(string(decoded)),
		fs:     f.FS,
		path:   *f.Content.Path,
	}, nil
}

type FileHandle struct {
	r    io.Reader
	fs   *FS
	path string
}

var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := fh.r.Read(buf)
	resp.Data = buf[:n]
	if err != nil && err != io.EOF {
		fh.fs.Logger.Error("file.read: read error", "path", fh.path, "error", err, "bytes_read", n)
		return err
	}
	if n > 0 {
		fh.fs.Logger.Debug("file.read: read bytes", "path", fh.path, "bytes", n)
	}
	return err
}

type Dir struct {
	Contents []*github.RepositoryContent
	FS       *FS
}

func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0755
	return nil
}
