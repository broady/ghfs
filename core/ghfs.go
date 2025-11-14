package core

import (
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

// GitHubHTTPTransport handles authentication, caching headers, logging, and HTTP requests.
// It wraps httpcache.Transport to add all necessary middleware in one place.
type GitHubHTTPTransport struct {
	Token  string // Empty if unauthenticated
	Base   http.RoundTripper
	Logger *slog.Logger
}

// RoundTrip implements http.RoundTripper.
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

	// Add cache headers if not already present
	if resp.Header.Get("Cache-Control") == "" {
		if resp.StatusCode == http.StatusNotFound {
			// Cache 404s for 5 minutes
			resp.Header.Set("Cache-Control", "public, max-age=300")
		} else if resp.StatusCode == http.StatusOK {
			// Cache successful responses for 60 seconds to handle rapid re-reads
			resp.Header.Set("Cache-Control", "public, max-age=60")
		}
	}

	// Ensure responses have proper cache validation headers
	if resp.Header.Get("ETag") == "" && resp.StatusCode != http.StatusNotFound {
		// Add ETag if missing for successful responses
		resp.Header.Set("ETag", "\""+req.URL.String()+"\"")
	}
	if resp.StatusCode == http.StatusNotFound && resp.Header.Get("ETag") == "" {
		resp.Header.Set("ETag", "\"404-"+req.URL.String()+"\"")
	}

	// Ensure Date header is set (required by httpcache)
	if resp.Header.Get("Date") == "" {
		resp.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}

	// Remove Vary header and related X-Varied-* headers to allow caching
	// GitHub sends Vary which prevents httpcache from reusing responses if request headers differ
	resp.Header.Del("Vary")
	// Also remove any X-Varied-* headers that httpcache may have cached
	for key := range resp.Header {
		if strings.HasPrefix(key, "X-Varied-") {
			resp.Header.Del(key)
		}
	}

	// Log the request with cache status
	if t.Logger != nil {
		cacheStatus := "miss"
		if resp.Header.Get("X-From-Cache") == "1" {
			cacheStatus = "hit"
		}
		t.Logger.Debug("http.request", "method", req.Method, "url", req.URL.Path, "status", resp.StatusCode, "cache", cacheStatus)
	}

	return resp, err
}

// FS represents the FUSE filesystem
type FS struct {
	Client *github.Client
	Logger *slog.Logger
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

var _ = fs.HandleReadDirAller(&Root{})

func (r *Root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var entries []fuse.Dirent

	// Get the authenticated user's information
	r.FS.Logger.Debug("root.readdir: getting authenticated user")
	user, _, err := r.FS.Client.Users.Get(ctx, "")
	if err != nil {
		r.FS.Logger.Error("root.readdir: failed to get authenticated user", "error", err)
		// If no token is provided or user can't be authenticated, return empty list
		// This allows the filesystem to still work, just without the user/org listing
		return entries, nil
	}

	// Add the authenticated user as a directory entry
	if user.Login != nil {
		r.FS.Logger.Debug("root.readdir: adding authenticated user", "login", *user.Login)
		entries = append(entries, fuse.Dirent{
			Name: *user.Login,
			Type: fuse.DT_Dir,
		})
	}

	// Get organizations the user has access to
	r.FS.Logger.Debug("root.readdir: getting user organizations")
	// List all organizations for the authenticated user with pagination
	opts := &github.ListOptions{PerPage: 100}
	for {
		orgs, resp, err := r.FS.Client.Organizations.List(ctx, "", opts)
		if err != nil {
			r.FS.Logger.Error("root.readdir: failed to get organizations", "error", err)
			break // Continue with what we have
		}

		for _, org := range orgs {
			if org.Login != nil {
				r.FS.Logger.Debug("root.readdir: adding organization", "org", *org.Login)
				entries = append(entries, fuse.Dirent{
					Name: *org.Login,
					Type: fuse.DT_Dir,
				})
			}
		}

		// Check if there are more pages
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	r.FS.Logger.Debug("root.readdir: returning entries", "count", len(entries))
	return entries, nil
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
	FS    *FS
	Repos map[string]*github.Repository
	mu    sync.RWMutex // Protects Repos
}

func (u *User) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0755
	return nil
}

var _ = fs.HandleReadDirAller(&User{})

func (u *User) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	u.FS.Logger.Debug("user.readdir: getting repositories", "user", *u.Login)
	var entries []fuse.Dirent

	// Initialize cache if needed
	u.mu.Lock()
	if u.Repos == nil {
		u.Repos = make(map[string]*github.Repository)
	}
	u.mu.Unlock()

	// List all repositories for the user with pagination
	opts := &github.RepositoryListOptions{
		ListOptions: github.ListOptions{PerPage: 100},
	}
	for {
		repos, resp, err := u.FS.Client.Repositories.List(ctx, *u.Login, opts)
		if err != nil {
			u.FS.Logger.Error("user.readdir: failed to get repositories", "user", *u.Login, "error", err)
			return nil, fuse.ENOENT
		}

		for _, repo := range repos {
			u.FS.Logger.Debug("user.readdir: adding repository", "user", *u.Login, "repo", *repo.Name)
			entries = append(entries, fuse.Dirent{
				Name: *repo.Name,
				Type: fuse.DT_Dir,
			})
			// Cache the repository for later lookups
			u.mu.Lock()
			u.Repos[*repo.Name] = repo
			u.mu.Unlock()
		}

		// Check if there are more pages
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	u.FS.Logger.Debug("user.readdir: returning entries", "user", *u.Login, "count", len(entries))
	return entries, nil
}

func (u *User) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	if blockedPaths[req.Name] {
		u.FS.Logger.Debug("user.lookup: blocked path", "user", *u.Login, "name", req.Name)
		return nil, fuse.ENOENT
	}

	u.FS.Logger.Debug("user.lookup: getting repository", "user", *u.Login, "repo", req.Name)

	// Check if we have the repository cached from a previous ReadDirAll
	u.mu.RLock()
	var r *github.Repository
	if u.Repos != nil {
		if cached, ok := u.Repos[req.Name]; ok {
			u.mu.RUnlock()
			u.FS.Logger.Debug("user.lookup: found repository in cache", "user", *u.Login, "repo", req.Name)
			return &Repository{FS: u.FS, Repository: cached}, nil
		}
	}
	u.mu.RUnlock()

	// Fall back to API call if not cached
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
	FS       *FS
	mu       sync.Mutex
	Contents []*github.RepositoryContent // Cached directory contents
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

	// Check cache first
	r.mu.Lock()
	if r.Contents != nil {
		for _, content := range r.Contents {
			if *content.Name == req.Name {
				r.mu.Unlock()
				if *content.Type == "file" {
					r.FS.Logger.Debug("repo.lookup: found file (cached)", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
					return &File{FS: r.FS, Content: content, Owner: *r.Owner.Login, Repo: *r.Name}, nil
				}
				r.FS.Logger.Debug("repo.lookup: found directory (cached)", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
				return &Dir{FS: r.FS, Contents: []*github.RepositoryContent{content}, Owner: *r.Owner.Login, Repo: *r.Name}, nil
			}
		}
	}
	r.mu.Unlock()

	// Cache miss - fetch from API
	r.FS.Logger.Debug("repo.lookup: getting contents", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
	fileContent, directoryContent, _, err := r.FS.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, req.Name, nil)
	if err != nil {
		r.FS.Logger.Error("repo.lookup: failed to get contents", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name, "error", err)
		return nil, fuse.ENOENT
	}
	if fileContent != nil {
		r.FS.Logger.Debug("repo.lookup: found file", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
		return &File{FS: r.FS, Content: fileContent, Owner: *r.Owner.Login, Repo: *r.Name}, nil
	}
	r.FS.Logger.Debug("repo.lookup: found directory", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name)
	return &Dir{FS: r.FS, Contents: directoryContent, Owner: *r.Owner.Login, Repo: *r.Name}, nil
}

func (r *Repository) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	r.FS.Logger.Debug("repo.readdir: getting root directory contents", "user", *r.Owner.Login, "repo", *r.Name)
	_, directoryContent, _, err := r.FS.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, "", nil)
	if err != nil {
		r.FS.Logger.Error("repo.readdir: failed to get contents", "user", *r.Owner.Login, "repo", *r.Name, "error", err)
		return nil, fuse.ENOENT
	}

	// Cache the contents for use in Lookup calls
	r.mu.Lock()
	r.Contents = directoryContent
	r.mu.Unlock()

	var entries []fuse.Dirent
	for _, f := range directoryContent {
		var dtype fuse.DirentType
		if *f.Type == "dir" {
			dtype = fuse.DT_Dir
		} else {
			dtype = fuse.DT_File
		}
		entries = append(entries, fuse.Dirent{
			Name: *f.Name,
			Type: dtype,
		})
	}
	r.FS.Logger.Debug("repo.readdir: returning entries", "user", *r.Owner.Login, "repo", *r.Name, "count", len(entries))
	return entries, nil
}

type File struct {
	Content *github.RepositoryContent
	FS      *FS
	Owner   string
	Repo    string
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

	// If Content is not populated (e.g., from directory listing), fetch it
	var content string
	if f.Content.Content == nil || *f.Content.Content == "" {
		fileContent, _, _, err := f.FS.Client.Repositories.GetContents(ctx, f.Owner, f.Repo, *f.Content.Path, nil)
		if err != nil {
			f.FS.Logger.Error("file.open: failed to get file content", "path", *f.Content.Path, "error", err)
			return nil, err
		}
		if fileContent == nil {
			f.FS.Logger.Error("file.open: got nil file content", "path", *f.Content.Path)
			return nil, fuse.EIO
		}
		content = *fileContent.Content
	} else {
		content = *f.Content.Content
	}

	// Decode base64 from GitHub API
	decoded, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding, strings.NewReader(content)))
	if err != nil {
		f.FS.Logger.Error("file.open: base64 decode error", "path", *f.Content.Path, "error", err)
		return nil, err
	}

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
	Owner    string
	Repo     string
}

var _ = fs.HandleReadDirAller(&Dir{})

func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0755
	return nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var entries []fuse.Dirent
	for _, content := range d.Contents {
		var dtype fuse.DirentType
		if *content.Type == "dir" {
			dtype = fuse.DT_Dir
		} else {
			dtype = fuse.DT_File
		}
		entries = append(entries, fuse.Dirent{
			Name: *content.Name,
			Type: dtype,
		})
	}
	return entries, nil
}

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	for _, content := range d.Contents {
		if *content.Name == req.Name {
			if *content.Type == "dir" {
				// For directories, we need to fetch the contents
				_, dirContents, _, err := d.FS.Client.Repositories.GetContents(ctx, d.Owner, d.Repo, *content.Path, nil)
				if err != nil {
					d.FS.Logger.Error("dir.lookup: failed to get directory contents", "owner", d.Owner, "repo", d.Repo, "path", *content.Path, "error", err)
					return nil, fuse.ENOENT
				}
				return &Dir{FS: d.FS, Contents: dirContents, Owner: d.Owner, Repo: d.Repo}, nil
			} else {
				return &File{FS: d.FS, Content: content, Owner: d.Owner, Repo: d.Repo}, nil
			}
		}
	}
	return nil, fuse.ENOENT
}
