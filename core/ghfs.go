package core

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/google/go-github/v60/github"
)

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

func (r *Root) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	if strings.HasPrefix(req.Name, ".") {
		return nil, fuse.ENOENT
	}

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
	if strings.HasPrefix(req.Name, ".") {
		return nil, fuse.ENOENT
	}

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
	if strings.HasPrefix(req.Name, ".") {
		return nil, fuse.ENOENT
	}

	fileContent, directoryContent, _, err := r.FS.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, req.Name, nil)
	if err != nil {
		r.FS.Logger.Error("repo.lookup: failed to get contents", "user", *r.Owner.Login, "repo", *r.Name, "path", req.Name, "error", err)
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
	return &FileHandle{
		r:      base64.NewDecoder(base64.StdEncoding, strings.NewReader(*f.Content.Content)),
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
