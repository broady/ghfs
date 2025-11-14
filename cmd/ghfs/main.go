package main

import (
	"context"
	"encoding/base64"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/google/go-github/v60/github"
)

func main() {
	log.SetFlags(0)

	// Parse arguments and require that we have the path.
	token := flag.String("token", "", "personal access token")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("path required")
	}
	log.Printf("mounting to: %s", flag.Arg(0))

	// Create FUSE connection.
	conn, err := fuse.Mount(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Create HTTP client with authentication if token is provided.
	var c *http.Client
	if *token != "" {
		c = &http.Client{
			Transport: &tokenTransport{
				token: *token,
				base:  http.DefaultTransport,
			},
		}
	}

	// Create filesystem.
	filesys := &FS{Client: github.NewClient(c)}
	if err := fs.Serve(conn, filesys); err != nil {
		log.Fatal(err)
	}
}

// tokenTransport is an http.RoundTripper that adds authentication token to requests.
type tokenTransport struct {
	token string
	base  http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t *tokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "token "+t.token)
	return t.base.RoundTrip(req)
}

// FS represents the
type FS struct {
	Client *github.Client
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
		return nil, fuse.ENOENT
	}
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
		return nil, fuse.ENOENT
	}
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
		return nil, fuse.ENOENT
	}
	if fileContent != nil {
		return &File{FS: r.FS, Content: fileContent}, nil
	}
	return &Dir{FS: r.FS, Contents: directoryContent}, nil
}

func (r *Repository) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	_, directoryContent, _, err := r.FS.Client.Repositories.GetContents(ctx, *r.Owner.Login, *r.Name, "", nil)
	if err != nil {
		return nil, fuse.ENOENT
	}

	var entries []fuse.Dirent
	for _, f := range directoryContent {
		entries = append(entries, fuse.Dirent{Name: *f.Name})
	}
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
	resp.Flags |= fuse.OpenNonSeekable
	return &FileHandle{r: base64.NewDecoder(base64.StdEncoding, strings.NewReader(*f.Content.Content))}, nil
}

type FileHandle struct {
	r io.Reader
}

var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := fh.r.Read(buf)
	resp.Data = buf[:n]
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
