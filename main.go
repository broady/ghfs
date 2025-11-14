package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/google/go-github/v60/github"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/broady/ghfs/core"
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
			Transport: &core.TokenTransport{
				Token: *token,
				Base:  http.DefaultTransport,
			},
		}
	}

	// Create filesystem.
	filesys := &core.FS{Client: github.NewClient(c)}
	if err := fs.Serve(conn, filesys); err != nil {
		log.Fatal(err)
	}
}
