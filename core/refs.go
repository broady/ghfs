package core

import (
	"fmt"
	"strings"
)

// ParseRepoAndRef splits a User-directory entry name of the form
// "<repo>" or "<repo>@<ref>" into its parts. Returns an error if the
// ref segment is present but empty, or if the repo name is empty.
//
// <ref> may be a branch, tag, or commit SHA — ResolveRef in the
// gitstore layer handles which.
func ParseRepoAndRef(name string) (repo, ref string, err error) {
	i := strings.Index(name, "@")
	if i == -1 {
		if name == "" {
			return "", "", fmt.Errorf("empty repo name")
		}
		return name, "", nil
	}
	repo = name[:i]
	ref = name[i+1:]
	if repo == "" {
		return "", "", fmt.Errorf("empty repo name in %q", name)
	}
	if ref == "" {
		return "", "", fmt.Errorf("empty ref after '@' in %q", name)
	}
	if strings.ContainsAny(ref, "/\x00") {
		// Note: branch names CAN contain '/' (e.g. release-branch/go1.21)
		// but we don't want to allow them here because a single FUSE
		// directory entry can't contain '/'. Users with slash-refs can
		// substitute '_' or use the SHA. Reject up-front to give a
		// clear error.
		return "", "", fmt.Errorf("ref %q must not contain '/' or NUL; use the commit SHA or a non-slashed branch/tag", ref)
	}
	return repo, ref, nil
}
