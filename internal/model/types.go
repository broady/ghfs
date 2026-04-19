// SPDX-License-Identifier: Apache-2.0
//
// Derived from github.com/cloudflare/artifact-fs. A small subset of the
// upstream types, modified for use in ghfs (read-only, git-backed).

// Package model defines the in-memory types shared between the gitstore,
// hydrator, reposrc, and core FUSE layers.
package model

import "time"

// RepoID uniquely identifies a repository within a ghfs process. For ghfs
// this is "<owner>/<repo>".
type RepoID string

// RepoConfig carries everything gitstore / hydrator need to operate on a
// single clone. Only the fields used downstream are populated; the rest of
// the struct (matching the upstream shape) is intentionally absent.
type RepoConfig struct {
	ID           RepoID
	Name         string // "<owner>/<repo>"
	RemoteURL    string // https://token@github.com/<owner>/<repo>.git, may embed credentials
	GitDir       string // absolute path to the bare/blobless clone's git dir
	BlobCacheDir string // absolute path to the shared content-addressed blob cache root
}

// BaseNode is one entry in the flattened tree index built from a git
// ls-tree. Inode IDs are assigned at runtime by the FUSE layer; BaseNode
// carries only git-derived metadata.
type BaseNode struct {
	RepoID    RepoID
	Path      string // clean path relative to repo root, "." for root
	Type      string // file, dir, symlink
	Mode      uint32 // git mode bits (0o100644 / 0o100755 / 0o040000 / 0o120000)
	ObjectOID string // blob/tree OID; empty for implicit dirs
	SizeState string // unknown, known
	SizeBytes int64
}

// HydrationTask is a request to materialise a blob into the shared cache.
type HydrationTask struct {
	RepoID     RepoID
	Path       string
	ObjectOID  string
	Priority   int
	Reason     string
	EnqueuedAt time.Time
}
