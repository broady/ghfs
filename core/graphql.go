package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/go-github/v60/github"
)

// graphqlEndpointForTest is the URL batchChunk POSTs to. In production
// it's pinned to api.github.com; tests override it to point at
// httptest.Server. We keep it as a package var rather than a const so
// tests have a tight seam without needing to plumb a URL field
// through User / FS (and so that a single-process binary can't
// accidentally target the wrong endpoint at runtime — it's written
// exactly once at init).
//
// GitHub Enterprise Server exposes GraphQL at <host>/api/graphql,
// which we don't yet support — main.go doesn't configure the REST
// BaseURL either, so ghfs is github.com-only. When GHES support
// lands, derive this from github.Client.BaseURL per User.
var graphqlEndpointForTest = "https://api.github.com/graphql"

// graphqlChunkSize bounds how many repos we shove into one GraphQL
// query. GitHub's public complexity limit is 500K result nodes and
// aliased-field queries rarely get close, but smaller chunks mean
// (a) smaller responses to parse, (b) partial failures localised to
// one chunk, (c) lower chance of tripping any per-request secondary
// rate-limit heuristics. 50 empirically keeps the query string under
// ~10 KB and the response under ~200 KB for typical repos.
const graphqlChunkSize = 50

// graphqlChunkConcurrency bounds parallel chunks. Each chunk is already
// doing the work of ~50 REST calls, so we don't need much fan-out here.
// 4 leaves headroom for other API traffic (the suppressor, a
// simultaneous listRepos paginating, etc).
const graphqlChunkConcurrency = 4

// graphqlTimeout caps a single batched GraphQL POST. GitHub's
// documented upper-bound is 10s for GraphQL queries; we give a little
// slack for slow networks. Same order as userPrefetchTimeout but
// scoped per-HTTP-request so a long tail can't starve the rest of
// the prefetch run.
const graphqlTimeout = 20 * time.Second

// maxGraphQLResponseBytes caps postGraphQL's body read so a rogue /
// compromised upstream can't OOM us by streaming an unbounded body.
// 32 MiB is ~3 orders of magnitude above our actual per-batch
// responses (a 50-repo batch is well under 200 KiB) and well below
// any sane server-side limit, so it's effectively only a
// defense-in-depth cap. We still allocate via io.ReadAll (we need
// the full body for json.Unmarshal anyway), just bounded.
const maxGraphQLResponseBytes = 32 << 20

// batchRepoRootContents populates each Repository's apiRootItems cache
// via a single batched GraphQL query (per chunk).
//
// Contract matches what cachedRootContents would write on success:
//
//   - ok:     apiRootItems = []*github.RepositoryContent{…}, apiRootErrno = 0
//   - empty:  apiRootItems = nil, apiRootErrno = syscall.ENOENT
//     (no HEAD / no commits / repo not visible)
//   - error:  cache left untouched (matches the REST path's EIO-is-not-cached
//     rule, so the next on-demand Readdir retries via contentsAt)
//
// Returns the count of Repositories whose cache we successfully wrote
// and any transport-level error (which callers should log + fall back
// from, not poison the cache with).
//
// Caller is responsible for filtering out cloned repos (their Readdir
// serves from the local tree, so warming the API cache is pointless)
// and for scoping ctx to a reasonable prefetch budget.
func (u *User) batchRepoRootContents(ctx context.Context, repos []*Repository) (int, error) {
	if len(repos) == 0 {
		return 0, nil
	}

	// Chunk and fan out. Each chunk is one POST; within a chunk all
	// repos are queried in a single aliased query.
	var (
		wg      sync.WaitGroup
		sem     = make(chan struct{}, graphqlChunkConcurrency)
		written int64
		mu      sync.Mutex
		firstErr error
	)

	for i := 0; i < len(repos); i += graphqlChunkSize {
		end := i + graphqlChunkSize
		if end > len(repos) {
			end = len(repos)
		}
		chunk := repos[i:end]
		wg.Add(1)
		sem <- struct{}{}
		go func(chunk []*Repository) {
			defer wg.Done()
			defer func() { <-sem }()
			cctx, cancel := context.WithTimeout(ctx, graphqlTimeout)
			defer cancel()
			n, err := u.batchChunk(cctx, chunk)
			mu.Lock()
			written += int64(n)
			if err != nil && firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}(chunk)
	}
	wg.Wait()

	return int(written), firstErr
}

// batchChunk issues exactly one GraphQL POST for the given chunk and
// applies results to each Repository's per-inode cache. Partial
// success is fine: GraphQL returns per-alias errors alongside usable
// data in a single response, and we write whatever came back.
func (u *User) batchChunk(ctx context.Context, chunk []*Repository) (int, error) {
	query, aliases := buildRootContentsQuery(chunk)

	body, err := postGraphQL(ctx, u.Client.Client(), query, nil)
	if err != nil {
		return 0, err
	}

	var resp graphqlResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("graphql: decode response: %w", err)
	}

	// Log per-alias errors but keep processing — partial data is still useful.
	if len(resp.Errors) > 0 {
		for _, e := range resp.Errors {
			u.Logger.Debug("graphql.batch: alias error",
				"type", e.Type,
				"path", fmt.Sprint(e.Path),
				"message", e.Message)
		}
	}

	written := 0
	for i, r := range chunk {
		alias := aliases[i]
		entry, ok := resp.Data[alias]
		if !ok || entry == nil {
			// Nothing for this alias — either it errored or the repo
			// itself is null (deleted, renamed, or permission-denied).
			// Leave the cache untouched; the REST fallback or a real
			// Readdir can retry. See graphqlResponse doc.
			continue
		}
		applyGraphQLEntry(r, entry)
		written++
	}
	return written, nil
}

// buildRootContentsQuery builds an aliased GraphQL query string plus the
// alias names it used (in order, 1:1 with `repos`). Aliases are `r0`,
// `r1`, … to avoid any issue with special characters in repo names.
func buildRootContentsQuery(repos []*Repository) (string, []string) {
	aliases := make([]string, len(repos))
	var b strings.Builder
	b.Grow(64 + 160*len(repos))
	b.WriteString("query {\n")
	for i, r := range repos {
		alias := "r" + strconv.Itoa(i)
		aliases[i] = alias
		// Escape owner/name for GraphQL string literals. Repo names
		// and owners are always [A-Za-z0-9._-] per GitHub rules, so
		// no embedded quotes/backslashes are possible, but JSON-encode
		// defensively so a future change to the naming rules doesn't
		// silently produce an injection.
		owner, _ := json.Marshal(r.Owner)
		name, _ := json.Marshal(r.Name)
		// expression:"HEAD:" means "the tree at HEAD". For non-default
		// refs (r.Ref != "") we'd use <ref>: but prefetch only fires
		// on the plain Repository inodes created by User.Readdir,
		// which all have Ref="". Ref-pinned repos are rare and don't
		// benefit from the prefetch.
		fmt.Fprintf(&b, "  %s: repository(owner:%s, name:%s) {\n", alias, owner, name)
		b.WriteString("    object(expression:\"HEAD:\") {\n")
		b.WriteString("      __typename\n")
		b.WriteString("      ... on Tree {\n")
		b.WriteString("        entries {\n")
		b.WriteString("          name\n")
		b.WriteString("          type\n")
		b.WriteString("          mode\n")
		b.WriteString("          oid\n")
		b.WriteString("          object {\n")
		b.WriteString("            __typename\n")
		b.WriteString("            ... on Blob { byteSize }\n")
		b.WriteString("          }\n")
		b.WriteString("        }\n")
		b.WriteString("      }\n")
		b.WriteString("    }\n")
		b.WriteString("  }\n")
	}
	b.WriteString("}\n")
	return b.String(), aliases
}

// postGraphQL executes a GraphQL query against GitHub using the given
// HTTP client (which must already carry auth). Returns the raw
// response body on 2xx, a typed error otherwise.
func postGraphQL(ctx context.Context, hc *http.Client, query string, variables map[string]any) ([]byte, error) {
	payload := map[string]any{"query": query}
	if len(variables) > 0 {
		payload["variables"] = variables
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("graphql: encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, graphqlEndpointForTest, bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("graphql: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphql: http do: %w", err)
	}
	defer resp.Body.Close()
	// Cap body size defensively so a misbehaving upstream can't OOM
	// us. Read one byte past the limit so we can distinguish
	// "exactly at limit" from "truncated at limit".
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxGraphQLResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("graphql: read body: %w", err)
	}
	if int64(len(body)) > maxGraphQLResponseBytes {
		return nil, fmt.Errorf("graphql: response exceeds %d bytes", maxGraphQLResponseBytes)
	}
	if resp.StatusCode/100 != 2 {
		// Truncate to keep logs sane. 512 is enough to capture
		// GitHub's JSON error envelope.
		snippet := body
		if len(snippet) > 512 {
			snippet = snippet[:512]
		}
		return nil, fmt.Errorf("graphql: http %d: %s", resp.StatusCode, snippet)
	}
	return body, nil
}

// graphqlResponse is the GitHub GraphQL envelope for our batched query.
// `Data` is an alias-keyed map; each value is a `repository` result or
// null (repo not found / not visible). When a single alias 404s or
// errors the server still returns 200 with the alias set to null and
// an entry in `Errors` keyed by path.
type graphqlResponse struct {
	Data   map[string]*graphqlRepoResult `json:"data"`
	Errors []graphqlError                `json:"errors,omitempty"`
}

type graphqlError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Path    []any  `json:"path,omitempty"`
}

type graphqlRepoResult struct {
	Object *graphqlObject `json:"object"`
}

// graphqlObject wraps the `object(expression:"HEAD:")` result. If the
// repo has no HEAD (empty repo, no commits yet), Object is null on the
// wire and we decode that as ENOENT below. Non-Tree typenames (e.g.
// someone pointed HEAD at a blob — unusual but legal) are treated as
// "nothing to list" and mapped to a successful-but-empty listing so
// the repo directory appears empty rather than broken.
type graphqlObject struct {
	Typename string          `json:"__typename"`
	Entries  []graphqlTreeEntry `json:"entries,omitempty"`
}

type graphqlTreeEntry struct {
	Name   string               `json:"name"`
	Type   string               `json:"type"` // "blob" | "tree" | "commit"
	Mode   int                  `json:"mode"`
	OID    string               `json:"oid"`
	Object *graphqlEntryObject  `json:"object,omitempty"`
}

type graphqlEntryObject struct {
	Typename string `json:"__typename"`
	ByteSize *int64 `json:"byteSize,omitempty"`
}

// applyGraphQLEntry writes a decoded `r0: repository { object { ... } }`
// block into r's per-inode cache, using the same mutex discipline as
// cachedRootContents so a simultaneous Readdir caller sees either the
// old cached value or the new one, never a torn write.
//
// If entry.Object is nil the repo has no HEAD → ENOENT (same semantics
// as the REST /contents/ 404 we negative-cache today). If Object is
// present but not a Tree we write an empty listing — unusual but not
// an error.
func applyGraphQLEntry(r *Repository, entry *graphqlRepoResult) {
	r.apiMu.Lock()
	defer r.apiMu.Unlock()

	if entry.Object == nil {
		r.apiRootItems = nil
		r.apiRootErrno = syscall.ENOENT
		r.apiRootCachedAt = time.Now()
		return
	}

	items := make([]*github.RepositoryContent, 0, len(entry.Object.Entries))
	for i := range entry.Object.Entries {
		e := entry.Object.Entries[i]
		// Map GraphQL TreeEntry → REST RepositoryContent. Naming:
		//   tree            → "dir"
		//   blob (mode 0120000 symlink) → "symlink"
		//   blob (other)    → "file"
		//   commit (submodule) → skip entirely (matches newInodeFromContent)
		kind := contentTypeFromTreeEntry(e)
		if kind == "" {
			continue
		}
		name := e.Name
		oid := e.OID
		typ := kind
		content := &github.RepositoryContent{
			Name: &name,
			// Path equals Name for root listings (see
			// newInodeFromContent which falls back to Name when Path
			// is nil). Setting both keeps downstream code simple.
			Path: &name,
			SHA:  &oid,
			Type: &typ,
		}
		if e.Object != nil && e.Object.ByteSize != nil {
			// github.RepositoryContent.Size is *int, not *int64.
			sz := int(*e.Object.ByteSize)
			content.Size = &sz
		}
		items = append(items, content)
	}
	r.apiRootItems = items
	r.apiRootErrno = 0
	r.apiRootCachedAt = time.Now()
}

// contentTypeFromTreeEntry maps a GraphQL TreeEntry to the REST
// Contents-API `type` string (which the rest of core/ consumes via
// newInodeFromContent + contentMode). Returns "" for entries we
// deliberately drop (submodules).
func contentTypeFromTreeEntry(e graphqlTreeEntry) string {
	switch e.Type {
	case "tree":
		return "dir"
	case "blob":
		// Symlink mode in git is 0120000 octal = 40960 decimal.
		// The REST Contents API reports these as type:"symlink".
		if e.Mode == 0o120000 {
			return "symlink"
		}
		return "file"
	default:
		// "commit" = submodule; everything else unknown.
		return ""
	}
}
