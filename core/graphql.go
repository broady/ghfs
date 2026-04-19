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
	"time"
)

// jsonNull is the raw four-byte literal we compare against when deciding
// whether a top-level alias came back as JSON null (repo deleted,
// renamed, or permission-denied). Declared as a package-level byte slice
// so the bytes.Equal check in batchChunk doesn't re-allocate per alias.
var jsonNull = []byte("null")

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
// query. Since we no longer fetch tree contents (see comment on
// buildRepoMetadataQuery), each alias is dirt cheap server-side —
// pushedAt + isArchived + a one-commit walk on defaultBranchRef. We
// could probably push all 150 repos into a single query, but keeping
// the chunk bound means (a) smaller responses to parse, (b) partial
// failures localised to one chunk, (c) headroom if we ever add richer
// metadata fields. 50 is a safe ceiling with 20+ fields per alias.
const graphqlChunkSize = 50

// graphqlChunkConcurrency bounds parallel chunks. 4 leaves headroom
// for other API traffic (the suppressor, a simultaneous listRepos
// paginating, etc). Empirically GitHub serialises large aliased
// GraphQL queries per connection anyway, so pushing this higher
// doesn't help much on the authed-user API budget we already have.
const graphqlChunkConcurrency = 4

// graphqlTimeout caps a single metadata GraphQL POST. Metadata-only
// queries are fast (well under 1s) so 20s is generous — the timeout
// exists to bound pathological tails (network partition, GitHub slow
// failure mode), not to wait for a slow happy path.
const graphqlTimeout = 20 * time.Second

// maxGraphQLResponseBytes caps postGraphQL's body read so a rogue /
// compromised upstream can't OOM us by streaming an unbounded body.
// 32 MiB is ~3 orders of magnitude above our actual per-batch
// responses (a 50-repo batch is well under 200 KiB) and well below
// any sane server-side limit, so it's effectively only a
// defense-in-depth cap. We still allocate via io.ReadAll (we need
// the full body for json.Unmarshal anyway), just bounded.
const maxGraphQLResponseBytes = 32 << 20

// batchRepoMetadata populates each Repository's metadata fields
// (pushedAt, archived, defaultBranch, headCommitDate) via a single
// batched GraphQL query (per chunk).
//
// This path USED to also fetch root tree contents via
// `object(expression:"HEAD:")`, but that was expensive server-side
// (~125ms/repo on GitHub) and ended up slower than a concurrent
// REST `/contents/` fan-out (~10ms/repo under the 5K/hr budget).
// Tree contents now come from the REST prefetch; this batch is
// metadata-only and completes in well under 1s for a 150-repo user.
//
// Contract:
//
//   - ok:    repo metadata fields updated under apiMu, count returned.
//   - null:  alias-null (repo deleted / permission-denied) leaves
//            metadata untouched — the REST listing's seeded pushedAt
//            stays in place, which is the best we can do.
//   - error: full-chunk failure returned as err; caller logs + proceeds
//            with whatever partial success landed (metadata is
//            best-effort — the listing already gave us pushedAt).
//
// Caller is responsible for filtering out cloned repos (their metadata
// is still worth warming for ls -l mtimes, but typically the caller
// already has a non-cloned subset from the REST prefetch path) and
// for scoping ctx to a reasonable prefetch budget.
func (u *User) batchRepoMetadata(ctx context.Context, repos []*Repository) (int, error) {
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
// applies results to each Repository's per-inode metadata. Partial
// success is fine: GraphQL returns per-alias errors alongside usable
// data in a single response, and we write whatever came back.
func (u *User) batchChunk(ctx context.Context, chunk []*Repository) (int, error) {
	query, aliases := buildRepoMetadataQuery(chunk)

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

	// rateLimit is a sibling query field, not a repo alias — decode and
	// log it separately. Useful fleet signal: if cost ever drifts above
	// ~1 per chunk we should rethink chunk size. Scalars only, adds ~0
	// to the query's complexity cost. Silently skipped if the field
	// wasn't returned (older test fixtures, partial responses).
	if raw, ok := resp.Data["rateLimit"]; ok && len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), jsonNull) {
		var rl graphqlRateLimit
		if err := json.Unmarshal(raw, &rl); err == nil {
			u.Logger.Debug("graphql.batch: rate limit",
				"cost", rl.Cost,
				"remaining", rl.Remaining,
				"limit", rl.Limit,
				"node_count", rl.NodeCount,
				"chunk_size", len(chunk))
		}
	}

	written := 0
	for i, r := range chunk {
		alias := aliases[i]
		raw, ok := resp.Data[alias]
		if !ok || len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), jsonNull) {
			// Nothing for this alias — either it errored or the repo
			// itself is null (deleted, renamed, or permission-denied).
			// Leave the cache untouched; the REST fallback or a real
			// Readdir can retry. See graphqlResponse doc.
			continue
		}
		entry := &graphqlRepoResult{}
		if err := json.Unmarshal(raw, entry); err != nil {
			u.Logger.Debug("graphql.batch: decode alias failed",
				"alias", alias,
				"owner", r.Owner,
				"name", r.Name,
				"error", err)
			continue
		}
		applyGraphQLEntry(r, entry)
		written++
	}
	return written, nil
}

// buildRepoMetadataQuery builds an aliased GraphQL query string plus
// the alias names it used (in order, 1:1 with `repos`). Aliases are
// `r0`, `r1`, … to avoid any issue with special characters in repo
// names.
//
// The query is metadata-only: pushedAt, isArchived, defaultBranchRef
// (name + head commit date). Notably NOT included is
// `object(expression:"HEAD:")` — fetching root tree contents via
// GraphQL turned out to be ~125ms/repo server-side (GitHub walks the
// tree to resolve every entry's OID + blob byteSize), which serialised
// behind aliased-field budgets and made the batch slower than a
// concurrent REST /contents/ fan-out. Tree contents live on the REST
// path (prefetchRepoContentsREST); this batch is CPU-cheap on GitHub
// and completes in well under 1s for 150-repo users.
//
// Alongside the aliased `repository(...)` fields, the query also asks
// for a top-level `rateLimit` — scalars only, free against GitHub's
// complexity cost — so batchChunk can log the observed cost per batch
// as a fleet signal.
func buildRepoMetadataQuery(repos []*Repository) (string, []string) {
	aliases := make([]string, len(repos))
	var b strings.Builder
	// Per-repo estimate ~200B after dropping the tree subquery.
	b.Grow(128 + 200*len(repos))
	b.WriteString("query {\n")
	// rateLimit sits alongside the aliased repos as a sibling field in
	// data; batchChunk decodes it separately from the alias map and
	// logs cost/remaining/nodeCount at debug. The field selects only
	// scalars so it contributes 0 to query complexity.
	b.WriteString("  rateLimit { cost remaining resetAt limit nodeCount }\n")
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
		fmt.Fprintf(&b, "  %s: repository(owner:%s, name:%s) {\n", alias, owner, name)
		// Repo-level metadata: pushedAt → Repository.Getattr mtime so
		// `ls -lt ~/github.com/<owner>/` orders by activity instead of
		// the uniform zero we used to report. defaultBranchRef + its
		// commit's committedDate/oid land on the struct for future use
		// (branch symlinks, committedDate as per-file fallback if we
		// ever revisit it) but aren't surfaced yet. isArchived is
		// captured for the same "stored but dormant" reason; if/when we
		// plumb it through an xattr (user.github.archived=1) the data's
		// already there without another round trip.
		//
		// Nullability to handle in applyGraphQLEntry:
		//   pushedAt          — non-null in practice for every repo the
		//                       API returns (reflects creation time if
		//                       never pushed), but typed optional.
		//   isArchived        — always present when the repo itself is.
		//   defaultBranchRef  — null for empty repos (no commits).
		//   target            — null if the ref exists but points at a
		//                       deleted object; rare but possible.
		//   __typename != "Commit" — annotated-tag-as-default-branch.
		//                       Never observed in sampling but handled.
		b.WriteString("    pushedAt\n")
		b.WriteString("    isArchived\n")
		b.WriteString("    defaultBranchRef {\n")
		b.WriteString("      name\n")
		b.WriteString("      target {\n")
		b.WriteString("        __typename\n")
		b.WriteString("        ... on Commit { committedDate oid }\n")
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
// `Data` is an alias-keyed map whose values we decode lazily (via
// json.RawMessage) for two reasons: (a) the top-level `rateLimit` field
// sits next to the repo aliases but has a completely different shape,
// and (b) distinguishing "alias present but JSON null" (repo deleted /
// permission-denied — cache untouched) from "alias present with a real
// object" needs a raw-bytes peek anyway. When a single alias 404s or
// errors the server still returns 200 with the alias set to null and
// an entry in `Errors` keyed by path.
type graphqlResponse struct {
	Data   map[string]json.RawMessage `json:"data"`
	Errors []graphqlError             `json:"errors,omitempty"`
}

type graphqlError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Path    []any  `json:"path,omitempty"`
}

// graphqlRateLimit mirrors the top-level `rateLimit` field we select in
// buildRootContentsQuery. Cost is what actually matters (measured
// against GitHub's 5000/hr primary budget); the rest is included for
// completeness / future-proofing if we ever want to back off when
// remaining gets low. nodeCount came back 0 in pre-flight probing
// against broady — GitHub only reports it for queries near the
// complexity ceiling.
type graphqlRateLimit struct {
	Cost      int    `json:"cost"`
	Remaining int    `json:"remaining"`
	Limit     int    `json:"limit"`
	ResetAt   string `json:"resetAt"`
	NodeCount int    `json:"nodeCount"`
}

// graphqlRepoResult is the decoded per-alias repository payload. Every
// field is a pointer/optional because GraphQL returns null for missing
// data rather than omitting the key — we use nil-vs-set to decide
// whether to overwrite the Repository's cached metadata or leave it
// alone.
type graphqlRepoResult struct {
	PushedAt         *time.Time        `json:"pushedAt,omitempty"`
	IsArchived       *bool             `json:"isArchived,omitempty"`
	DefaultBranchRef *graphqlBranchRef `json:"defaultBranchRef,omitempty"`
}

// graphqlBranchRef is the `defaultBranchRef` subtree. Null for empty
// repos (no commits). Name is always set when the ref itself is
// non-null; Target can still be null if the ref points at a deleted
// object — rare but worth not crashing over.
type graphqlBranchRef struct {
	Name   string               `json:"name"`
	Target *graphqlBranchTarget `json:"target,omitempty"`
}

// graphqlBranchTarget is the commit the default branch points at. We
// peek at __typename because a ref could theoretically target an
// annotated tag; committedDate/oid are only valid when it's a Commit.
type graphqlBranchTarget struct {
	Typename      string     `json:"__typename"`
	CommittedDate *time.Time `json:"committedDate,omitempty"`
	OID           string     `json:"oid,omitempty"`
}

// applyGraphQLEntry writes a decoded `r0: repository { ... }` block
// into r's per-inode metadata under apiMu — same mutex discipline as
// cachedRootContents so a simultaneous Readdir / Getattr caller sees
// either the old cached values or the new ones, never a torn write.
//
// Metadata-only by design: tree listings come from the REST prefetch
// path (prefetchRepoContentsREST). Each field is guarded with a nil
// check so a partial response never clobbers a previously-known value
// (e.g. seeded from the /user/repos listing at buildEntries time) with
// a zero.
//
// Not touched here: apiRootItems / apiRootErrno / apiRootCachedAt.
// The REST path owns those — co-writing from GraphQL would create a
// "which one wrote last" confusion that has no benefit now that we've
// dropped the tree subquery. If a future refactor re-adds tree via
// GraphQL (unlikely — REST is cheaper), that code goes here.
func applyGraphQLEntry(r *Repository, entry *graphqlRepoResult) {
	r.apiMu.Lock()
	defer r.apiMu.Unlock()

	if entry.PushedAt != nil {
		r.pushedAt = *entry.PushedAt
	}
	if entry.IsArchived != nil {
		r.archived = *entry.IsArchived
	}
	if ref := entry.DefaultBranchRef; ref != nil {
		r.defaultBranch = ref.Name
		// Only trust committedDate when the target is a Commit. An
		// annotated-tag target (Typename == "Tag") would point at a
		// tag object, not a commit — we'd have to dereference further
		// to get a date, and that's a complexity we don't need yet.
		if t := ref.Target; t != nil && t.Typename == "Commit" && t.CommittedDate != nil {
			r.headCommitDate = *t.CommittedDate
		}
	}
}
