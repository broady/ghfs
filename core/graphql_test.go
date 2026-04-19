package core

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-github/v77/github"
)

// TestBuildRepoMetadataQuery_AliasesAndEscaping checks that every repo
// gets a distinct alias and that owner/name are JSON-encoded (defensive
// against a future widening of GitHub's naming rules that would permit
// embedded quotes).
func TestBuildRepoMetadataQuery_AliasesAndEscaping(t *testing.T) {
	t.Parallel()

	repos := []*Repository{
		{Owner: "broady", Name: "ghfs"},
		{Owner: "broady", Name: "with\"quote"}, // would never happen in practice, but we should handle it
	}
	q, aliases := buildRepoMetadataQuery(repos)

	if len(aliases) != len(repos) {
		t.Fatalf("alias count = %d, want %d", len(aliases), len(repos))
	}
	if aliases[0] != "r0" || aliases[1] != "r1" {
		t.Errorf("aliases = %v, want [r0 r1]", aliases)
	}
	// Owners + names must be JSON-quoted inside the query; a bare
	// embedded `"` would break the GraphQL document.
	for _, want := range []string{
		`r0: repository(owner:"broady", name:"ghfs")`,
		// Second repo's name is `with"quote` → JSON-encoded as `"with\"quote"`.
		`r1: repository(owner:"broady", name:"with\"quote")`,
	} {
		if !strings.Contains(q, want) {
			t.Errorf("query missing %q\n---query---\n%s", want, q)
		}
	}
}

// TestBuildRepoMetadataQuery_IncludesMetadata asserts the
// repo-level fields + rateLimit envelope are wired into the query.
// If someone refactors the builder and quietly drops one, Repository
// mtime silently degrades to zero — this test catches that.
func TestBuildRepoMetadataQuery_IncludesMetadata(t *testing.T) {
	t.Parallel()

	q, _ := buildRepoMetadataQuery([]*Repository{{Owner: "broady", Name: "ghfs"}})

	for _, want := range []string{
		"rateLimit { cost remaining resetAt limit nodeCount }",
		"pushedAt",
		"isArchived",
		"defaultBranchRef {",
		"... on Commit { committedDate oid }",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("query missing %q\n---query---\n%s", want, q)
		}
	}
}

// TestApplyGraphQLEntry_PopulatesMetadata verifies the full-happy-path
// repo-level fields land on the Repository struct. The pre-flight
// probe against real GitHub confirmed the server returns them in this
// shape for any non-empty repo.
func TestApplyGraphQLEntry_PopulatesMetadata(t *testing.T) {
	t.Parallel()

	pushed := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	committed := time.Date(2026, 4, 18, 9, 0, 0, 0, time.UTC)
	archived := false

	entry := &graphqlRepoResult{
		PushedAt:   &pushed,
		IsArchived: &archived,
		DefaultBranchRef: &graphqlBranchRef{
			Name: "main",
			Target: &graphqlBranchTarget{
				Typename:      "Commit",
				CommittedDate: &committed,
				OID:           "d35486eb",
			},
		},
	}

	r := &Repository{Owner: "broady", Name: "ghfs"}
	applyGraphQLEntry(r, entry)

	if !r.pushedAt.Equal(pushed) {
		t.Errorf("pushedAt = %v, want %v", r.pushedAt, pushed)
	}
	if !r.headCommitDate.Equal(committed) {
		t.Errorf("headCommitDate = %v, want %v", r.headCommitDate, committed)
	}
	if r.defaultBranch != "main" {
		t.Errorf("defaultBranch = %q, want %q", r.defaultBranch, "main")
	}
	if r.archived != false {
		t.Errorf("archived = %v, want false", r.archived)
	}
}

// TestApplyGraphQLEntry_EmptyRepoStillStampsPushedAt is the empty-repo
// case: defaultBranchRef is null (no commits ⇒ no head), but pushedAt
// is still present. The bug to prevent: a sparse response must still
// stamp pushedAt so every empty repo doesn't end up at epoch-zero mtime
// despite having a perfectly valid creation timestamp.
func TestApplyGraphQLEntry_EmptyRepoStillStampsPushedAt(t *testing.T) {
	t.Parallel()

	pushed := time.Date(2019, 10, 30, 16, 46, 6, 0, time.UTC)
	archived := false
	entry := &graphqlRepoResult{
		PushedAt:         &pushed,
		IsArchived:       &archived,
		DefaultBranchRef: nil, // empty repo: no commits, so no default branch
	}

	r := &Repository{Owner: "broady", Name: "some-old-empty"}
	applyGraphQLEntry(r, entry)

	// pushedAt landed despite the null defaultBranchRef.
	if !r.pushedAt.Equal(pushed) {
		t.Errorf("pushedAt = %v, want %v (must be applied independently of defaultBranchRef)", r.pushedAt, pushed)
	}
	// defaultBranch / headCommitDate stay zero because we got nothing
	// useful for them — don't fabricate a branch name.
	if r.defaultBranch != "" {
		t.Errorf("defaultBranch = %q, want empty (no defaultBranchRef)", r.defaultBranch)
	}
	if !r.headCommitDate.IsZero() {
		t.Errorf("headCommitDate = %v, want zero (no target commit)", r.headCommitDate)
	}
}

// TestApplyGraphQLEntry_NilFieldsDoNotClobber guards the nil-check
// discipline in applyGraphQLEntry. A partial response (say, the
// GraphQL server drops a field during an incident) must not overwrite
// a previously-known timestamp with the zero value — the worst
// outcome is a stale mtime, not a fake 1970 one.
func TestApplyGraphQLEntry_NilFieldsDoNotClobber(t *testing.T) {
	t.Parallel()

	existing := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := &Repository{
		Owner:          "broady",
		Name:           "ghfs",
		pushedAt:       existing,
		headCommitDate: existing,
		defaultBranch:  "main",
		archived:       true,
	}
	// Entry with every metadata field nil.
	entry := &graphqlRepoResult{}
	applyGraphQLEntry(r, entry)

	if !r.pushedAt.Equal(existing) {
		t.Errorf("pushedAt = %v, want %v (nil must not clobber)", r.pushedAt, existing)
	}
	if !r.headCommitDate.Equal(existing) {
		t.Errorf("headCommitDate = %v, want %v", r.headCommitDate, existing)
	}
	if r.defaultBranch != "main" {
		t.Errorf("defaultBranch = %q, want %q", r.defaultBranch, "main")
	}
	if !r.archived {
		t.Errorf("archived = false, want true preserved")
	}
}

// TestApplyGraphQLEntry_AnnotatedTagDefaultBranch covers the rare case
// of a default branch pointing at an annotated tag — Typename won't be
// "Commit" so committedDate is untrustworthy. We still capture the
// branch name for potential future use but leave headCommitDate zero.
func TestApplyGraphQLEntry_AnnotatedTagDefaultBranch(t *testing.T) {
	t.Parallel()

	tagDate := time.Date(2026, 4, 19, 0, 0, 0, 0, time.UTC)
	entry := &graphqlRepoResult{
		DefaultBranchRef: &graphqlBranchRef{
			Name: "weird-tag-branch",
			Target: &graphqlBranchTarget{
				Typename:      "Tag",
				CommittedDate: &tagDate, // would be nil in practice but be defensive
			},
		},
	}
	r := &Repository{Owner: "broady", Name: "weirdo"}
	applyGraphQLEntry(r, entry)

	if r.defaultBranch != "weird-tag-branch" {
		t.Errorf("defaultBranch = %q, want %q", r.defaultBranch, "weird-tag-branch")
	}
	if !r.headCommitDate.IsZero() {
		t.Errorf("headCommitDate = %v, want zero (target isn't a Commit)", r.headCommitDate)
	}
}

// TestBatchChunk_DecodesRateLimit verifies rateLimit is picked up from
// the same data map that carries the alias results, without interfering
// with alias iteration or getting mistaken for a repo.
//
// Not t.Parallel: shares graphqlEndpointForTest.
func TestBatchChunk_DecodesRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"data": {
				"rateLimit": {"cost":1,"remaining":4999,"resetAt":"2026-04-19T18:00:00Z","limit":5000,"nodeCount":0},
				"r0": {
					"pushedAt": "2026-04-19T03:16:17Z",
					"isArchived": false,
					"defaultBranchRef": {"name":"main","target":{"__typename":"Commit","committedDate":"2026-04-18T10:03:35Z","oid":"abc"}},
					"object": {"__typename":"Tree","entries":[]}
				}
			}
		}`)
	}))
	t.Cleanup(srv.Close)

	orig := graphqlEndpointForTest
	graphqlEndpointForTest = srv.URL
	t.Cleanup(func() { graphqlEndpointForTest = orig })

	u := &User{
		Login:  "broady",
		Client: github.NewClient(srv.Client()),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	repos := []*Repository{{Owner: "broady", Name: "ghfs"}}
	written, err := u.batchRepoMetadata(context.Background(), repos)
	if err != nil {
		t.Fatalf("batchRepoMetadata: %v", err)
	}
	if written != 1 {
		t.Errorf("written = %d, want 1 (rateLimit key must not be counted as an alias)", written)
	}
	// Metadata landed on r0.
	if repos[0].pushedAt.IsZero() {
		t.Error("pushedAt should be populated from the response")
	}
	if repos[0].defaultBranch != "main" {
		t.Errorf("defaultBranch = %q, want %q", repos[0].defaultBranch, "main")
	}
}

// TestBatchRepoMetadata_EndToEnd spins up an httptest server that plays
// the role of api.github.com/graphql, verifies the request shape,
// returns a canned response with one populated repo, one empty repo
// (defaultBranchRef null), and one permission-denied repo (null alias +
// errors[]), and confirms each Repository's metadata reflects the
// expected outcome.
//
// Not t.Parallel: mutates the package-level graphqlEndpointForTest
// seam, which the other http-backed tests in this file also touch.
// Pure-parsing tests stay parallel.
func TestBatchRepoMetadata_EndToEnd(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", req.Method)
		}
		if got := req.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		body, _ := io.ReadAll(req.Body)
		// Verify aliases r0/r1/r2 are in the query.
		for _, alias := range []string{"r0:", "r1:", "r2:"} {
			if !strings.Contains(string(body), alias) {
				t.Errorf("query missing alias %q; body=%s", alias, body)
			}
		}
		// Canned response: r0 fully populated, r1 empty (pushedAt set
		// but no defaultBranchRef — mirrors GitHub's shape for a repo
		// with zero commits), r2 has errors + null data (permission
		// denial).
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"data": {
				"r0": {
					"pushedAt": "2026-04-19T03:16:17Z",
					"isArchived": false,
					"defaultBranchRef": {"name":"main","target":{"__typename":"Commit","committedDate":"2026-04-18T10:03:35Z","oid":"abc"}}
				},
				"r1": {
					"pushedAt": "2026-04-01T00:00:00Z",
					"isArchived": false,
					"defaultBranchRef": null
				},
				"r2": null
			},
			"errors": [
				{"type":"FORBIDDEN","path":["r2"],"message":"resource not accessible"}
			]
		}`)
	}))
	t.Cleanup(srv.Close)

	// Pin the GraphQL endpoint to the test server for the duration.
	// (This package-level const is the simplest seam; tests run with
	// t.Parallel but only within this test file so races between
	// setters are not a real concern in practice. If we add more
	// GraphQL tests later we should plumb the endpoint through as a
	// per-User field.)
	origEndpoint := graphqlEndpointForTest
	graphqlEndpointForTest = srv.URL
	t.Cleanup(func() { graphqlEndpointForTest = origEndpoint })

	repos := []*Repository{
		{Owner: "broady", Name: "ghfs"},
		{Owner: "broady", Name: "empty-repo"},
		{Owner: "broady", Name: "private-denied"},
	}
	u := &User{
		Login:  "broady",
		Client: github.NewClient(srv.Client()),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	written, err := u.batchRepoMetadata(context.Background(), repos)
	if err != nil {
		t.Fatalf("batchRepoMetadata: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("requests = %d, want 1 (single batched POST)", got)
	}
	// r0 + r1 were applied; r2 was null so we skipped it entirely.
	if written != 2 {
		t.Errorf("written = %d, want 2", written)
	}

	// r0: full metadata landed on the struct.
	if repos[0].pushedAt.IsZero() {
		t.Error("ghfs pushedAt should be populated")
	}
	if repos[0].defaultBranch != "main" {
		t.Errorf("ghfs defaultBranch = %q, want %q", repos[0].defaultBranch, "main")
	}
	if repos[0].headCommitDate.IsZero() {
		t.Error("ghfs headCommitDate should be populated from Commit target")
	}

	// r1: pushedAt populated, but defaultBranchRef null → defaultBranch
	// and headCommitDate remain zero. applyGraphQLEntry's nil checks
	// keep us from clobbering a previously-set value with a zero.
	if repos[1].pushedAt.IsZero() {
		t.Error("empty-repo pushedAt should be populated")
	}
	if repos[1].defaultBranch != "" {
		t.Errorf("empty-repo defaultBranch = %q, want empty (no ref)", repos[1].defaultBranch)
	}

	// r2: null alias → metadata untouched. No assertion on specific
	// fields beyond "we didn't panic and didn't count it as written".
	if !repos[2].pushedAt.IsZero() {
		t.Error("private-denied pushedAt should NOT be touched when data=null")
	}
}

// TestBatchRepoMetadata_HTTPErrorPropagates verifies that a 5xx or
// network-level failure surfaces as an error so prefetchRepoContents
// can fall back to the REST path rather than silently losing the
// warmup.
//
// Not t.Parallel: shares the graphqlEndpointForTest seam with
// TestBatchRepoMetadata_EndToEnd and TestPostGraphQL_DecodeError.
func TestBatchRepoMetadata_HTTPErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `{"message":"upstream down"}`)
	}))
	t.Cleanup(srv.Close)

	orig := graphqlEndpointForTest
	graphqlEndpointForTest = srv.URL
	t.Cleanup(func() { graphqlEndpointForTest = orig })

	u := &User{
		Login:  "broady",
		Client: github.NewClient(srv.Client()),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	repos := []*Repository{{Owner: "broady", Name: "ghfs"}}
	written, err := u.batchRepoMetadata(context.Background(), repos)
	if err == nil {
		t.Fatal("expected error from 502 response, got nil")
	}
	if written != 0 {
		t.Errorf("written = %d, want 0 on hard error", written)
	}
	if !strings.Contains(err.Error(), "502") {
		t.Errorf("error should mention HTTP 502, got: %v", err)
	}
}

// TestPostGraphQL_DecodeError guards against a future regression where
// we accept a 200 with a body that isn't JSON (e.g. an HTML error page
// from an intermediary). The caller should see a decode error, not
// silently think the batch succeeded.
//
// Not t.Parallel: mutates graphqlEndpointForTest.
func TestPostGraphQL_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, `<html>cloudflare error</html>`)
	}))
	t.Cleanup(srv.Close)

	orig := graphqlEndpointForTest
	graphqlEndpointForTest = srv.URL
	t.Cleanup(func() { graphqlEndpointForTest = orig })

	body, err := postGraphQL(context.Background(), srv.Client(), "{ __typename }", nil)
	if err != nil {
		t.Fatalf("postGraphQL returned transport error: %v", err)
	}
	var resp graphqlResponse
	if err := json.Unmarshal(body, &resp); err == nil {
		t.Error("expected JSON decode error for HTML body, got nil")
	} else if !errors.As(err, new(*json.SyntaxError)) {
		t.Errorf("unexpected error type: %v", err)
	}
}

// TestPostGraphQL_RejectsOversizedResponse: a hostile / buggy upstream
// must not be able to OOM ghfs by streaming an unbounded body. We
// simulate a response just over the cap and verify postGraphQL bails
// with a size error before the caller tries to json.Unmarshal the
// whole thing.
//
// Not t.Parallel: mutates graphqlEndpointForTest.
func TestPostGraphQL_RejectsOversizedResponse(t *testing.T) {
	// Respond with maxGraphQLResponseBytes+1024 bytes of valid JSON
	// filler so the only thing that can reject this is the size cap.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// `{"data":{"filler":"AAA...AAA"}}` shape, padded to exceed cap.
		const header = `{"data":{"filler":"`
		const footer = `"}}`
		padLen := maxGraphQLResponseBytes + 1024 - len(header) - len(footer)
		pad := strings.Repeat("A", padLen)
		_, _ = io.WriteString(w, header+pad+footer)
	}))
	t.Cleanup(srv.Close)

	orig := graphqlEndpointForTest
	graphqlEndpointForTest = srv.URL
	t.Cleanup(func() { graphqlEndpointForTest = orig })

	_, err := postGraphQL(context.Background(), srv.Client(), "{ __typename }", nil)
	if err == nil {
		t.Fatal("expected size-cap error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error should mention size cap, got: %v", err)
	}
}
