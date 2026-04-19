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
	"syscall"
	"testing"

	"github.com/google/go-github/v60/github"
)

// TestBuildRootContentsQuery_AliasesAndEscaping checks that every repo
// gets a distinct alias, that owner/name are JSON-encoded (defensive
// against a future widening of GitHub's naming rules), and that the
// query includes the Tree-entry selection set cachedRootContents
// consumers expect (name, type, mode, oid, blob byteSize).
func TestBuildRootContentsQuery_AliasesAndEscaping(t *testing.T) {
	t.Parallel()

	repos := []*Repository{
		{Owner: "broady", Name: "ghfs"},
		{Owner: "broady", Name: "with\"quote"}, // would never happen in practice, but we should handle it
	}
	q, aliases := buildRootContentsQuery(repos)

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
		`object(expression:"HEAD:")`,
		"... on Tree",
		"entries {",
		"name",
		"type",
		"mode",
		"oid",
		"... on Blob { byteSize }",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("query missing %q\n---query---\n%s", want, q)
		}
	}
}

// TestContentTypeFromTreeEntry verifies the TreeEntry → REST type
// mapping is exhaustive. Submodules must be dropped (upstream
// newInodeFromContent already skips `submodule`, but we drop earlier
// to keep apiRootItems small).
func TestContentTypeFromTreeEntry(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   graphqlTreeEntry
		want string
	}{
		{"dir", graphqlTreeEntry{Type: "tree"}, "dir"},
		{"file", graphqlTreeEntry{Type: "blob", Mode: 0o100644}, "file"},
		{"exec-file", graphqlTreeEntry{Type: "blob", Mode: 0o100755}, "file"},
		{"symlink", graphqlTreeEntry{Type: "blob", Mode: 0o120000}, "symlink"},
		{"submodule-dropped", graphqlTreeEntry{Type: "commit"}, ""},
		{"unknown-dropped", graphqlTreeEntry{Type: "???"}, ""},
	}
	for _, c := range cases {
		got := contentTypeFromTreeEntry(c.in)
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// TestApplyGraphQLEntry_PopulatesCache verifies applyGraphQLEntry writes
// an identically-shaped cache to what cachedRootContents would after a
// REST call — name/type/path/sha/size all round-trip.
func TestApplyGraphQLEntry_PopulatesCache(t *testing.T) {
	t.Parallel()

	var bs int64 = 4096
	entry := &graphqlRepoResult{
		Object: &graphqlObject{
			Typename: "Tree",
			Entries: []graphqlTreeEntry{
				{Name: "README.md", Type: "blob", Mode: 0o100644, OID: "abc123",
					Object: &graphqlEntryObject{Typename: "Blob", ByteSize: &bs}},
				{Name: "core", Type: "tree", Mode: 0o040000, OID: "deadbeef",
					Object: &graphqlEntryObject{Typename: "Tree"}},
				{Name: "link", Type: "blob", Mode: 0o120000, OID: "cafef00d",
					Object: &graphqlEntryObject{Typename: "Blob"}},
				{Name: "vendor-mod", Type: "commit", Mode: 0o160000, OID: "1234"}, // dropped
			},
		},
	}

	r := &Repository{Owner: "broady", Name: "ghfs"}
	applyGraphQLEntry(r, entry)

	if r.apiRootErrno != 0 {
		t.Fatalf("apiRootErrno = %v, want 0", r.apiRootErrno)
	}
	if r.apiRootCachedAt.IsZero() {
		t.Error("apiRootCachedAt should be set")
	}
	if len(r.apiRootItems) != 3 {
		t.Fatalf("items = %d, want 3 (submodule must be dropped)", len(r.apiRootItems))
	}

	// Spot-check each entry's shape.
	want := []struct{ name, typ string }{
		{"README.md", "file"},
		{"core", "dir"},
		{"link", "symlink"},
	}
	for i, w := range want {
		item := r.apiRootItems[i]
		if item.Name == nil || *item.Name != w.name {
			t.Errorf("item[%d].Name = %v, want %q", i, item.Name, w.name)
		}
		if item.Path == nil || *item.Path != w.name {
			t.Errorf("item[%d].Path = %v, want %q (path==name for root)", i, item.Path, w.name)
		}
		if item.Type == nil || *item.Type != w.typ {
			t.Errorf("item[%d].Type = %v, want %q", i, item.Type, w.typ)
		}
	}
	// byteSize round-trips to Size (*int, not *int64).
	if r.apiRootItems[0].Size == nil || *r.apiRootItems[0].Size != 4096 {
		t.Errorf("README.md Size = %v, want 4096", r.apiRootItems[0].Size)
	}
	// SHA also populated, for lazy Open against the hydrator.
	if r.apiRootItems[0].SHA == nil || *r.apiRootItems[0].SHA != "abc123" {
		t.Errorf("README.md SHA = %v, want abc123", r.apiRootItems[0].SHA)
	}
}

// TestApplyGraphQLEntry_NullObjectENOENT verifies that a repo with no
// HEAD (e.g. no commits yet) gets negative-cached as ENOENT — matching
// the REST path's treatment of /contents/ returning 404.
func TestApplyGraphQLEntry_NullObjectENOENT(t *testing.T) {
	t.Parallel()

	r := &Repository{Owner: "broady", Name: "brand-new-empty"}
	applyGraphQLEntry(r, &graphqlRepoResult{Object: nil})

	if r.apiRootErrno != syscall.ENOENT {
		t.Errorf("apiRootErrno = %v, want ENOENT", r.apiRootErrno)
	}
	if r.apiRootItems != nil {
		t.Errorf("apiRootItems = %v, want nil", r.apiRootItems)
	}
	if r.apiRootCachedAt.IsZero() {
		t.Error("apiRootCachedAt should be set so we don't re-probe every ls")
	}
}

// TestBatchRepoRootContents_EndToEnd spins up an httptest server that
// plays the role of api.github.com/graphql, verifies the request shape,
// returns a canned response with two repos + one empty, and confirms
// each Repository's cache reflects the expected outcome.
//
// Not t.Parallel: mutates the package-level graphqlEndpointForTest
// seam, which the other http-backed tests in this file also touch.
// Pure-parsing tests stay parallel.
func TestBatchRepoRootContents_EndToEnd(t *testing.T) {
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
		// Canned response: r0 populated (ghfs), r1 empty (no HEAD), r2
		// has errors + null data (simulating a permission denial).
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"data": {
				"r0": {"object": {"__typename":"Tree","entries":[
					{"name":"README.md","type":"blob","mode":33188,"oid":"a1","object":{"__typename":"Blob","byteSize":123}},
					{"name":"src","type":"tree","mode":16384,"oid":"b2","object":{"__typename":"Tree"}}
				]}},
				"r1": {"object": null},
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
	written, err := u.batchRepoRootContents(context.Background(), repos)
	if err != nil {
		t.Fatalf("batchRepoRootContents: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("requests = %d, want 1 (single batched POST)", got)
	}
	// r0 + r1 wrote caches; r2 was null so we left its cache alone.
	if written != 2 {
		t.Errorf("written = %d, want 2", written)
	}

	// r0: populated.
	if repos[0].apiRootErrno != 0 {
		t.Errorf("ghfs errno = %v, want 0", repos[0].apiRootErrno)
	}
	if len(repos[0].apiRootItems) != 2 {
		t.Errorf("ghfs items = %d, want 2", len(repos[0].apiRootItems))
	}

	// r1: empty repo → ENOENT cached.
	if repos[1].apiRootErrno != syscall.ENOENT {
		t.Errorf("empty-repo errno = %v, want ENOENT", repos[1].apiRootErrno)
	}
	if repos[1].apiRootCachedAt.IsZero() {
		t.Error("empty-repo cachedAt should be set (negative-cache)")
	}

	// r2: null data → cache untouched, next Readdir falls through to REST.
	if !repos[2].apiRootCachedAt.IsZero() {
		t.Error("private-denied cache should NOT be touched when data=null + errors[]")
	}
}

// TestBatchRepoRootContents_HTTPErrorPropagates verifies that a 5xx or
// network-level failure surfaces as an error so prefetchRepoContents
// can fall back to the REST path rather than silently losing the
// warmup.
//
// Not t.Parallel: shares the graphqlEndpointForTest seam with
// TestBatchRepoRootContents_EndToEnd and TestPostGraphQL_DecodeError.
func TestBatchRepoRootContents_HTTPErrorPropagates(t *testing.T) {
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
	written, err := u.batchRepoRootContents(context.Background(), repos)
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
