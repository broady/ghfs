package githubhttp

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockRT is a test RoundTripper that returns a canned response and
// counts invocations. The response factory is called per request so
// each caller gets a fresh Body.
type mockRT struct {
	count atomic.Int64
	resp  func() *http.Response
	mu    sync.Mutex // serialize header mutation across concurrent calls
}

func (m *mockRT) RoundTrip(req *http.Request) (*http.Response, error) {
	m.count.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resp(), nil
}

func newResp(status int, hdr http.Header, body string) *http.Response {
	if hdr == nil {
		hdr = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Header:     hdr.Clone(),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// TestAuthTransport_404_ETagInjected covers the 404-caching path:
// gregjones/httpcache won't cache a response without an ETag, so
// AuthTransport synthesises one from the URL.
func TestAuthTransport_404_ETagInjected(t *testing.T) {
	t.Parallel()
	m := &mockRT{resp: func() *http.Response { return newResp(404, nil, "Not Found") }}
	at := &AuthTransport{Base: m}

	req, _ := http.NewRequest("GET", "https://api.github.com/repos/owner/repo/contents/missing", nil)
	resp, err := at.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if got := resp.Header.Get("ETag"); got == "" {
		t.Error("expected ETag injected on 404, got none")
	}
	if resp.StatusCode != 404 {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestAuthTransport_PreservesCacheControl ensures we don't clobber
// GitHub's own Cache-Control values.
func TestAuthTransport_PreservesCacheControl(t *testing.T) {
	t.Parallel()
	hdr := http.Header{"Cache-Control": []string{"public, max-age=60"}}
	m := &mockRT{resp: func() *http.Response { return newResp(200, hdr, "OK") }}
	at := &AuthTransport{Base: m}

	req, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	resp, _ := at.RoundTrip(req)

	if got := resp.Header.Get("Cache-Control"); got != "public, max-age=60" {
		t.Errorf("Cache-Control: got %q, want preserved", got)
	}
}

// TestAuthTransport_StripsVary covers the bit that actually makes
// caching work — GitHub returns Vary: Accept which would otherwise
// partition the cache per Accept header.
func TestAuthTransport_StripsVary(t *testing.T) {
	t.Parallel()
	hdr := http.Header{
		"Vary":            []string{"Accept, Accept-Encoding"},
		"X-Varied-Accept": []string{"application/json"},
	}
	m := &mockRT{resp: func() *http.Response { return newResp(200, hdr, "OK") }}
	at := &AuthTransport{Base: m}

	req, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	resp, _ := at.RoundTrip(req)

	if resp.Header.Get("Vary") != "" {
		t.Errorf("Vary not stripped: %q", resp.Header.Get("Vary"))
	}
	if resp.Header.Get("X-Varied-Accept") != "" {
		t.Error("X-Varied-* not stripped")
	}
}

// TestAuthTransport_TokenAttached verifies the auth header is set.
func TestAuthTransport_TokenAttached(t *testing.T) {
	t.Parallel()
	var seen string
	m := &authObservingRT{seen: &seen, resp: func() *http.Response { return newResp(200, nil, "") }}
	at := &AuthTransport{Token: "abc123", Base: m}

	req, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	if _, err := at.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if seen != "token abc123" {
		t.Errorf("Authorization: got %q, want %q", seen, "token abc123")
	}
}

type authObservingRT struct {
	seen *string
	resp func() *http.Response
}

func (a *authObservingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	*a.seen = req.Header.Get("Authorization")
	return a.resp(), nil
}

// TestAuthTransport_DoesNotMutateCallerRequest verifies the
// RoundTripper contract (net/http docs: "RoundTrip should not modify
// the request"). A caller reusing the same *http.Request across
// retries must not see an Authorization header left over from a
// previous round-trip — especially if the transport's token is later
// rotated or cleared.
func TestAuthTransport_DoesNotMutateCallerRequest(t *testing.T) {
	t.Parallel()
	m := &mockRT{resp: func() *http.Response { return newResp(200, nil, "") }}
	at := &AuthTransport{Token: "abc123", Base: m}

	req, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	if before := req.Header.Get("Authorization"); before != "" {
		t.Fatalf("precondition: Authorization should be empty, got %q", before)
	}
	if _, err := at.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if got := req.Header.Get("Authorization"); got != "" {
		t.Errorf("caller's req.Header mutated: Authorization=%q, want empty", got)
	}
}

// TestSuppressor_HitWithinWindow_UsesOnlyIfCached verifies the core
// behaviour: once we've observed a cache hit, the next request
// inside the suppression window is rewritten with
// Cache-Control: only-if-cached (so httpcache skips revalidation).
func TestSuppressor_HitWithinWindow_UsesOnlyIfCached(t *testing.T) {
	t.Parallel()

	// Upstream: first call returns X-From-Cache: 1 (prompting
	// Suppressor to record the hit); subsequent calls capture the
	// request headers so we can verify only-if-cached was added.
	var lastCC string
	var callCount atomic.Int64
	firstHitHeaders := http.Header{"X-From-Cache": []string{"1"}}
	upstream := &captureRT{
		seenCacheControl: &lastCC,
		seenCount:        &callCount,
		respFn: func(n int64) *http.Response {
			// Mark every response as from-cache so recordHit fires.
			return newResp(200, firstHitHeaders, "")
		},
	}

	s := &Suppressor{
		Base:             upstream,
		SuppressDuration: time.Minute,
		Logger:           slog.Default(),
	}

	// Prime the cache entry.
	req1, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	if _, err := s.RoundTrip(req1); err != nil {
		t.Fatalf("first RoundTrip: %v", err)
	}
	if lastCC != "" {
		t.Errorf("first request should not carry Cache-Control, got %q", lastCC)
	}

	// Second request should be suppressed → only-if-cached.
	req2, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	if _, err := s.RoundTrip(req2); err != nil {
		t.Fatalf("second RoundTrip: %v", err)
	}
	if lastCC != "only-if-cached" {
		t.Errorf("expected only-if-cached on second request, got %q", lastCC)
	}
}

// TestSuppressor_WindowExpiry: after SuppressDuration, the URL
// falls out and the next request is a normal revalidation.
func TestSuppressor_WindowExpiry(t *testing.T) {
	t.Parallel()

	var lastCC string
	var callCount atomic.Int64
	upstream := &captureRT{
		seenCacheControl: &lastCC,
		seenCount:        &callCount,
		respFn: func(int64) *http.Response {
			return newResp(200, http.Header{"X-From-Cache": []string{"1"}}, "")
		},
	}

	s := &Suppressor{
		Base:             upstream,
		SuppressDuration: 10 * time.Millisecond,
		Logger:           slog.Default(),
	}

	req, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	if _, err := s.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	// Confirm suppression path would fire now.
	req, _ = http.NewRequest("GET", "https://api.github.com/x", nil)
	if _, err := s.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if lastCC != "only-if-cached" {
		t.Fatalf("expected only-if-cached, got %q", lastCC)
	}
	time.Sleep(25 * time.Millisecond)
	// After expiry, Cache-Control should be blank again.
	lastCC = ""
	req, _ = http.NewRequest("GET", "https://api.github.com/x", nil)
	if _, err := s.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if lastCC != "" {
		t.Errorf("after window expiry, Cache-Control should be empty, got %q", lastCC)
	}
}

// TestSuppressor_ClearForgetsEverything: SIGUSR1 → ClearSuppressionCache
// should make the next request a full revalidation.
func TestSuppressor_ClearForgetsEverything(t *testing.T) {
	t.Parallel()

	var lastCC string
	var callCount atomic.Int64
	upstream := &captureRT{
		seenCacheControl: &lastCC,
		seenCount:        &callCount,
		respFn: func(int64) *http.Response {
			return newResp(200, http.Header{"X-From-Cache": []string{"1"}}, "")
		},
	}

	s := &Suppressor{
		Base:             upstream,
		SuppressDuration: time.Minute,
		Logger:           slog.Default(),
	}

	req, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	_, _ = s.RoundTrip(req)
	req, _ = http.NewRequest("GET", "https://api.github.com/x", nil)
	_, _ = s.RoundTrip(req)
	if lastCC != "only-if-cached" {
		t.Fatalf("pre-clear: want suppressed, got %q", lastCC)
	}

	s.ClearSuppressionCache()

	lastCC = ""
	req, _ = http.NewRequest("GET", "https://api.github.com/x", nil)
	_, _ = s.RoundTrip(req)
	if lastCC != "" {
		t.Errorf("after Clear, should be normal revalidation (no CC), got %q", lastCC)
	}
}

// TestSuppressor_GatewayTimeoutFallsThrough: only-if-cached path
// yielding 504 (not in cache) must fall through to a normal request,
// not return the 504 to the caller.
func TestSuppressor_GatewayTimeoutFallsThrough(t *testing.T) {
	t.Parallel()

	var ccSeenPerCall []string
	var mu sync.Mutex
	respondFn := func(req *http.Request) *http.Response {
		mu.Lock()
		ccSeenPerCall = append(ccSeenPerCall, req.Header.Get("Cache-Control"))
		idx := len(ccSeenPerCall)
		mu.Unlock()
		switch {
		case idx == 1:
			// First call: prime hit.
			return newResp(200, http.Header{"X-From-Cache": []string{"1"}}, "")
		case idx == 2:
			// only-if-cached replay: pretend cache was evicted.
			return newResp(504, nil, "")
		default:
			// Fall-through: normal 200 that is also a cache hit.
			return newResp(200, http.Header{"X-From-Cache": []string{"1"}}, "")
		}
	}
	upstream := &funcRT{fn: respondFn}

	s := &Suppressor{Base: upstream, SuppressDuration: time.Minute, Logger: slog.Default()}
	req, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	_, _ = s.RoundTrip(req)

	req, _ = http.NewRequest("GET", "https://api.github.com/x", nil)
	resp, err := s.RoundTrip(req)
	if err != nil {
		t.Fatalf("second RoundTrip: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected fallthrough to 200 after 504, got %d", resp.StatusCode)
	}
	mu.Lock()
	got := append([]string(nil), ccSeenPerCall...)
	mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("expected 3 upstream calls (prime, only-if-cached, fallthrough), got %d: %v", len(got), got)
	}
	if got[1] != "only-if-cached" {
		t.Errorf("second call should be only-if-cached, got %q", got[1])
	}
	if got[2] != "" {
		t.Errorf("third call should be a plain revalidation (no CC), got %q", got[2])
	}
}

// TestSuppressor_NonGET_Passthrough: POST/PUT/etc should never be
// suppressed.
func TestSuppressor_NonGET_Passthrough(t *testing.T) {
	t.Parallel()
	var lastCC string
	var n atomic.Int64
	upstream := &captureRT{
		seenCacheControl: &lastCC,
		seenCount:        &n,
		respFn: func(int64) *http.Response {
			return newResp(200, http.Header{"X-From-Cache": []string{"1"}}, "")
		},
	}
	s := &Suppressor{Base: upstream, SuppressDuration: time.Minute, Logger: slog.Default()}

	// Prime with a GET so the URL is in the LRU.
	req, _ := http.NewRequest("GET", "https://api.github.com/x", nil)
	_, _ = s.RoundTrip(req)
	// POST to same URL: should pass through, no Cache-Control rewrite.
	req, _ = http.NewRequest("POST", "https://api.github.com/x", strings.NewReader("{}"))
	lastCC = ""
	_, _ = s.RoundTrip(req)
	if lastCC != "" {
		t.Errorf("POST should bypass suppressor, got CC=%q", lastCC)
	}
}

// captureRT is a RoundTripper that records Cache-Control and call
// counts, then emits whatever its respFn produces.
type captureRT struct {
	seenCacheControl *string
	seenCount        *atomic.Int64
	respFn           func(n int64) *http.Response
	mu               sync.Mutex
}

func (c *captureRT) RoundTrip(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.seenCount.Add(1)
	*c.seenCacheControl = req.Header.Get("Cache-Control")
	return c.respFn(n), nil
}

// funcRT lets a test compute a response per-request without juggling
// captureRT state.
type funcRT struct {
	fn func(*http.Request) *http.Response
	mu sync.Mutex
}

func (f *funcRT) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fn(req), nil
}
