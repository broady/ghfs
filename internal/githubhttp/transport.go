// Package githubhttp builds the layered HTTP transport chain used
// for all GitHub REST API calls: auth → httpcache (ETag + two-tier
// LRU/disk store) → revalidation suppressor.
//
// The goal is to make repeated per-repo Contents API probes (e.g. by
// eza --icons listing ~/github.com/<owner>/) effectively free: once
// warm, each request is served from the in-process LRU without any
// network I/O, for SuppressDuration after the last hit.
//
// Chain shape (outermost first):
//
//	Suppressor               ← 5-min window: Cache-Control: only-if-cached, zero network
//	 └─ httpcache.Transport  ← RFC-7234 ETag cache, X-From-Cache: 1 on hit
//	     └─ AuthTransport    ← token injection + header normalisation
//	         └─ http.DefaultTransport
package githubhttp

import (
	"container/list"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/die-net/lrucache"
	"github.com/die-net/lrucache/twotier"
	"github.com/gregjones/httpcache"
	"github.com/gregjones/httpcache/diskcache"
)

// Default cache sizing + suppression window.
const (
	DefaultMemCacheMB    = 128
	DefaultSuppressDur   = 5 * time.Minute
	DefaultMaxCachedURLs = 1000
	cacheEntryMaxAgeSec  = 24 * 60 * 60 // lrucache entry TTL
)

// Config configures New.
type Config struct {
	// Token is the GitHub PAT to attach as `Authorization: token <T>`.
	// Empty => anonymous.
	Token string

	// CacheDir is the disk-cache root (e.g. ~/.cache/ghfs). Empty
	// disables the disk tier (in-memory LRU only). Disabled wins
	// over both.
	CacheDir string

	// MemCacheMB is the in-memory LRU budget in MiB. 0 => default.
	MemCacheMB int

	// SuppressDuration is how long a URL stays "just served from
	// cache, don't even revalidate". 0 => default.
	SuppressDuration time.Duration

	// Disabled skips the entire cache chain (just AuthTransport).
	Disabled bool

	// Logger receives HTTP request traces (at debug) and cache
	// clear notifications (at info). Must be non-nil.
	Logger *slog.Logger
}

// Client is the assembled HTTP client plus the runtime handle the
// SIGUSR1 handler needs to force-refresh cached data.
type Client struct {
	HTTP *http.Client

	// Suppressor is nil when Disabled. Its ClearSuppressionCache
	// method makes the next request for any URL fall through to the
	// httpcache revalidation path instead of the "only-if-cached"
	// shortcut.
	Suppressor *Suppressor
}

// New assembles the chain described in the package doc.
func New(cfg Config) (*Client, error) {
	if cfg.Logger == nil {
		return nil, fmt.Errorf("githubhttp: Logger is required")
	}

	auth := &AuthTransport{
		Token:  cfg.Token,
		Base:   http.DefaultTransport,
		Logger: cfg.Logger,
	}

	if cfg.Disabled {
		return &Client{HTTP: &http.Client{Transport: auth}}, nil
	}

	memMB := cfg.MemCacheMB
	if memMB <= 0 {
		memMB = DefaultMemCacheMB
	}
	mem := lrucache.New(int64(memMB)*1024*1024, cacheEntryMaxAgeSec)

	var cache httpcache.Cache = mem
	if cfg.CacheDir != "" {
		if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
			return nil, fmt.Errorf("githubhttp: mkdir cache dir: %w", err)
		}
		cache = twotier.New(mem, diskcache.New(cfg.CacheDir))
	}

	cacheT := &httpcache.Transport{
		Cache:               cache,
		MarkCachedResponses: true,
		Transport:           auth,
	}

	window := cfg.SuppressDuration
	if window <= 0 {
		window = DefaultSuppressDur
	}

	suppressor := &Suppressor{
		Base:             cacheT,
		SuppressDuration: window,
		Logger:           cfg.Logger,
	}

	return &Client{
		HTTP:       &http.Client{Transport: suppressor},
		Suppressor: suppressor,
	}, nil
}

// AuthTransport attaches an auth token + normalises response headers
// so the downstream httpcache layer can actually cache things.
//
// Normalisations (all required by gregjones/httpcache):
//   - Date header set/refreshed on 304s and any response missing it.
//   - Vary and X-Varied-* stripped (GitHub returns Vary: Accept,
//     Accept-Encoding which would otherwise partition the cache and
//     defeat it for nearly every request).
//   - Synthetic ETag injected on 404s so "this doesn't exist" is
//     cacheable (avoids re-probing missing paths).
type AuthTransport struct {
	Token  string
	Base   http.RoundTripper
	Logger *slog.Logger
}

// RoundTrip implements http.RoundTripper.
func (t *AuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// RoundTripper contract forbids mutating the caller's request
	// (net/http docs: "RoundTrip should not modify the request"). The
	// Suppressor below already follows this discipline — copy the
	// pattern here so a retried request isn't already tagged with
	// someone else's Authorization header.
	if t.Token != "" {
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "token "+t.Token)
	}

	resp, err := t.Base.RoundTrip(req)
	if resp == nil || err != nil {
		if t.Logger != nil && err != nil {
			t.Logger.Debug("http.request", "method", req.Method, "url", req.URL.Path, "error", err.Error())
		}
		return resp, err
	}

	// Refresh Date on 304s so httpcache computes freshness from now,
	// not from whatever GitHub cached last.
	if resp.StatusCode == http.StatusNotModified {
		resp.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}
	if resp.Header.Get("Date") == "" {
		resp.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}

	// Kill Vary/X-Varied-* so httpcache uses the URL as the cache key
	// alone rather than partitioning per Accept header.
	resp.Header.Del("Vary")
	for key := range resp.Header {
		if strings.HasPrefix(key, "X-Varied-") {
			resp.Header.Del(key)
		}
	}

	// 404s need an ETag to be cacheable. Synthesise one from the URL;
	// stable across retries for the same missing path.
	if resp.StatusCode == http.StatusNotFound && resp.Header.Get("ETag") == "" {
		resp.Header.Set("ETag", "\"404-"+req.URL.String()+"\"")
	}

	if t.Logger != nil {
		status := "miss"
		if resp.Header.Get("X-From-Cache") == "1" {
			status = "hit"
		} else if resp.StatusCode == http.StatusNotModified {
			status = "revalidated"
		}
		t.Logger.Debug("http.request",
			"method", req.Method,
			"url", req.URL.Path,
			"status", resp.StatusCode,
			"cache", status,
		)
	}
	return resp, err
}

// Suppressor sits above httpcache. For SuppressDuration after a cache
// hit on a URL, it replays the request with Cache-Control:
// only-if-cached — httpcache then serves from storage without ever
// touching the network (not even an If-None-Match revalidation).
//
// Without this, GitHub will 304 every request with ~30ms RTT, which
// multiplied by N repos becomes visible to eza --icons.
type Suppressor struct {
	Base             http.RoundTripper
	SuppressDuration time.Duration
	Logger           *slog.Logger
	MaxEntries       int // 0 => DefaultMaxCachedURLs

	mu    sync.Mutex
	cache map[string]*list.Element // URL → *suppressorEntry in lru
	lru   *list.List               // MRU at Front, LRU at Back
}

type suppressorEntry struct {
	url       string
	timestamp time.Time
}

// ClearSuppressionCache forces the next request for every URL to go
// through revalidation (304 or full response) rather than the
// only-if-cached shortcut. SIGUSR1 should call this so users can
// see fresh data without unmounting.
func (r *Suppressor) ClearSuppressionCache() {
	r.mu.Lock()
	r.cache = make(map[string]*list.Element)
	r.lru = list.New()
	r.mu.Unlock()
	if r.Logger != nil {
		r.Logger.Info("revalidation suppression cache cleared - next requests will revalidate")
	}
}

// RoundTrip implements http.RoundTripper.
func (r *Suppressor) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return r.Base.RoundTrip(req)
	}

	cacheKey := req.URL.String()

	// Fast path: within the suppression window — serve from cache
	// without revalidation. Decide under the lock, act after.
	var suppress bool
	r.mu.Lock()
	if elem, ok := r.cache[cacheKey]; ok {
		entry := elem.Value.(*suppressorEntry)
		if time.Since(entry.timestamp) < r.SuppressDuration {
			r.lru.MoveToFront(elem)
			suppress = true
		} else {
			delete(r.cache, cacheKey)
			r.lru.Remove(elem)
		}
	}
	r.mu.Unlock()

	if suppress {
		// Clone so we don't mutate the caller's headers.
		reqCopy := req.Clone(req.Context())
		reqCopy.Header.Set("Cache-Control", "only-if-cached")
		resp, err := r.Base.RoundTrip(reqCopy)
		// 504 from only-if-cached means "not in cache" — fall
		// through to a normal request so we don't hand the caller a
		// bogus error. Anything else is the real answer.
		if err != nil || resp == nil || resp.StatusCode != http.StatusGatewayTimeout {
			return resp, err
		}
		if resp != nil {
			resp.Body.Close()
		}
		// fall through to the normal path below
	}

	// Normal path: full request (which httpcache may turn into a
	// revalidation).
	resp, err := r.Base.RoundTrip(req)

	// Record the hit if httpcache served from cache (X-From-Cache: 1
	// is set on both straight-from-cache responses AND 304-turned-200
	// responses).
	if err == nil && resp != nil && resp.Header.Get("X-From-Cache") == "1" {
		r.recordHit(cacheKey)
	}
	return resp, err
}

func (r *Suppressor) recordHit(cacheKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cache == nil {
		r.cache = make(map[string]*list.Element)
		r.lru = list.New()
	}

	maxEntries := r.MaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultMaxCachedURLs
	}

	if elem, ok := r.cache[cacheKey]; ok {
		elem.Value.(*suppressorEntry).timestamp = time.Now()
		r.lru.MoveToFront(elem)
		return
	}

	entry := &suppressorEntry{url: cacheKey, timestamp: time.Now()}
	elem := r.lru.PushFront(entry)
	r.cache[cacheKey] = elem

	for r.lru.Len() > maxEntries {
		oldest := r.lru.Back()
		if oldest == nil {
			break
		}
		delete(r.cache, oldest.Value.(*suppressorEntry).url)
		r.lru.Remove(oldest)
	}
}
