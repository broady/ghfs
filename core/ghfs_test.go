package core

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// MockTransport is a mock http.RoundTripper for testing
type MockTransport struct {
	requestCount int
	response     *http.Response
}

func (m *MockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	m.requestCount++
	return m.response, nil
}

func TestCacheHTTPTransport404Caching(t *testing.T) {
	// Create a mock 404 response without Cache-Control headers
	notFoundResp := &http.Response{
		StatusCode: http.StatusNotFound,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("Not Found")),
	}

	mockTransport := &MockTransport{response: notFoundResp}
	cacheTransport := &GitHubHTTPTransport{Base: mockTransport}

	// Make a request
	req, _ := http.NewRequest("GET", "https://api.github.com/repos/owner/repo/contents/nonexistent", nil)
	resp, _ := cacheTransport.RoundTrip(req)

	// Verify that Cache-Control header was added
	cacheControl := resp.Header.Get("Cache-Control")
	if cacheControl == "" {
		t.Error("Expected Cache-Control header to be set for 404 response")
	}

	// Verify that ETag was added
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Error("Expected ETag header to be set for 404 response")
	}

	// Verify status code is still 404
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected StatusNotFound (404), got %d", resp.StatusCode)
	}
}

func TestCacheHTTPTransport200NoModification(t *testing.T) {
	// Create a mock 200 response with existing cache headers
	successResp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Cache-Control": []string{"public, max-age=60"}},
		Body:       io.NopCloser(strings.NewReader("OK")),
	}

	mockTransport := &MockTransport{response: successResp}
	cacheTransport := &GitHubHTTPTransport{Base: mockTransport}

	req, _ := http.NewRequest("GET", "https://api.github.com/repos/owner/repo/contents/file", nil)
	resp, _ := cacheTransport.RoundTrip(req)

	// Verify that existing Cache-Control header is not modified
	cacheControl := resp.Header.Get("Cache-Control")
	if cacheControl != "public, max-age=60" {
		t.Errorf("Expected existing Cache-Control to be preserved, got %q", cacheControl)
	}
}

func TestCacheHTTPTransport404WithExistingCacheControl(t *testing.T) {
	// Create a mock 404 response with existing Cache-Control headers
	notFoundResp := &http.Response{
		StatusCode: http.StatusNotFound,
		Header:     http.Header{"Cache-Control": []string{"private, max-age=60"}},
		Body:       io.NopCloser(strings.NewReader("Not Found")),
	}

	mockTransport := &MockTransport{response: notFoundResp}
	cacheTransport := &GitHubHTTPTransport{Base: mockTransport}

	req, _ := http.NewRequest("GET", "https://api.github.com/repos/owner/repo/contents/nonexistent", nil)
	resp, _ := cacheTransport.RoundTrip(req)

	// Verify that existing Cache-Control header is preserved
	cacheControl := resp.Header.Get("Cache-Control")
	if cacheControl != "private, max-age=60" {
		t.Errorf("Expected existing Cache-Control to be preserved, got %q", cacheControl)
	}
}
