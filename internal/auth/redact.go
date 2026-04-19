// SPDX-License-Identifier: Apache-2.0
//
// Derived from github.com/cloudflare/artifact-fs/internal/auth.
// Modified for use in ghfs.

// Package auth provides helpers for redacting credentials from strings
// before they are logged or returned in error messages.
package auth

import (
	"net/url"
	"regexp"
	"strings"
)

var tokenLike = regexp.MustCompile(`(?i)(access_token|token|password|passwd|secret|key|authorization|x-token-auth)=([^&\s]+)`)

// RedactRemoteURL strips credentials from a URL and redacts token-like
// query parameters. Returns the input unchanged if it is not a valid URL.
func RedactRemoteURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return tokenLike.ReplaceAllString(raw, `$1=REDACTED`)
	}
	if u.User != nil {
		username := u.User.Username()
		if _, ok := u.User.Password(); ok || username != "" {
			u.User = url.User("REDACTED")
		}
	}
	if u.RawQuery != "" {
		u.RawQuery = tokenLike.ReplaceAllString(u.RawQuery, `$1=REDACTED`)
	}
	return u.String()
}

// RedactString applies the same redaction to any URL-shaped substrings
// contained within s, and scrubs token-like key=value pairs outside URLs.
func RedactString(s string) string {
	if s == "" {
		return ""
	}
	s = tokenLike.ReplaceAllString(s, `$1=REDACTED`)
	if strings.Contains(s, "://") {
		parts := strings.Split(s, " ")
		for i := range parts {
			if strings.Contains(parts[i], "://") {
				parts[i] = RedactRemoteURL(parts[i])
			}
		}
		s = strings.Join(parts, " ")
	}
	return s
}
