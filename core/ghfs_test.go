package core

import (
	"testing"
)

func TestParseRepoAndRef_PlainName(t *testing.T) {
	t.Parallel()
	repo, ref, err := ParseRepoAndRef("ghfs")
	if err != nil {
		t.Fatalf("ParseRepoAndRef: %v", err)
	}
	if repo != "ghfs" {
		t.Errorf("repo = %q, want %q", repo, "ghfs")
	}
	if ref != "" {
		t.Errorf("ref = %q, want empty (HEAD)", ref)
	}
}

func TestParseRepoAndRef_WithBranch(t *testing.T) {
	t.Parallel()
	repo, ref, err := ParseRepoAndRef("ghfs@main")
	if err != nil {
		t.Fatalf("ParseRepoAndRef: %v", err)
	}
	if repo != "ghfs" || ref != "main" {
		t.Errorf("got (%q, %q), want (%q, %q)", repo, ref, "ghfs", "main")
	}
}

func TestParseRepoAndRef_WithTag(t *testing.T) {
	t.Parallel()
	repo, ref, err := ParseRepoAndRef("linux@v6.1")
	if err != nil {
		t.Fatalf("ParseRepoAndRef: %v", err)
	}
	if repo != "linux" || ref != "v6.1" {
		t.Errorf("got (%q, %q), want (%q, %q)", repo, ref, "linux", "v6.1")
	}
}

func TestParseRepoAndRef_WithShortSHA(t *testing.T) {
	t.Parallel()
	repo, ref, err := ParseRepoAndRef("go@abc1234")
	if err != nil {
		t.Fatalf("ParseRepoAndRef: %v", err)
	}
	if repo != "go" || ref != "abc1234" {
		t.Errorf("got (%q, %q), want (%q, %q)", repo, ref, "go", "abc1234")
	}
}

func TestParseRepoAndRef_EmptyName(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoAndRef("")
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestParseRepoAndRef_EmptyRef(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoAndRef("ghfs@")
	if err == nil {
		t.Fatal("expected error for empty ref after '@'")
	}
}

func TestParseRepoAndRef_EmptyRepo(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoAndRef("@main")
	if err == nil {
		t.Fatal("expected error for empty repo")
	}
}

func TestParseRepoAndRef_SlashInRef(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRepoAndRef("ghfs@feature/x")
	if err == nil {
		t.Fatal("expected error for '/' in ref (FUSE can't carry slashes in entry names)")
	}
}

func TestJoinRepoPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		dir, base, want string
	}{
		{".", "file", "file"},
		{"", "file", "file"},
		{"a", "b", "a/b"},
		{"a/b", "c", "a/b/c"},
	}
	for _, c := range cases {
		got := joinRepoPath(c.dir, c.base)
		if got != c.want {
			t.Errorf("joinRepoPath(%q, %q) = %q, want %q", c.dir, c.base, got, c.want)
		}
	}
}

func TestBlockedPathsCoverVCS(t *testing.T) {
	t.Parallel()
	for _, name := range []string{".git", ".svn", ".cvs", ".hg", ".bzr", ".jj"} {
		if !blockedInRepo[name] {
			t.Errorf("blockedInRepo[%q] should be true (VCS metadata must block in-repo)", name)
		}
		if !blockedTopLevel[name] {
			t.Errorf("blockedTopLevel[%q] should be true (VCS metadata must block at owner level too)", name)
		}
	}
}

func TestBlockedTopLevelCoversDesktopProbes(t *testing.T) {
	t.Parallel()
	// Regression: desktop-stack probes fired at mount (see ghfs.log)
	// should not reach the GitHub API.
	for _, name := range []string{
		"autorun.inf", ".xdg-volume-info", ".Trash",
		"AACS", "BDMV", "bdmv", "VIDEO_TS",
		".DS_Store", "System Volume Information",
	} {
		if !isBlockedTopLevel(name) {
			t.Errorf("isBlockedTopLevel(%q) should be true", name)
		}
	}
	// .Trash-<uid> is matched by prefix, not exact membership.
	for _, name := range []string{".Trash-1000", ".Trash-1000-files", ".Trash-0"} {
		if !isBlockedTopLevel(name) {
			t.Errorf("isBlockedTopLevel(%q) should be true (per-uid trash)", name)
		}
	}
	// Must NOT block things a repo could legitimately contain.
	for _, name := range []string{"autorun.inf", "BDMV", "AACS"} {
		if blockedInRepo[name] {
			t.Errorf("blockedInRepo[%q] should be false (legit repo contents must pass through)", name)
		}
	}
}
