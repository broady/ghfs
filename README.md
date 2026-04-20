ghfs
====

The GitHub Filesystem (GHFS) is a user space filesystem that overlays the
GitHub API. It allows you to access repositories and files using standard
Unix commands such as `ls` and `cat`.

Fork of [benbjohnson/ghfs](https://github.com/benbjohnson/ghfs).

## Install

To use ghfs, you'll need to install [Go][go]. If you're running macOS then you'll
also need to install [macFUSE][macfuse].

To run ghfs:

```sh
$ go install github.com/broady/ghfs@latest
$ ghfs --help
```

Or run without installing (npx/uvx-equivalent):

```sh
$ go run github.com/broady/ghfs@latest ~/github.com
```

Now you can read data from the GitHub API via the `~/github.com` directory.

[go]: https://golang.org
[macfuse]: https://osxfuse.github.io


## Usage

### Basic Usage

GHFS uses GitHub URL conventions for pathing. For example, to go to a user
you can `cd` using their username and list their repos:

```sh
$ ls ~/github.com/broady/ | grep ghfs
ghfs
```

To go to a repository, you can use the username and repository name:

```sh
$ cd ~/github.com/broady/ghfs
```

Once you're in a repository, you can list files using `ls` and you can print
out file contents using the `cat` tool.

```sh
$ cat ~/github.com/broady/ghfs/LICENSE | head -1
The MIT License (MIT)
```

### Configuration

#### GitHub Authentication

To authenticate with GitHub and increase API rate limits, pass your personal access token:

```sh
$ ghfs --token=YOUR_TOKEN ~/github.com
```

Or use the GitHub CLI to automatically provide your token:

```sh
$ ghfs --token $(gh auth token) ~/github.com
```

The token can also be passed via the `GITHUB_TOKEN` environment variable, which keeps it out of `ps` / cmdline. ghfs reads it at startup and unsets it immediately so spawned children (e.g. `git cat-file`) don't inherit it. Handy for systemd units:

```sh
$ GITHUB_TOKEN=$(gh auth token) ghfs ~/github.com
```

Pass `--anonymous` to skip authentication entirely (the public GitHub API allows 60 requests/hour).

#### Caching

GHFS keeps three kinds of local state under `--cache-dir` (default `~/.cache/ghfs`):

1. **Blob cache** — hydrated git blob contents, content-addressed under `<cache-dir>/blobs`. Sized by `--cache-disk-mb` (default 1024 MB); oldest-first eviction when over budget.
2. **API response cache** — cached GitHub REST responses (org/user repo listings, Contents API results). Two-tier: in-memory LRU sized by `--api-cache-mem-mb` (default 128 MB), backed by disk at `<cache-dir>/api` (disk portion has no size cap). Revalidated with ETags via [`httpcache`](https://github.com/gregjones/httpcache).
3. **Blobless repo clones** — `git clone --filter=blob:none` checkouts under `<cache-dir>/repos`, one per repo you touch. Not size-bounded; delete a subdirectory to reclaim space (it will re-clone on next access).

Examples:

```sh
# Defaults: 1 GB blob cache, 128 MB API-response memory cache, ~/.cache/ghfs
$ ghfs ~/github.com

# Larger blob cache (5 GB) and API-response memory cache (500 MB)
$ ghfs --cache-disk-mb=5120 --api-cache-mem-mb=500 ~/github.com

# Custom cache root
$ ghfs --cache-dir=/tmp/my-cache ~/github.com

# Disable the API response cache (always hit the network for metadata;
# blob cache and repo clones are unaffected)
$ ghfs --no-api-cache ~/github.com
```

**Force cache refresh:**

The API response cache fronts a 5-minute revalidation-suppression window so shell prompts and eza-style icon probes don't hit GitHub on every render. To force immediate revalidation and pull updates for every cloned repo, send SIGUSR1:

```sh
# The command is shown in the startup logs, or find the PID:
$ kill -USR1 $(pgrep ghfs)
```

This clears the suppression window (so the next API request revalidates via ETag) and runs `git fetch` on every cloned repo. It does **not** invalidate kernel FUSE dcache entries, which expire naturally after 30 minutes.

#### Other flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `--anonymous` | off | Skip authentication. Unauthenticated GitHub API allows 60 req/hour. |
| `--cat-file-pool=N` | 4 | Max persistent `git cat-file --batch` processes per repo. |
| `--hydrate-workers=N` | 4 | Blob hydration worker goroutines per repo. |

#### Logging

Control the log level using the `GHFS_LOG_LEVEL` environment variable. Valid levels are `debug`, `info`, `warn`, and `error`. Defaults to `info`.

```sh
# Enable debug logging
$ GHFS_LOG_LEVEL=debug ghfs ~/github.com

# Suppress warnings
$ GHFS_LOG_LEVEL=error ghfs ~/github.com
```

## TODO

- Handle rate limits and 429s properly.

## License

ghfs is distributed under the [MIT License](LICENSE), with the following exceptions:

- `internal/gitstore/`, `internal/hydrator/`, `internal/auth/redact.go`, and `internal/model/types.go` are derived from [cloudflare/artifact-fs](https://github.com/cloudflare/artifact-fs) and are licensed under the [Apache License, Version 2.0](LICENSE-APACHE). Individual files carry `SPDX-License-Identifier: Apache-2.0` headers. Thank you to the Cloudflare authors.
