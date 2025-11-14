ghfs
====

The GitHub Filesystem (GHFS) is a user space filesystem that overlays the
GitHub API. It allows you to access repositories and files using standard
Unix commands such as `ls` and `cat`.

Fork of [benbjohnson/ghfs](https://github.com/benbjohnson/ghfs)

## Install

To use ghfs, you'll need to install [Go][go]. If you're running OS X then you'll
also need to install [MacFUSE][macfuse].

To run ghfs:

```sh
$ go install github.com/broady/ghfs@latest
$ ghfs -h
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
$ ghfs -token=YOUR_TOKEN ~/github.com
```

Or use the GitHub CLI to automatically provide your token:

```sh
$ ghfs -token $(gh auth token) ~/github.com
```

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
