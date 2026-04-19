//go:build linux

package blobcache

import (
	"os"
	"syscall"
	"time"
)

// fileAtime returns the access time from a FileInfo. On noatime mounts
// this is approximately the same as ctime/mtime, which is still fine
// for a FIFO-ish eviction fallback.
func fileAtime(info os.FileInfo) time.Time {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return time.Unix(st.Atim.Sec, st.Atim.Nsec)
	}
	return info.ModTime()
}
