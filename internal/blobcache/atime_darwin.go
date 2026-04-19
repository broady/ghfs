//go:build darwin

package blobcache

import (
	"os"
	"syscall"
	"time"
)

// fileAtime returns the access time from a FileInfo on darwin.
func fileAtime(info os.FileInfo) time.Time {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return time.Unix(st.Atimespec.Sec, st.Atimespec.Nsec)
	}
	return info.ModTime()
}
