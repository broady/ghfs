//go:build !linux && !darwin

package blobcache

import (
	"os"
	"time"
)

// fileAtime falls back to ModTime on platforms we don't have atime
// syscall bindings for. Eviction becomes FIFO-ish rather than LRU.
func fileAtime(info os.FileInfo) time.Time {
	return info.ModTime()
}
