// SPDX-License-Identifier: Apache-2.0
//
// Taken verbatim from github.com/cloudflare/artifact-fs/internal/hydrator.

// Inflight deduplicates multiple callers waiting on the same key. Used
// to coalesce FUSE readers that all want the same OID hydrated.
package hydrator

type inflight[T any] map[string][]chan T

func newInflight[T any]() inflight[T] {
	return make(map[string][]chan T)
}

// add registers ch as a waiter for key. Returns true if this is the
// first waiter (the caller should initiate the underlying work).
func (f inflight[T]) add(key string, ch chan T) bool {
	f[key] = append(f[key], ch)
	return len(f[key]) == 1
}

// remove drops ch from the waiter list (e.g., when its context was
// cancelled). Safe to call on an unknown channel.
func (f inflight[T]) remove(key string, ch chan T) {
	waiters, ok := f[key]
	if !ok {
		return
	}
	for i, waiter := range waiters {
		if waiter != ch {
			continue
		}
		waiters = append(waiters[:i], waiters[i+1:]...)
		if len(waiters) == 0 {
			delete(f, key)
		} else {
			f[key] = waiters
		}
		return
	}
}

// take returns the waiter list for key and removes the key.
func (f inflight[T]) take(key string) []chan T {
	waiters := f[key]
	delete(f, key)
	return waiters
}

// closeAll notifies every waiter under every key with value, then drops
// all keys. Used on Service shutdown.
func (f inflight[T]) closeAll(value T) {
	for key := range f {
		notifyWaiters(f.take(key), value)
	}
}

func notifyWaiters[T any](waiters []chan T, value T) {
	for _, ch := range waiters {
		ch <- value
		close(ch)
	}
}
