package lister

import "errors"

var (
	// ErrCacheSyncFailed is returned by a lister when we fail to synchronize its internal cache.
	ErrCacheSyncFailed = errors.New("cache sync failed")
)
