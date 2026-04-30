//go:build !no_local

package blobcache

import (
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
)
