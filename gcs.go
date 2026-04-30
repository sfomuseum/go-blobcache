//go:build !no_gcs

package blobcache

import (
	_ "gocloud.dev/blob/gcsblob"
)
