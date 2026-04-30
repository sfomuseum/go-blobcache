//go:build !no_s3

package blobcache

import (
	_ "github.com/aaronland/gocloud/blob/s3"
	_ "gocloud.dev/blob/s3blob"
)
