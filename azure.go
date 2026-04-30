//go:build !no_azure

package blobcache

import (
	_ "gocloud.dev/blob/azureblob"
)
