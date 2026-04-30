# go-blobcache

Minimalist and opionated Go package implementing a file-based cache using the [gocloud.dev/blob](https://pkg.go.dev/gocloud.dev/blob) package.

## Important

This is work in progress and incomplete. Notably there is no code to prune the cache.

## Example

```
package main

import (
	"context"
	"os"
	"log"

	"github.com/aaronland/go-blobcache"
)

func main() {

	ctx := context.Background()

	// Create a local file‑system cache in ./cache
	//   - keep items < 1 GB total
	//   - purge items older than 7 days
	
	cache, _ := blobcache.NewBlobCache(ctx, "file:///tmp/blobcache?max-size=1073741824")
	defer cache.Close()

	// Store an image

	im_path := "/path/to/image.png"
	im, _ := os.Open(im_path)
	
	_ cache.Set(ctx, im_path, im)

	// Retrieve the same image
	
	r, _ := cache.Get(ctx, im_path)
}
```

_Error handling omitted for the sake of brevity._

## Tools

```
$> make cli
go build -tags= -mod vendor -ldflags="-s -w" -o bin/blobcache cmd/blobcache/main.go
```

## Build tags

### no_azure

### no_gcs

### no_local

### no_s3