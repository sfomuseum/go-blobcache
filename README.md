# go-blobcache

Minimalist and opionated Go package implementing a file-based cache using the [gocloud.dev/blob](https://pkg.go.dev/gocloud.dev/blob) package.

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

	// Remove item from cache

	cache.Unset(ctx, im_path)
}
```

_Error handling omitted for the sake of brevity._

## Blob caches

Blob caches are instantiations of the [gocloud.dev/blob.Bucket](https://gocloud.dev/howto/blob/) abstraction which provides common file (blob) related operations for a number of different providers. These providers are configured using a URI-based syntax. These are the same URIs you pass to the `blobcache.NewBlobCache` method.

### Providers

#### Local

Support for caching data on the local filesystem or in-memory. For example:

```
file:///usr/local/data/blobcache

mem://
```

Please consult the [local storage documentation](https://gocloud.dev/howto/blob/#local) for details. 

#### AWS S3

Support for caching data using the AWS S3 storage service. For example:

```
s3://my-bucket?region=us-west-1&prefix=blobcache/
```

Please consult the [AWS storage documentation](https://gocloud.dev/howto/blob/#s3) for details. 

#### Google Cloud

Support for caching data using the Google Cloud Storage service. For example:

```
gs://my-blobcache
```

Please consult the [Google storage documentation](https://gocloud.dev/howto/blob/#gcs) for details.

#### Azure

Support for caching data using the Azure Blob Storage service. For example:

```
azblob://my-blobcache"
```

Please consult the [Azure storage documentation](https://gocloud.dev/howto/blob/#azure) for details.

### Additional query parameters

In addition to the query parameters specific to the provider you choose, the following parameters are also accepted.

| Name | Type | Required | Notes |
| --- | --- | --- | --- |
| max-age | string | No | The maximum age of cached items expressed as an ISO8601 duration string. Default is "P1W" (7 days). |
| max-size | string | No | The maximum total cache size expressed as a string. Default is "10GB". |
| index-dsn | string | No | A valid SQLite DSN string used to create a local database to track (and remove) cached items. Default is ":default:" which creates a `blobcache/blobcache.db` file in the current user's "cache" directory. |

## Pruning cached items

This package maintains a separate index of cached items in a (local) SQLite database. Items are added and removed from the index as they are added and removed from the cache. Internally, the index is checked every (60) seconds to determine whether cached items which are older than the configured maximum age should be removed or whether the overall cache size exceeds the configure limits.

If a cache exceeds its size limits objects with the least amount of access and the oldest creation dates are removed first. This process continues until the cache no longer exceeds its size limits.

If you need to repopulate an index from an existing blobcache you can run the `blobcache -action index` command described below.

## Tools

```
$> make cli
go build -tags= -mod vendor -ldflags="-s -w" -o bin/blobcache cmd/blobcache/main.go
```

### blobcache

```
$> ./bin/blobcache -h
  -action string
    	Valid actions are: get, set, unset, index, prune.
  -cache-uri string
    	A valid blobcache.BlobCache URI constructor. (default "mem://")
  -data string
    	The data to store in the blobcache. This flag is ignored unless -action is "set".
  -key string
    	The name of the key to access from the blobcache. This flag is ignored if -action is "index" or "prune".
  -verbose
    	Enable verbose (debug) logging.
```

#### blobcache set

```
$> ./bin/blobcache -cache-uri file:///tmp/blobcache -action set -key test -data 'hello world'
```

#### blobcache get

```
$> ./bin/blobcache -cache-uri file:///tmp/blobcache -action get -key test
hello world
```

#### blobcache unset

```
$> ./bin/blobcache -cache-uri file:///tmp/blobcache -action unset -key test
$> ./bin/blobcache -cache-uri file:///tmp/blobcache -action get -key test
2026/04/30 12:04:15 Failed to run cache application, Cache miss
```

#### blobcache prune

```
$> ./bin/blobcache -cache-uri file:///usr/local/data/blobcache -verbose -action prune
2026/04/30 11:32:05 DEBUG Verbose logging enabled
2026/04/30 11:32:05 DEBUG Set up index database dsn=:default:
2026/04/30 11:32:05 DEBUG Result blobcache index DSN dsn="/Users/example/Library/Caches/blobcache/blobcache.db?cache=shared"
2026/04/30 11:32:05 DEBUG Query objects in blobcache index older than age=1776969125
2026/04/30 11:32:05 DEBUG No more records older than
2026/04/30 11:32:05 DEBUG Query size of objects in blobcache index
2026/04/30 11:32:05 DEBUG Cache is still within max size limits size=0 "max size"=10737418240
```

#### blobcache index

```
$./bin/blobcache -cache-uri file:///usr/local/data/blobcache -verbose -action index
2026/04/30 11:38:19 DEBUG Verbose logging enabled
2026/04/30 11:38:19 DEBUG Set up index database dsn=:default:
2026/04/30 11:38:19 DEBUG Result blobcache index DSN dsn="/Users/example/Library/Caches/blobcache/blobcache.db?cache=shared"
2026/04/30 11:38:19 DEBUG Start cache indexing
2026/04/30 11:38:19 DEBUG Gather existing keys in index
2026/04/30 11:38:19 DEBUG Walk cache
2026/04/30 11:38:49 DEBUG Indexing in progress count=0 size=0
2026/04/30 11:39:19 DEBUG Indexing already in progress.
2026/04/30 11:39:19 DEBUG Indexing in progress count=1000 size=156628336
2026/04/30 11:39:49 DEBUG Indexing in progress count=2000 size=319889721
...time passes
2026/04/30 12:05:19 DEBUG Indexing in progress count=37000 size=6054550603
2026/04/30 12:05:48 DEBUG Time to walk cache time=27m28.997104333s
2026/04/30 12:05:48 DEBUG Remove errant keys from index
2026/04/30 12:05:48 DEBUG Completed cache indexing count=37406 size=6121126481 time=27m28.997153625s
2026/04/30 12:05:48 DEBUG Indexing complete count=37406 size=6121126481
```

And then:

```
$> ./bin/blobcache -cache-uri file:///usr/local/data/blobcache -verbose -action prune
2026/04/30 12:08:02 DEBUG Verbose logging enabled
2026/04/30 12:08:02 DEBUG Set up index database dsn=:default:
2026/04/30 12:08:02 DEBUG Result blobcache index DSN dsn="/Users/asc/Library/Caches/blobcache/blobcache.db?cache=shared"
2026/04/30 12:08:02 DEBUG Query objects in blobcache index older than age=1776971282
2026/04/30 12:08:02 DEBUG No more records older than
2026/04/30 12:08:02 DEBUG Query size of objects in blobcache index
2026/04/30 12:08:02 DEBUG Cache is still within max size limits size=6121126481 "max size"=10737418240
```

## Build tags

The following build tags are available:

### no_azure

Disable support for storing data using the Azure Blob Storage service.

### no_gcs

Disable support for storing data using the Google Cloud Storage service.

### no_local

Disable support for storing data on the local filesystem or in memory.

### no_s3

Disable support for storing data using the AWS S3 service.