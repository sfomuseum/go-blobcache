package main

import (
	"context"
	"log"

	_ "github.com/aaronland/gocloud/blob/s3"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/sfomuseum/go-blobcache/app/blobcache"
)

func main() {

	ctx := context.Background()
	err := blobcache.Run(ctx)

	if err != nil {
		log.Fatalf("Failed to run cache application, %v", err)
	}
}
