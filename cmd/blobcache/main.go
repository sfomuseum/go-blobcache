package main

import (
	"context"
	"log"

	"github.com/sfomuseum/go-blobcache/app/blobcache"
)

func main() {

	ctx := context.Background()
	err := blobcache.Run(ctx)

	if err != nil {
		log.Fatalf("Failed to run cache application, %v", err)
	}
}
