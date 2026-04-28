package main

import (
	"context"
	"io"
	"log"
	"log/slog"
	"os"

	_ "github.com/aaronland/gocloud/blob/s3"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/sfomuseum/go-blobcache"
	"github.com/sfomuseum/go-blobcache/http"
	"github.com/sfomuseum/go-flags/flagset"
)

func main() {

	var cache_uri string
	var uri string
	var verbose bool

	fs := flagset.NewFlagSet("cache")

	fs.StringVar(&cache_uri, "cache-uri", "mem://", "...")
	fs.StringVar(&uri, "uri", "", "...")
	fs.BoolVar(&verbose, "verbose", false, "Enable verbose (debug) logging.")

	flagset.Parse(fs)

	if verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Debug("Verbose logging enabled")
	}

	ctx := context.Background()

	c, err := blobcache.NewBlobCache(ctx, cache_uri)

	if err != nil {
		log.Fatal(err)
	}

	defer c.Close()

	r, err := http.GetWithCache(ctx, c, uri)

	if err != nil {
		log.Fatalf("Failed to get with cache, %v", err)
	}

	_, err = io.Copy(os.Stdout, r)

	if err != nil {
		log.Fatalf("Failed to write data, %v", err)
	}

}
