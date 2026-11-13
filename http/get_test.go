package http

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/sfomuseum/go-blobcache"
)

func TestGetWithCache(t *testing.T) {

	ctx := context.Background()
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c, err := blobcache.NewBlobCache(ctx, "mem://?max-size=1M&index-dsn=:memory:")

	if err != nil {
		t.Fatalf("Failed to create blobcache, %v", err)
	}

	uri := "https://static.sfomuseum.org/media/152/784/368/7/1527843687_4UP45I9W4Gt87hNytE7ZUFD2fZGJjCl3_n.jpg"

	r, err := GetWithCache(ctx, c, uri)

	if err != nil {
		t.Fatalf("Failed to get with cache, %v", err)
	}

	defer r.Close()

	body, err := io.ReadAll(r)

	if err != nil {
		t.Fatalf("Failed to read URI body, %v", err)
	}

	if len(body) == 0 {
		t.Fatalf("Zero length body")
	}

	_, err = GetWithCache(ctx, c, uri)

	if err != nil {
		t.Fatalf("Failed to get with cache (twice), %v", err)
	}

}
