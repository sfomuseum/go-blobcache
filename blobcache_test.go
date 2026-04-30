package blobcache

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestBlobCache(t *testing.T) {

	ctx := context.Background()

	c, err := NewBlobCache(ctx, "mem://?max-size=1M&index-dsn=:memory:")

	if err != nil {
		t.Fatalf("Failed to create blobcache, %v", err)
	}

	k := "key"
	v := "hello world"

	err = c.Set(ctx, k, strings.NewReader(v))

	if err != nil {
		t.Fatalf("Failed to set cache item, %v", err)
	}

	r, err := c.Get(ctx, k)

	if err != nil {
		t.Fatalf("Failed to retrieve cache item, %v", err)
	}

	body, err := io.ReadAll(r)

	if err != nil {
		t.Fatalf("Failed to read cache item, %v", err)
	}

	if string(body) != v {
		t.Fatalf("Unexpected body for cache item")
	}

	err = c.Unset(ctx, k)

	if err != nil {
		t.Fatalf("Failed to unset cache item, %v", err)
	}

	_, err = c.Get(ctx, k)

	if err == nil {
		t.Fatalf("Expected error after removing cache item")
	}

	if err != CacheMiss {
		t.Fatalf("Expected cache miss but got %v", err)
	}
}
