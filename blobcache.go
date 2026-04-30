package blobcache

import (
	"context"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/aaronland/gocloud/blob/bucket"
	"gocloud.dev/blob"
)

var CacheMiss = errors.New("Cache miss")

const (
	Kilobyte = 1024
	Megabyte = 1024 * 1024
	Gigabyte = 1024 * 1024 * 1024
)

type BlobCache struct {
	bucket   *blob.Bucket
	ticker   *time.Ticker
	done_ch  chan bool
	max_age  int64
	max_size int64
	index_db *sql.DB
	indexing *atomic.Bool
	indexed  *atomic.Bool
	pruning  *atomic.Bool
}

func NewBlobCache(ctx context.Context, uri string) (*BlobCache, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()

	d, err := time.ParseDuration(fmt.Sprintf("%dh", 7*24))

	if err != nil {
		return nil, err
	}

	max_age := int64(d.Seconds())
	max_size := int64(10 * Gigabyte)

	if q.Has("max-age") {
		//
		q.Del("max-age")
	}

	if q.Has("max-size") {
		//
		q.Del("max-size")
	}

	index_dsn := ":memory:?cache=shared&mode=memory"

	if q.Has("index-dsn") {
		index_dsn = q.Get("index-dsn")
		q.Del("index-dsn")
	}

	u.RawQuery = q.Encode()
	uri = u.String()

	var b *blob.Bucket

	switch u.Scheme {
	case "null":
		//
	default:

		switch u.Scheme {
		case "file":

			err := os.MkdirAll(u.Path, 0750)

			if err != nil {
				return nil, fmt.Errorf("Failed to make cache root, %w", err)
			}
		}

		b, err = bucket.OpenBucket(ctx, uri)

		if err != nil {
			return nil, fmt.Errorf("Failed to open cache bucket, %w", err)
		}
	}

	slog.Debug("Set up index database", "dsn", index_dsn)

	index_db, err := setupBlobCacheIndex(ctx, index_dsn)

	if err != nil {
		return nil, err
	}

	indexing := new(atomic.Bool)
	indexing.Store(false)

	indexed := new(atomic.Bool)
	indexed.Store(false)

	pruning := new(atomic.Bool)
	pruning.Store(false)

	c := &BlobCache{
		bucket:   b,
		max_age:  max_age,
		max_size: max_size,
		index_db: index_db,
		indexing: indexing,
		indexed:  indexed,
		pruning:  pruning,
	}

	if b != nil {

		ticker := time.NewTicker(60 * time.Second)
		done_ch := make(chan bool)

		c.ticker = ticker
		c.done_ch = done_ch

		go func() {

			for {
				select {
				case <-done_ch:
					return
				case <-ticker.C:

					ctx := context.Background()
					err := c.Prune(ctx)

					if err != nil {
						slog.Error("Failed to prune blobcache", "error", err)
					}
				}
			}
		}()
	}

	return c, nil
}

func (c *BlobCache) Get(ctx context.Context, key string) (io.ReadSeekCloser, error) {

	if c.bucket == nil {
		return nil, CacheMiss
	}

	path := c.derivePathFromKey(key)

	exists, err := c.bucket.Exists(ctx, path)

	if err != nil {
		return nil, fmt.Errorf("Failed to determine if key exists, %w", err)
	}

	if !exists {
		return nil, CacheMiss
	}

	return c.bucket.NewReader(ctx, path, nil)
}

func (c *BlobCache) Set(ctx context.Context, key string, r io.Reader) error {

	if c.bucket == nil {
		return nil
	}

	path := c.derivePathFromKey(key)
	wr, err := c.bucket.NewWriter(ctx, path, nil)

	if err != nil {
		return fmt.Errorf("Failed to create new writer for key, %w", err)
	}

	sz, err := io.Copy(wr, r)

	if err != nil {
		return fmt.Errorf("Failed to write cache item, %w", err)
	}

	err = wr.Close()

	if err != nil {
		return fmt.Errorf("Failed to close cache item after writing, %w", err)
	}

	go func() {

		now := time.Now()
		ts := now.Unix()

		err := c.addToIndex(ctx, path, sz, ts)

		if err != nil {
			slog.Warn("Failed to add item to index", "path", path, "error", err)
		}
	}()

	return nil
}

func (c *BlobCache) Unset(ctx context.Context, key string) error {

	if c.bucket == nil {
		return nil
	}

	path := c.derivePathFromKey(key)
	err := c.bucket.Delete(ctx, path)

	if err != nil {
		return err
	}

	go func() {

		err := c.removeFromIndex(ctx, path)

		if err != nil {
			slog.Warn("Failed to remove item from index", "path", path, "error", err)
		}
	}()

	return nil
}

func (c *BlobCache) Close() error {

	if c.ticker != nil {
		c.ticker.Stop()
	}

	if c.done_ch != nil {
		c.done_ch <- true
	}

	if c.bucket != nil {
		c.bucket.Close()
	}

	return nil
}

func (c *BlobCache) Attributes(ctx context.Context, key string) (*blob.Attributes, error) {

	path := c.derivePathFromKey(key)

	exists, err := c.bucket.Exists(ctx, path)

	if err != nil {
		return nil, fmt.Errorf("Failed to determine if key exists, %w", err)
	}

	if !exists {
		return nil, CacheMiss
	}

	return c.bucket.Attributes(ctx, path)
}

func (c *BlobCache) derivePathFromKey(key string) string {

	path := c.deriveTreeFromKey(key)

	u, err := url.Parse(key)

	if err == nil {
		path = filepath.Join(u.Host, path)
	}

	return path
}

func (c *BlobCache) deriveTreeFromKey(key string) string {

	input := c.hashKey(key)
	parts := []string{}

	for len(input) > 6 {

		chunk := input[0:6]
		input = input[6:]
		parts = append(parts, chunk)
	}

	if len(input) > 0 {
		parts = append(parts, input)
	}

	path := filepath.Join(parts...)
	return path
}

func (c *BlobCache) hashKey(key string) string {

	data := []byte(key)
	return fmt.Sprintf("%x", md5.Sum(data))
}
