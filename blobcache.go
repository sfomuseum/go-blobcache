package blobcache

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
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

	c := &BlobCache{
		bucket:   b,
		max_age:  max_age,
		max_size: max_size,
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

					/*
						ctx := context.Background()
						err := c.Prune(ctx)

						if err != nil {
							slog.Error("Failed to prune blobcache", "error", err)
						}
					*/
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

	_, err = io.Copy(wr, r)

	if err != nil {
		return fmt.Errorf("Failed to write cache item, %w", err)
	}

	return wr.Close()
}

func (c *BlobCache) Unset(ctx context.Context, key string) error {

	if c.bucket == nil {
		return nil
	}

	path := c.derivePathFromKey(key)
	return c.bucket.Delete(ctx, path)
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

type object struct {
	Key     string
	ModTime int64
	Size    int64
}

func (c *BlobCache) Prune(ctx context.Context) error {

	if c.bucket == nil {
		return nil
	}

	logger := slog.Default()

	objects_map := new(sync.Map)

	size := int64(0)
	count := int64(0)

	li := c.bucket.List(nil)
	iter, err_fn := li.All(ctx)

	wg := new(sync.WaitGroup)
	t1 := time.Now()

	t := time.NewTicker(30 * time.Second)
	defer t.Stop()

	t_done := make(chan bool)

	go func() {

		for {
			select {
			case <-t_done:
				logger.Debug("Indexing complete", "count", atomic.LoadInt64(&count), "size", atomic.LoadInt64(&size))
				return
			case <-t.C:
				logger.Debug("Indexing in progress", "count", atomic.LoadInt64(&count), "size", atomic.LoadInt64(&size))

			}
		}
	}()

	for obj, _ := range iter {

		if obj.IsDir {
			continue
		}

		wg.Go(func() {

			now := time.Now()
			ts := obj.ModTime.Unix()

			if now.Unix()-c.max_age > ts {

				logger.Debug("Key triggers max age, schedule for removal", "key", obj.Key, "mod time", obj.ModTime)

				wg.Go(func() {
					ctx := context.Background()
					err := c.bucket.Delete(ctx, obj.Key)

					if err != nil {
						logger.Warn("Failed to delete stale object", "key", obj.Key, "error", err)
					}
				})

				return
			}

			objects_map.Store(obj.Key, obj)

			atomic.AddInt64(&size, obj.Size)
			atomic.AddInt64(&count, 1)
		})
	}

	wg.Wait()
	t_done <- true

	err := err_fn()

	if err != nil {
		logger.Error("Bucket iterator returned an error", "error", err)
		return err
	}

	logger.Debug("Completed cache indexing", "count", count, "size", size, "time", time.Since(t1))

	if size < c.max_size {
		logger.Debug("Cache does not exceed limits")
		return nil
	}

	logger.Debug("Cache exceeds size limits")

	objects := make([]object, count)

	objects_map.Range(func(k, v any) bool {
		objects = append(objects, v.(object))
		return true
	})

	sort.Slice(objects, func(i, j int) bool { return objects[i].ModTime < objects[j].ModTime })

	for i := int64(0); i < count; i++ {

		key := objects[i].Key
		err := c.bucket.Delete(ctx, key)

		if err != nil {
			logger.Warn("Failed to delete stale object", "key", key, "error", err)
			continue
		}

		size -= objects[i].Size

		if size < c.max_size {
			logger.Debug("Cache now fits size limits", "size", size)
			break
		}
	}

	return nil
}
