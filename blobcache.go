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
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/aaronland/gocloud/blob/bucket"
	sfom_sql "github.com/sfomuseum/go-database/sql"
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

	index_dsn := ":memory:?cache=shared"

	if q.Has("index-dsn") {
		index_dsn = q.Get("index-dsn")
	}

	index_db, err := sql.Open("sqlite3", index_dsn)

	if err != nil {
		return nil, fmt.Errorf("Failed to instantiate index database, %w", err)
	}

	has_table, err := sfom_sql.HasTable(ctx, index_db, "blobcache")

	if err != nil {
		return nil, fmt.Errorf("Failed to determine if blobcache table exists, %w", err)
	}

	if !has_table {

		_, err := index_db.ExecContext(ctx, "CREATE TABLE blobcache (key TEXT PRIMARY KEY, size INTEGER, modtime INTEGER)")

		if err != nil {
			return nil, fmt.Errorf("Failed to create blobcache table, %w", err)
		}
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

func (c *BlobCache) addToIndex(ctx context.Context, key string, size int64, modtime int64) error {

	_, err := c.index_db.ExecContext(ctx, "INSERT OR REPLACE INTO blobcache (key, size, modtime) VALUES(?, ?, ?)", key, size, modtime)
	return err
}

func (c *BlobCache) removeFromIndex(ctx context.Context, key string) error {

	_, err := c.index_db.ExecContext(ctx, "DELETE from blobcache WHERE key = ?", key)
	return err
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

func (c *BlobCache) Index(ctx context.Context) error {

	if c.bucket == nil {
		return nil
	}

	logger := slog.Default()

	if c.indexing.Load() == true {
		logger.Debug("Indexing already in progress.")
		return nil
	}

	c.indexing.Store(true)
	defer c.indexing.Store(false)

	logger.Debug("Start cache indexing")

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

	// First gather all the keys in the index

	logger.Debug("Gather existing keys in index")

	rows, err := c.index_db.QueryContext(ctx, "SELECT key FROM blobcache")

	if err != nil {
		logger.Error("Failed to select keys from index", "error", err)
		return err
	}

	defer rows.Close()

	for rows.Next() {

		var key string

		err := rows.Scan(&key)

		if err != nil {
			logger.Error("Failed to scan row", "error", err)
			return err
		}

		objects_map.Store(key, true)
	}

	err = rows.Close()

	if err != nil {
		logger.Error("Failed to close rows after reading keys", "error", err)
		return err
	}

	err = rows.Err()

	if err != nil {
		logger.Error("Error reported after reading keys", "error", err)
		return err
	}

	// Now plough through the cache deleting keys from objects_map as we go.
	// Anything left over should be removed from the index in the next step

	logger.Debug("Walk cache")

	for obj, _ := range iter {

		if obj.IsDir {
			continue
		}

		wg.Go(func() {

			now := time.Now()
			ts := obj.ModTime.Unix()

			objects_map.Delete(obj.Key)

			if now.Unix()-c.max_age > ts {

				logger.Debug("Key triggers max age, schedule for removal", "key", obj.Key, "mod time", obj.ModTime)

				wg.Go(func() {

					ctx := context.Background()
					err := c.bucket.Delete(ctx, obj.Key)

					if err != nil {
						logger.Error("Failed to delete stale object", "key", obj.Key, "error", err)
						return
					}

					err = c.removeFromIndex(ctx, obj.Key)

					if err != nil {
						logger.Error("Failed to delete object from blobcache", "key", obj.Key, "error", err)
						return
					}
				})

				return
			}

			err := c.addToIndex(ctx, obj.Key, obj.Size, ts)

			if err != nil {
				logger.Error("Failed to insert object in to index", "key", obj.Key, "error", err)
			}

			atomic.AddInt64(&size, obj.Size)
			atomic.AddInt64(&count, 1)
		})
	}

	wg.Wait()
	t_done <- true

	err = err_fn()

	if err != nil {
		logger.Error("Bucket iterator returned an error", "error", err)
		return err
	}

	// Now remove old or errant entries in the database

	logger.Debug("Remove errant keys from index")

	objects_map.Range(func(k, v any) bool {

		key := k.(string)

		err = c.removeFromIndex(ctx, key)

		if err != nil {
			logger.Error("Failed to delete object from blobcache", "key", key, "error", err)
		}

		return true
	})

	logger.Debug("Completed cache indexing", "count", count, "size", size, "time", time.Since(t1))

	c.indexed.Store(true)
	return nil
}

func (c *BlobCache) Prune(ctx context.Context) error {

	if c.bucket == nil {
		return nil
	}

	logger := slog.Default()

	if c.indexing.Load() == true {
		logger.Debug("Indexing already in progress.")
		return nil
	}

	if !c.indexed.Load() == true {
		logger.Debug("Indexing not complete, start indexing")
		go c.Index(ctx)
		return nil
	}

	if c.pruning.Load() == true {
		logger.Debug("Pruning is already in progress.")
		return nil
	}

	c.pruning.Store(true)
	defer c.pruning.Store(false)

	row := c.index_db.QueryRowContext(ctx, "SELECT SUM(size) AS size FROM blobcache")

	var size int64
	err := row.Scan(&size)

	if err != nil {
		logger.Error("Failed to determine total size of items in index", "error", err)
		return err
	}

	if size <= c.max_size {
		logger.Debug("Cache is still within max size limits", "size", size, "max size", c.max_size)
		return nil
	}

	logger.Debug("Cache exceeds max size limits", "size", size, "max size", c.max_size)

	rows, err := c.index_db.QueryContext(ctx, "SELECT key, size FROM blobcache ORDER BY modtime ASC")

	if err != nil {
		logger.Error("Failed to select keys for pruning", "error", err)
		return err
	}

	defer rows.Close()

	for rows.Next() {

		var key string
		var sz int64

		err := rows.Scan(&key, &sz)

		if err != nil {
			logger.Error("Failed to scan key for pruning", "error", err)
			return err
		}

		logger.Debug("Remove key from cache", "key", key)

		err = c.bucket.Delete(ctx, key)

		if err != nil {
			logger.Error("Failed to delete stale object", "key", key, "error", err)
			continue
		}

		err = c.removeFromIndex(ctx, key)

		if err != nil {
			logger.Error("Failed to delete stale object from index", "key", key, "error", err)
			continue
		}

		size -= sz

		if size < c.max_size {
			logger.Debug("Cache now fits size limits", "size", size)
			break
		}
	}

	err = rows.Close()

	if err != nil {
		logger.Error("Failed to close rows after pruning", "error", err)
		return err
	}

	err = rows.Err()

	if err != nil {
		logger.Error("Rows reported an error after pruning", "error", err)
		return err
	}

	return nil
}
