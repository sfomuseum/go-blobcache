package blobcache

import (
	"context"
	"crypto/md5"
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	sfom_sql "github.com/sfomuseum/go-database/sql"
)

//go:embed blobcache_schema.txt
var blobCacheTableSchema string

// removeObjectsStopFunc is a callback that determines whether
// pruning should stop after a particular object has been removed.
// The function receives the context, the key of the removed object,
// and either its size or modification time depending on the pruning
// strategy.  It should return true to stop pruning.
type removeObjectsStopFunc func(context.Context, string, int64) bool

// hashKey returns the MD5 digest of the supplied key as a hex string.
func hashKey(key string) string {

	data := []byte(key)
	return fmt.Sprintf("%x", md5.Sum(data))
}

// setupBlobCacheIndex creates or opens the SQLite database that
// stores the cache index.  If the supplied DSN is ":default:" the
// function creates a file in the user's cache directory under
// `blobcache/blobcache.db`.  The function configures the SQLite
// PRAGMA settings, ensures the `blobcache` table exists, and
// returns the opened *sql.DB.
func setupBlobCacheIndex(ctx context.Context, index_dsn string) (*sql.DB, error) {

	if index_dsn == ":default:" {

		cache_dir, err := os.UserCacheDir()

		if err != nil {
			return nil, fmt.Errorf("Failed to derive cache dir, %w", err)
		}

		root := filepath.Join(cache_dir, "blobcache")

		err = os.MkdirAll(root, 0750)

		if err != nil {
			return nil, fmt.Errorf("Failed to create blobcache root, %w", err)
		}

		db_path := filepath.Join(root, "blobcache.db")

		index_dsn = fmt.Sprintf("%s?cache=shared", db_path)
		slog.Debug("Result blobcache index DSN", "dsn", index_dsn)
	}

	index_db, err := sql.Open("sqlite3", index_dsn)

	if err != nil {
		return nil, fmt.Errorf("Failed to instantiate index database, %w", err)
	}

	pragma := sfom_sql.DefaultSQLitePragma()

	err = sfom_sql.ConfigureSQLitePragma(ctx, index_db, pragma)

	if err != nil {
		return nil, fmt.Errorf("Failed to configure PRAGMA, %w", err)
	}

	index_db.SetMaxOpenConns(1)

	has_table, err := sfom_sql.HasTable(ctx, index_db, "blobcache")

	if err != nil {
		return nil, fmt.Errorf("Failed to determine if blobcache table exists, %w", err)
	}

	if !has_table {

		_, err := index_db.ExecContext(ctx, blobCacheTableSchema)

		if err != nil {
			return nil, fmt.Errorf("Failed to create blobcache table, %w", err)
		}
	}

	return index_db, err
}

// addToIndex inserts or replaces the record for the supplied key
// in the cache index.  The size and modification time are stored
// as integer seconds.
func (c *BlobCache) addToIndex(ctx context.Context, key string, size int64, modtime int64) error {

	if c.index_db == nil {
		return nil
	}

	q := "INSERT INTO blobcache (bucket, key, size, accessed, modtime) VALUES(?, ?, ?, ?, ?) ON CONFLICT(bucket, key) DO UPDATE SET accessed=accessed+1, size=?, modtime=?"

	_, err := c.index_db.ExecContext(ctx, q, c.bucket_hash, key, size, 1, modtime, size, modtime)
	return err
}

// removeFromIndex deletes the record for the supplied key from
// the cache index.
func (c *BlobCache) removeFromIndex(ctx context.Context, key string) error {

	if c.index_db == nil {
		return nil
	}

	_, err := c.index_db.ExecContext(ctx, "DELETE from blobcache WHERE bucket = ? AND key = ?", c.bucket_hash, key)
	return err
}

// Index synchronises the cache index with the underlying bucket.
// It walks the bucket, adding new items, and removes any index entries
// that no longer exist in the bucket.  The function is safe to run
// concurrently – it uses atomic flags to prevent overlapping runs.
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

	t1 := time.Now()

	t := time.NewTicker(30 * time.Second)
	defer t.Stop()

	done_ch := make(chan bool)
	size_ch := make(chan int64)
	count_ch := make(chan int64)

	defer func() {
		done_ch <- true
	}()

	go func() {

		for {
			select {
			case <-done_ch:
				logger.Debug("Indexing complete", "count", atomic.LoadInt64(&count), "size", atomic.LoadInt64(&size))
				return
			case i := <-size_ch:
				atomic.AddInt64(&size, i)
			case i := <-count_ch:
				atomic.AddInt64(&count, i)
			case <-t.C:
				logger.Debug("Indexing in progress", "count", atomic.LoadInt64(&count), "size", atomic.LoadInt64(&size))

			}
		}
	}()

	// First gather all the keys in the index

	logger.Debug("Gather existing keys in index")

	rows, err := c.index_db.QueryContext(ctx, "SELECT key FROM blobcache WHERE bucket = ?", c.bucket_hash)

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

	err = c.walkCache(ctx, objects_map, size_ch, count_ch)

	if err != nil {
		logger.Error("Failed to walk cache", "error", err)
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

// Prune removes cached objects that are older than the configured
// max‑age or that cause the total size to exceed max‑size.
// It uses the index to determine which objects to delete and updates
// the index as it removes entries.
func (c *BlobCache) Prune(ctx context.Context) error {

	if c.bucket == nil {
		return nil
	}

	logger := slog.Default()

	if c.indexing.Load() == true {
		logger.Debug("Indexing already in progress.")
		return nil
	}

	if c.pruning.Load() == true {
		logger.Debug("Pruning is already in progress.")
		return nil
	}

	c.pruning.Store(true)
	defer c.pruning.Store(false)

	// Delete older here...

	now := time.Now()
	ts := now.Unix()
	then := ts - c.max_age

	logger.Debug("Query objects in blobcache index older than", "age", then)

	stopFunc := func(ctx context.Context, obj_key string, obj_modtime int64) bool {
		return false
	}

	for {

		count, err := c.removeObjects(ctx, stopFunc, "SELECT key, size FROM blobcache WHERE bucket = ? AND modtime < ?", c.bucket_hash, then)

		if err != nil {
			logger.Error("Failed to remove objects older than", "age", then, "error", err)
			return err
		}

		if count == 0 {
			logger.Debug("No more records older than")
			break
		}
	}

	logger.Debug("Query size of objects in blobcache index")

	var size int64

	row := c.index_db.QueryRowContext(ctx, "SELECT IFNULL(SUM(size), 0) AS size FROM blobcache WHERE bucket = ?", c.bucket_hash)
	err := row.Scan(&size)

	if err != nil {
		logger.Error("Failed to determine total size of items in index", "error", err)
		return err
	}

	if size <= c.max_size {
		logger.Debug("Cache is still within max size limits", "size", size, "max size", c.max_size)
		return nil
	}

	for size > c.max_size {

		logger.Debug("Cache exceeds max size limits", "size", size, "max size", c.max_size)

		stopFunc := func(ctx context.Context, obj_key string, obj_size int64) bool {

			size -= obj_size

			if size < c.max_size {
				// Stop pruning
				logger.Debug("Cache no longer exceeds max size limits", "size", size, "max size", c.max_size)
				return true
			}

			return false
		}

		count, err := c.removeObjects(ctx, stopFunc, "SELECT key, size FROM blobcache WHERE bucket = ? ORDER BY accessed ASC, modtime ASC LIMIT 100", c.bucket_hash)

		if err != nil {
			logger.Error("Failed to prune objects", "error", err)
			return err
		}

		// This should never happen but computers, amirite?
		if count == 0 {
			break
		}
	}

	return nil
}

// removeObjects deletes objects from the cache based on the supplied
// query.  The stopFunc callback can be used to abort the loop early
// (for example, when the size limit has been met).  The function
// returns the number of objects it attempted to delete.
func (c *BlobCache) removeObjects(ctx context.Context, stopFunc removeObjectsStopFunc, q string, args ...any) (int, error) {

	logger := slog.Default()

	objects := make(map[string]int64)

	rows, err := c.index_db.QueryContext(ctx, q, args...)

	if err != nil {
		logger.Error("Failed to select keys for pruning", "q", q, "error", err)
		return 0, err
	}

	defer rows.Close()

	for rows.Next() {

		var key string
		var sz int64

		err := rows.Scan(&key, &sz)

		if err != nil {
			logger.Error("Failed to scan key for pruning", "error", err)
			return len(objects), err
		}

		objects[key] = sz
	}

	err = rows.Close()

	if err != nil {
		logger.Error("Failed to close rows after pruning", "error", err)
		return len(objects), err
	}

	err = rows.Err()

	if err != nil {
		logger.Error("Rows reported an error after pruning", "error", err)
		return len(objects), err
	}

	for key, sz := range objects {

		// logger.Debug("Remove key from cache", "key", key)

		exists, err := c.bucket.Exists(ctx, key)

		if err != nil {
			logger.Warn("Failed to determine if key exists", "key", key, "error", err)
			continue
		}

		if exists {

			err = c.bucket.Delete(ctx, key)

			if err != nil {
				logger.Error("Failed to delete stale object", "key", key, "error", err)
			} else {
				logger.Debug("Key removed from cache", "key", key)
			}

		} else {
			logger.Debug("Key not found in cache", "key", key)
		}

		err = c.removeFromIndex(ctx, key)

		if err != nil {
			logger.Error("Failed to delete stale object from index", "key", key, "error", err)
		}

		if stopFunc(ctx, key, sz) {
			logger.Debug("stopFunc returned true, stopping")
			break
		}
	}

	return len(objects), nil
}

func (c *BlobCache) walkCache(ctx context.Context, objects_map *sync.Map, size_ch chan int64, count_ch chan int64) error {

	logger := slog.Default()
	logger.Debug("Walk cache")

	t1 := time.Now()

	defer func() {
		logger.Debug("Time to walk cache", "time", time.Since(t1))
	}()

	wg := new(sync.WaitGroup)

	li := c.bucket.List(nil)
	iter, err_fn := li.All(ctx)

	for obj, _ := range iter {

		if obj.IsDir {
			continue
		}

		wg.Go(func() {

			logger := slog.Default()
			logger = logger.With("key", obj.Key)

			// logger.Debug("Process object")

			now := time.Now()
			ts := obj.ModTime.Unix()

			// Confirm whether object already in index so we don't have
			// to add it again below

			_, exists := objects_map.LoadAndDelete(obj.Key)

			if now.Unix()-c.max_age > ts {

				logger.Debug("Key triggers max age, schedule for removal", "mod time", obj.ModTime)

				wg.Go(func() {

					ctx := context.Background()
					err := c.bucket.Delete(ctx, obj.Key)

					if err != nil {
						logger.Error("Failed to delete stale object", "error", err)
						return
					}

					err = c.removeFromIndex(ctx, obj.Key)

					if err != nil {
						logger.Error("Failed to delete object from blobcache", "error", err)
						return
					}
				})

				return
			}

			if !exists {

				// logger.Debug("Add object to index")

				wg.Go(func() {

					err := c.addToIndex(ctx, obj.Key, obj.Size, ts)

					if err != nil {
						logger.Error("Failed to insert object in to index", "error", err)
					}
				})
			}

			size_ch <- obj.Size
			count_ch <- int64(1)
		})
	}

	wg.Wait()

	err := err_fn()

	if err != nil {
		logger.Error("Bucket iterator returned an error", "error", err)
		return err
	}

	return nil
}
