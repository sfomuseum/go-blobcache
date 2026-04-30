package blobcache

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	sfom_sql "github.com/sfomuseum/go-database/sql"
)

func setupBlobCacheIndex(ctx context.Context, index_dsn string) (*sql.DB, error) {

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

		_, err := index_db.ExecContext(ctx, "CREATE TABLE blobcache (key TEXT PRIMARY KEY, size INTEGER, modtime INTEGER)")

		if err != nil {
			return nil, fmt.Errorf("Failed to create blobcache table, %w", err)
		}
	}

	return index_db, err
}

func (c *BlobCache) addToIndex(ctx context.Context, key string, size int64, modtime int64) error {

	_, err := c.index_db.ExecContext(ctx, "INSERT OR REPLACE INTO blobcache (key, size, modtime) VALUES(?, ?, ?)", key, size, modtime)
	return err
}

func (c *BlobCache) removeFromIndex(ctx context.Context, key string) error {

	_, err := c.index_db.ExecContext(ctx, "DELETE from blobcache WHERE key = ?", key)
	return err
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

			wg.Go(func(){
				
				err := c.addToIndex(ctx, obj.Key, obj.Size, ts)
				
				if err != nil {
					logger.Error("Failed to insert object in to index", "key", obj.Key, "error", err)
				}
			})
			
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

	if c.pruning.Load() == true {
		logger.Debug("Pruning is already in progress.")
		return nil
	}

	c.pruning.Store(true)
	defer c.pruning.Store(false)

	var size int64

	row := c.index_db.QueryRowContext(ctx, "SELECT IFNULL(SUM(size), 0) AS size FROM blobcache")
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

	for size > c.max_size {

		objects := make(map[string]int64)

		rows, err := c.index_db.QueryContext(ctx, "SELECT key, size FROM blobcache ORDER BY modtime ASC LIMIT 100")

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

			objects[key] = sz
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

		for key, sz := range objects {

			// logger.Debug("Remove key from cache", "key", key)

			exists, err := c.bucket.Exists(ctx, key)

			if err != nil {
				//
			}

			if exists {

				err = c.bucket.Delete(ctx, key)

				if err != nil {
					logger.Error("Failed to delete stale object", "key", key, "error", err)
				} else {
					logger.Debug("Key removed from cache", "key", key)
					size -= sz
				}

			} else {
				logger.Debug("Key not found in cache", "key", key)
			}

			err = c.removeFromIndex(ctx, key)

			if err != nil {
				logger.Error("Failed to delete stale object from index", "key", key, "error", err)
			}

			if size < c.max_size {
				logger.Debug("Cache now fits size limits", "size", size)
				break
			}
		}
	}

	return nil
}
