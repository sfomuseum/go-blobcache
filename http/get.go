// Package http provides helpers for downloading resources over HTTP
// while transparently caching them in a BlobCache.
package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	net_http "net/http"

	"github.com/sfomuseum/go-blobcache"
	"github.com/whosonfirst/go-ioutil"
)

var http_cl = net_http.Client{}

// GetWithCacheOptions specifies optional behaviour for GetWithCache.
type GetWithCacheOptions struct {
	// CheckLastModTime indicates whether the function should perform an HTTP HEAD
	// request to determine if the cached copy is still up‑to‑date.
	CheckLastModTime bool
}

// GetWithCache attempts to fetch a resource identified by uri from the
// supplied BlobCache.  If the resource is not cached or the cached copy
// is stale, it is fetched from the source and stored in the cache.
func GetWithCache(ctx context.Context, c *blobcache.BlobCache, uri string) (io.ReadSeekCloser, error) {

	opts := &GetWithCacheOptions{
		CheckLastModTime: true,
	}

	return GetWithCacheAndOptions(ctx, c, uri, opts)
}

// GetWithCacheAndOptions behaves like GetWithCache but accepts a
// GetWithCacheOptions struct to control optional behaviour.
func GetWithCacheAndOptions(ctx context.Context, c *blobcache.BlobCache, uri string, opts *GetWithCacheOptions) (io.ReadSeekCloser, error) {

	logger := slog.Default()
	logger = logger.With("uri", uri, "check lastmod", opts.CheckLastModTime)

	r, err := c.Get(ctx, uri)

	if err != nil && err != blobcache.CacheMiss {
		logger.Debug("Failed to retrieve from cache", "error", err)
		return nil, fmt.Errorf("Failed to retrieve cache item, %w", err)
	}

	if err != nil && err == blobcache.CacheMiss {
		logger.Debug("Cache miss, fetch from source")
		return getWithCache(ctx, c, uri, r)
	}

	attrs, err := c.Attributes(ctx, uri)

	if err != nil {
		logger.Debug("Failed to determine attributes, fetch from source", "error", err)
		return getWithCache(ctx, c, uri, r)
	}

	if opts.CheckLastModTime {

		cache_t := attrs.ModTime

		req, err := net_http.NewRequestWithContext(ctx, net_http.MethodHead, uri, nil)

		if err != nil {
			logger.Debug("Failed to create request, fetch from source", "error", err)
			return getWithCache(ctx, c, uri, r)
		}

		rsp, err := http_cl.Do(req)

		if err != nil {
			logger.Debug("Failed to complete request, fetch from source", "error", err)
			return getWithCache(ctx, c, uri, r)
		}

		source_lastmod := rsp.Header.Get("Last-Modified")

		source_t, err := net_http.ParseTime(source_lastmod)

		if err != nil {
			logger.Debug("Failed to parse last mod header", "last mod", source_lastmod, "error", err)
			return getWithCache(ctx, c, uri, r)
		}

		if cache_t.Before(source_t) {
			logger.Debug("Source has been modified, fecth from source", "cache", cache_t, "source", source_t)
			return getWithCache(ctx, c, uri, r)
		}

	}

	logger.Debug("Return from cache")
	return r, nil
}

func getWithCache(ctx context.Context, c *blobcache.BlobCache, uri string, r io.ReadSeekCloser) (io.ReadSeekCloser, error) {

	r2, err := fetchFromSource(ctx, c, uri)

	if err != nil {
		logger := slog.Default()
		logger.Warn("Failed to fetch from source, returning cache", "uri", uri, "error", err)

		if r == nil {
			logger.Error("Failed to retrieve from cache AND source")
			return nil, fmt.Errorf("Failed to retrieve from cache AND source")
		}

		return r, nil
	}

	return r2, nil
}

// fetchFromSource downloads a resource from uri using an HTTP GET request,
// stores the response body in the cache, and returns a ReadSeekCloser
// for the downloaded data.
func fetchFromSource(ctx context.Context, c *blobcache.BlobCache, uri string) (io.ReadSeekCloser, error) {

	logger := slog.Default()
	logger = logger.With("uri", uri)

	req, err := net_http.NewRequestWithContext(ctx, net_http.MethodGet, uri, nil)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new HTTP request, %w", err)
	}

	rsp, err := http_cl.Do(req)

	if err != nil {
		return nil, fmt.Errorf("Failed to execute HTTP request, %w", err)
	}

	defer rsp.Body.Close()

	if rsp.StatusCode != net_http.StatusOK {
		return nil, fmt.Errorf("Request failed %s", rsp.Status)
	}

	body, err := io.ReadAll(rsp.Body)

	if err != nil {
		return nil, fmt.Errorf("Failed to read HTTP response, %w", err)
	}

	br := bytes.NewReader(body)
	rsc, err := ioutil.NewReadSeekCloser(br)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new ReadSeekCloser, %w", err)
	}

	err = c.Set(ctx, uri, rsc)

	if err != nil {
		return nil, fmt.Errorf("Failed to set cache item, %w", err)
	}

	_, err = rsc.Seek(0, 0)

	if err != nil {
		return nil, fmt.Errorf("Failed to rewind ReadSeekCloser, %w", err)
	}

	logger.Debug("Return from source")
	return rsc, nil
}
