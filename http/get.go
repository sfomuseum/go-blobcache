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

// GetWithCacheOptions specifies optional behaviour for GetWithCache.
type GetWithCacheOptions struct {
	// CheckLastModTime indicates whether the function should perform an HTTP HEAD
	// request to determine if the cached copy is still up‑to‑date.
	CheckLastModTime bool
	// Client is the http.Client instance to use when fetching resources.
	Client *net_http.Client
	// Optional user-agent string to include with HTTP requests.
	UserAgent string
	// The BlobCache instance used to store and retrieve cached items.
	BlobCache *blobcache.BlobCache
}

// GetWithCache attempts to fetch a resource identified by uri from the
// supplied BlobCache returning a []byte array. If the resource is not cached or the
// cached copy is stale, it is fetched from the source and stored in the cache.
func GetBytesWithCache(ctx context.Context, c *blobcache.BlobCache, uri string) ([]byte, error) {

	r, err := GetWithCache(ctx, c, uri)

	if err != nil {
		return nil, err
	}

	defer r.Close()
	return io.ReadAll(r)
}

// GetWithCache attempts to fetch a resource identified by uri from the
// supplied BlobCache.  If the resource is not cached or the cached copy
// is stale, it is fetched from the source and stored in the cache.
func GetWithCache(ctx context.Context, c *blobcache.BlobCache, uri string) (io.ReadSeekCloser, error) {

	opts := &GetWithCacheOptions{
		CheckLastModTime: true,
		Client:           NewClient(),
		BlobCache:        c,
	}

	return GetWithCacheAndOptions(ctx, opts, uri)
}

// GetWithCache attempts to fetch a resource identified by uri from the
// supplied BlobCache return a []byte array. If the resource is not cached or the
// cached copy is stale, it is fetched from the source and stored in the cache.
func GetBytesWithCacheAndOptions(ctx context.Context, opts *GetWithCacheOptions, uri string) ([]byte, error) {

	r, err := GetWithCacheAndOptions(ctx, opts, uri)

	if err != nil {
		return nil, err
	}

	defer r.Close()
	return io.ReadAll(r)
}

// GetWithCacheAndOptions behaves like GetWithCache but accepts a
// GetWithCacheOptions struct to control optional behaviour.
func GetWithCacheAndOptions(ctx context.Context, opts *GetWithCacheOptions, uri string) (io.ReadSeekCloser, error) {

	logger := slog.Default()
	logger = logger.With("uri", uri, "check lastmod", opts.CheckLastModTime)

	r, err := opts.BlobCache.Get(ctx, uri)

	if err != nil && err != blobcache.CacheMiss {
		logger.Debug("Failed to retrieve from cache", "error", err)
		return nil, fmt.Errorf("Failed to retrieve cache item, %w", err)
	}

	if err != nil && err == blobcache.CacheMiss {
		logger.Debug("Cache miss, fetch from source")
		return getWithCache(ctx, opts, uri, r)
	}

	attrs, err := opts.BlobCache.Attributes(ctx, uri)

	if err != nil {
		logger.Debug("Failed to determine attributes, fetch from source", "error", err)
		return getWithCache(ctx, opts, uri, r)
	}

	if opts.CheckLastModTime {

		cache_t := attrs.ModTime

		req, err := newRequest(ctx, opts, net_http.MethodHead, uri)

		if err != nil {
			logger.Debug("Failed to create request, fetch from source", "error", err)
			return getWithCache(ctx, opts, uri, r)
		}

		rsp, err := opts.Client.Do(req)

		if err != nil {
			logger.Debug("Failed to complete request, fetch from source", "error", err)
			return getWithCache(ctx, opts, uri, r)
		}

		source_lastmod := rsp.Header.Get("Last-Modified")

		source_t, err := net_http.ParseTime(source_lastmod)

		if err != nil {
			logger.Debug("Failed to parse last mod header", "last mod", source_lastmod, "error", err)
			return getWithCache(ctx, opts, uri, r)
		}

		if cache_t.Before(source_t) {
			logger.Debug("Source has been modified, fecth from source", "cache", cache_t, "source", source_t)
			return getWithCache(ctx, opts, uri, r)
		}

	}

	logger.Debug("Return from cache")
	return r, nil
}

func getWithCache(ctx context.Context, opts *GetWithCacheOptions, uri string, r io.ReadSeekCloser) (io.ReadSeekCloser, error) {

	r2, err := fetchFromSource(ctx, opts, uri)

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
func fetchFromSource(ctx context.Context, opts *GetWithCacheOptions, uri string) (io.ReadSeekCloser, error) {

	logger := slog.Default()
	logger = logger.With("uri", uri)
	logger = logger.With("user agent", opts.UserAgent)

	req, err := newRequest(ctx, opts, net_http.MethodGet, uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new HTTP request, %w", err)
	}

	rsp, err := opts.Client.Do(req)

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

	err = opts.BlobCache.Set(ctx, uri, rsc)

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

func newRequest(ctx context.Context, opts *GetWithCacheOptions, http_method string, uri string) (*net_http.Request, error) {

	req, err := net_http.NewRequestWithContext(ctx, http_method, uri, nil)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new HTTP request, %w", err)
	}

	if opts.UserAgent == "" {

		err := AssignRandomUserAgent(req)

		if err != nil {
			slog.Warn("Failed to assign random user agent", "error", err)
		}
	}

	// slog.Debug("New request", "uri", uri, "ua", req.Header.Get("User-Agent"))
	return req, nil
}
