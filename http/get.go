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

func GetWithCache(ctx context.Context, c *blobcache.BlobCache, uri string) (io.ReadSeekCloser, error) {

	logger := slog.Default()
	logger = logger.With("uri", uri)

	r, err := c.Get(ctx, uri)

	if err != nil && err != blobcache.CacheMiss {
		logger.Debug("Failed to retrieve from cache", "error", err)
		return nil, err
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

	logger.Debug("Return from cache")
	return r, nil
}

func getWithCache(ctx context.Context, c *blobcache.BlobCache, uri string, r io.ReadSeekCloser) (io.ReadSeekCloser, error) {

	r2, err := fetchFromSource(ctx, c, uri)

	if err != nil {
		logger := slog.Default()
		logger.Warn("Failed to fetch from source, returning cache", "uri", uri, "error", err)
		return r, nil
	}

	return r2, nil
}

func fetchFromSource(ctx context.Context, c *blobcache.BlobCache, uri string) (io.ReadSeekCloser, error) {

	logger := slog.Default()
	logger = logger.With("uri", uri)

	req, err := net_http.NewRequestWithContext(ctx, net_http.MethodGet, uri, nil)

	if err != nil {
		return nil, err
	}

	rsp, err := http_cl.Do(req)

	if err != nil {
		return nil, err
	}

	defer rsp.Body.Close()

	if rsp.StatusCode != net_http.StatusOK {
		return nil, fmt.Errorf("Request failed %s", rsp.Status)
	}

	body, err := io.ReadAll(rsp.Body)

	if err != nil {
		return nil, err
	}

	br := bytes.NewReader(body)
	rsc, err := ioutil.NewReadSeekCloser(br)

	if err != nil {
		return nil, err
	}

	err = c.Set(ctx, uri, rsc)

	if err != nil {
		return nil, err
	}

	_, err = rsc.Seek(0, 0)

	if err != nil {
		return nil, err
	}

	logger.Debug("Return from source")
	return rsc, nil
}
