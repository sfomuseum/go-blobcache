package blobcache

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/sfomuseum/go-blobcache"
	"github.com/sfomuseum/go-flags/flagset"
)

func Run(ctx context.Context) error {
	fs := DefaultFlagSet()
	return RunWithFlagSet(ctx, fs)
}

func RunWithFlagSet(ctx context.Context, fs *flag.FlagSet) error {

	flagset.Parse(fs)

	if verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Debug("Verbose logging enabled")
	}

	c, err := blobcache.NewBlobCache(ctx, cache_uri)

	if err != nil {
		return err
	}

	defer c.Close()

	switch action {
	case "get":

		r, err := c.Get(ctx, key)

		if err != nil {
			return err
		}

		_, err = io.Copy(os.Stdout, r)

		if err != nil {
			return err
		}

	case "set":

		r := strings.NewReader(data)

		err = c.Set(ctx, key, r)

		if err != nil {
			return err
		}

	case "unset":

		err := c.Unset(ctx, key)

		if err != nil {
			return err
		}

	case "index":

		err := c.Index(ctx)
		
		if err != nil {
			return err
		}

	case "prune":
		
		err := c.Prune(ctx)

		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("Invalid or unsupported action")
	}

	return nil
}
