package blobcache

import (
	"flag"

	"github.com/sfomuseum/go-flags/flagset"
)

var cache_uri string
var action string
var key string
var data string
var verbose bool

func DefaultFlagSet() *flag.FlagSet {

	fs := flagset.NewFlagSet("cache")

	fs.StringVar(&cache_uri, "cache-uri", "mem://", "...")
	fs.StringVar(&action, "action", "", "...")
	fs.StringVar(&key, "key", "", "...")
	fs.StringVar(&data, "data", "", "...")

	fs.BoolVar(&verbose, "verbose", false, "Enable verbose (debug) logging.")
	return fs
}
