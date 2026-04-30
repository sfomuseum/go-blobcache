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

	fs.StringVar(&cache_uri, "cache-uri", "mem://", "A valid blobcache.BlobCache URI constructor.")
	fs.StringVar(&action, "action", "", "Valid actions are: get, set, unset, index, prune.")
	fs.StringVar(&key, "key", "", "The name of the key to access from the blobcache. This flag is ignored if -action is \"index\" or \"prune\".")
	fs.StringVar(&data, "data", "", "The data to store in the blobcache. This flag is ignored unless -action is \"set\".")

	fs.BoolVar(&verbose, "verbose", false, "Enable verbose (debug) logging.")
	return fs
}
