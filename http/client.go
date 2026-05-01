package http

import (
	net_http "net/http"
	"time"
)

// NewClient returns a new HTTP client configured with a default
// request‑throttling limit: a maximum of 100 requests are allowed
// per 1‑second window.  It is a convenience wrapper around
// NewClientWithLimits that supplies these default values.
func NewClient() *net_http.Client {

	limit := 1 * time.Second
	requests := 100
	return NewClientWithLimits(limit, requests)
}

// NewClientWithLimits creates and returns a new HTTP client whose
// Transport enforces a limit on the rate of outgoing requests.
// The limit is specified by the duration 'limit', which represents
// the time window over which at most 'requests' requests may be
// sent.
func NewClientWithLimits(limit time.Duration, requests int) *net_http.Client {

	cl := &net_http.Client{}
	cl.Transport = NewThrottledTransport(limit, requests, net_http.DefaultTransport)
	return cl
}
