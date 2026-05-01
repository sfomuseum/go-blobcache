package http

// https://gist.github.com/zdebra/10f0e284c4672e99f0cb767298f20c11

import (
	net_http "net/http"
	"time"

	"golang.org/x/time/rate"
)

// ThrottledTransport is a RoundTripper that limits the rate at which
// requests are sent.
//
// It wraps another RoundTripper (the transport that actually performs
// the network I/O) and uses a *rate.Limiter to enforce a maximum
// request rate.  The limiter is safe for concurrent use, so a
// ThrottledTransport can be shared by many goroutines.
type ThrottledTransport struct {
	roundTripperWrap net_http.RoundTripper
	ratelimiter      *rate.Limiter
}

// RoundTrip implements the net/http.RoundTripper interface.
// Before delegating to the wrapped transport it waits until the
// rate limiter permits the request.  If the context of the request
// is cancelled or times out while waiting, an error is returned.
func (c *ThrottledTransport) RoundTrip(r *net_http.Request) (*net_http.Response, error) {

	err := c.ratelimiter.Wait(r.Context())

	if err != nil {
		return nil, err
	}

	return c.roundTripperWrap.RoundTrip(r)
}

// NewThrottledTransport returns a RoundTripper that throttles requests
// according to the supplied rate limiter configuration.
//
//	limitPeriod  – the interval between the generation of individual
//	               tokens (e.g. 100 ms).  Tokens are generated at a
//	               steady rate of 1/limitPeriod.
//	requestCount – the maximum burst size; up to this many requests can
//	               be sent immediately before the limiter starts to
//	               block.  Think of it as the “burst” capacity of the
//	               limiter.
//	transportWrap – the underlying RoundTripper that actually
//	               performs the network I/O.  If nil, http.DefaultTransport
//	               is used.
//
// The returned value implements net/http.RoundTripper and can be
// assigned to http.Client.Transport.
func NewThrottledTransport(limitPeriod time.Duration, requestCount int, transportWrap net_http.RoundTripper) net_http.RoundTripper {
	return &ThrottledTransport{
		roundTripperWrap: transportWrap,
		ratelimiter:      rate.NewLimiter(rate.Every(limitPeriod), requestCount),
	}
}
