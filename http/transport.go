package http

// https://gist.github.com/zdebra/10f0e284c4672e99f0cb767298f20c11

import (
	net_http "net/http"
	"time"

	"golang.org/x/time/rate"
)

// ThrottledTransport Rate Limited HTTP Client
type ThrottledTransport struct {
	roundTripperWrap net_http.RoundTripper
	ratelimiter      *rate.Limiter
}

func (c *ThrottledTransport) RoundTrip(r *net_http.Request) (*net_http.Response, error) {
	err := c.ratelimiter.Wait(r.Context())
	if err != nil {
		return nil, err
	}
	return c.roundTripperWrap.RoundTrip(r)
}

// NewThrottledTransport wraps transportWrap with a rate limitter
func NewThrottledTransport(limitPeriod time.Duration, requestCount int, transportWrap net_http.RoundTripper) net_http.RoundTripper {
	return &ThrottledTransport{
		roundTripperWrap: transportWrap,
		ratelimiter:      rate.NewLimiter(rate.Every(limitPeriod), requestCount),
	}
}
