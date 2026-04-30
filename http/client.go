package http

import (
	net_http "net/http"
	"time"
)

func NewClient() *net_http.Client {

	limit := 1 * time.Second
	requests := 100
	return NewClientWithLimits(limit, requests)
}

func NewClientWithLimits(limit time.Duration, requests int) *net_http.Client {

	cl := net_http.DefaultClient
	cl.Transport = NewThrottledTransport(limit, requests, net_http.DefaultTransport)
	return cl
}
