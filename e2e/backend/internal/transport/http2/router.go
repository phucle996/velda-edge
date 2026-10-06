package http2

import (
	"net/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// NewHandler wraps an HTTP handler with cleartext HTTP/2 (H2C) capability.
func NewHandler(baseHandler http.Handler) http.Handler {
	h2Server := &http2.Server{
		MaxConcurrentStreams: 256,
	}
	return h2c.NewHandler(baseHandler, h2Server)
}
