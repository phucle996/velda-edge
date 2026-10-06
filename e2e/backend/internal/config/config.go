package config

import (
	"flag"
	"time"
)

// Config encapsulates configuration for the E2E upstream backend servers.
type Config struct {
	HTTPPort     int
	H2Port       int
	TCPPort      int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// ParseFlags parses CLI flags and returns a populated Config.
func ParseFlags() *Config {
	httpPort := flag.Int("http-port", 8081, "HTTP/1.1 upstream server port")
	h2Port := flag.Int("h2-port", 8082, "HTTP/2 H2C upstream server port")
	tcpPort := flag.Int("tcp-port", 19001, "Raw L4 TCP Echo upstream server port")

	flag.Parse()

	return &Config{
		HTTPPort:     *httpPort,
		H2Port:       *h2Port,
		TCPPort:      *tcpPort,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}
