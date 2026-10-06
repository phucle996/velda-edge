package tcp

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"velda-e2e-backend/internal/metrics"
)

// Server provides high-performance raw L4 TCP echo serving.
type Server struct {
	port     int
	metrics  *metrics.Tracker
	listener net.Listener
	wg       sync.WaitGroup
}

// NewServer creates a new TCP echo server.
func NewServer(port int, tracker *metrics.Tracker) *Server {
	return &Server{
		port:    port,
		metrics: tracker,
	}
}

// Start begins listening and serving TCP echo connections.
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind tcp listener on %s: %w", addr, err)
	}
	s.listener = ln
	log.Printf("[E2E Backend] Raw L4 TCP Echo listening on %s", addr)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	s.metrics.IncConns()
	defer s.metrics.DecConns()

	buf := make([]byte, 16384)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			s.metrics.AddBytesIn(uint64(n))
			wn, werr := conn.Write(buf[:n])
			if wn > 0 {
				s.metrics.AddBytesOut(uint64(wn))
			}
			if werr != nil {
				break
			}
		}
		if err != nil {
			if err != io.EOF {
				// connection reset or closed
			}
			break
		}
	}
}

// Stop waits for active connections to finish or closes them.
func (s *Server) Stop() {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
}
