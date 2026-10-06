package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"velda-e2e-backend/internal/config"
	"velda-e2e-backend/internal/metrics"
	"velda-e2e-backend/internal/transport/http1"
	"velda-e2e-backend/internal/transport/http2"
	"velda-e2e-backend/internal/transport/tcp"
)

// App is the unified upstream backend server application.
type App struct {
	cfg        *config.Config
	metrics    *metrics.Tracker
	httpServer *http.Server
	h2Server   *http.Server
	tcpServer  *tcp.Server
}

// New constructs an App instance.
func New(cfg *config.Config) *App {
	tracker := metrics.NewTracker()

	h1Handler := http1.NewRouter(tracker)
	h2Handler := http2.NewHandler(h1Handler)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      h1Handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	h2Server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.H2Port),
		Handler:      h2Handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	tcpServer := tcp.NewServer(cfg.TCPPort, tracker)

	return &App{
		cfg:        cfg,
		metrics:    tracker,
		httpServer: httpServer,
		h2Server:   h2Server,
		tcpServer:  tcpServer,
	}
}

// Run starts all transport servers and blocks until OS termination signals.
func (a *App) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup

	// 1. Start HTTP/1.1 Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("[E2E Backend] HTTP/1.1 upstream listening on :%d", a.cfg.HTTPPort)
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[E2E Backend] HTTP/1.1 server error: %v", err)
		}
	}()

	// 2. Start HTTP/2 H2C Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("[E2E Backend] HTTP/2 H2C upstream listening on :%d", a.cfg.H2Port)
		if err := a.h2Server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[E2E Backend] HTTP/2 H2C server error: %v", err)
		}
	}()

	// 3. Start Raw L4 TCP Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.tcpServer.Start(ctx); err != nil {
			log.Printf("[E2E Backend] TCP server error: %v", err)
		}
	}()

	log.Printf("[E2E Backend] All upstream transports initialized successfully.")

	// Wait for OS shutdown signal
	sig := <-sigChan
	log.Printf("[E2E Backend] Received signal %v, initiating graceful shutdown...", sig)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	_ = a.httpServer.Shutdown(shutdownCtx)
	_ = a.h2Server.Shutdown(shutdownCtx)
	a.tcpServer.Stop()

	wg.Wait()
	log.Printf("[E2E Backend] All servers stopped cleanly.")
	return nil
}
