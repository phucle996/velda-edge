package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/phucle/velda-edge/control-plane/infra"
)

func main() {
	log.Println("Starting Velda Edge Control Plane...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Init Database Connection
	dbCfg := infra.PostgresConfig{
		Host:     getEnv("DB_HOST", "127.0.0.1"),
		Port:     5432,
		User:     getEnv("DB_USER", "postgres"),
		Password: getEnv("DB_PASSWORD", "postgres"),
		Database: getEnv("DB_NAME", "velda_edge"),
		SSLMode:  getEnv("DB_SSLMODE", "disable"),
	}

	_ = dbCfg
	// pool, err := infra.NewPostgresPool(ctx, dbCfg)
	// if err != nil {
	// 	log.Printf("PostgreSQL not connected (starting in standalone dev mode): %v", err)
	// } else {
	// 	defer pool.Close()
	// }

	// 2. Setup Gin Router
	r := gin.Default()
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "healthy",
			"time":   time.Now().UTC(),
		})
	})

	api := r.Group("/api/v1")
	{
		api.GET("/ping", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"message": "velda control-plane ready"})
		})
	}

	port := getEnv("PORT", "8080")
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: r,
	}

	go func() {
		log.Printf("HTTP Server listening on :%s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %s\n", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down Control Plane gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Control Plane exited")
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
