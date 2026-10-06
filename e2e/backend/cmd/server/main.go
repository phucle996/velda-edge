package main

import (
	"log"

	"velda-e2e-backend/internal/config"
	"velda-e2e-backend/internal/server"
)

func main() {
	cfg := config.ParseFlags()
	app := server.New(cfg)

	if err := app.Run(); err != nil {
		log.Fatalf("[E2E Backend] Fatal server error: %v", err)
	}
}
