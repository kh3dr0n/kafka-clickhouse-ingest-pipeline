package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	"go-kafka-ingest/internal/api"
	"go-kafka-ingest/internal/auth"
	"go-kafka-ingest/internal/config"
	"go-kafka-ingest/internal/kafka"
	"go-kafka-ingest/internal/storage"
)

func main() {
	log.Println("Starting application...")
	// Load Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Setup Database Connection
	db, err := storage.NewDB(cfg.PostgresURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close() // Ensure DB pool is closed on exit

	// Ping DB to ensure connectivity
	if err := db.Ping(context.Background()); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Setup Kafka Publisher
	publisher, err := kafka.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		log.Fatalf("Failed to create Kafka publisher: %v", err)
	}
	// *** IMPORTANT: This defer ensures Close is called on normal exit, panic, OR after graceful shutdown sequence completes ***
	defer func() {
		log.Println("Executing deferred Kafka publisher close...")
		if err := publisher.Close(); err != nil {
			log.Printf("Error during deferred Kafka publisher close: %v", err)
		} else {
			log.Println("Deferred Kafka publisher close completed.")
		}
	}()

	// Setup Authenticator
	authenticator := auth.NewPostgresAuthenticator(db.Pool, cfg.APIKeyTableName)

	// Setup API Handler
	apiHandler := api.NewAPIHandler(authenticator, publisher)

	// Setup Router
	router := mux.NewRouter()
	router.HandleFunc("/ingest", apiHandler.IngestHandler).Methods(http.MethodPost)
	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	}).Methods(http.MethodGet)

	// Setup HTTP Server
	server := &http.Server{
		Addr:         cfg.ListenAddress,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start Server in a Goroutine
	go func() {
		log.Printf("Server starting on %s", cfg.ListenAddress)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s: %v\n", cfg.ListenAddress, err)
		}
	}()

	// --- Graceful Shutdown Handling ---
	quit := make(chan os.Signal, 1)
	// Notify about SIGINT (Ctrl+C) and SIGTERM (sent by Docker/Kubernetes)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Application started. Waiting for shutdown signal...")

	// Block until a signal is received
	sig := <-quit
	log.Printf("Received signal: %s. Starting graceful shutdown...", sig)

	// Create a context with timeout for shutdown
	// Give enough time for active requests AND Kafka flushing
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 35*time.Second) // Increased timeout slightly
	defer cancel()

	// Attempt graceful shutdown of the HTTP server (stops accepting new requests, waits for active ones)
	log.Println("Shutting down HTTP server...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	} else {
		log.Println("HTTP server gracefully stopped.")
	}

	// *** Publisher Close is handled by the defer statement above when main exits ***
	// No need to explicitly call publisher.Close() here again.

	log.Println("Shutdown sequence complete. Exiting.")
}
