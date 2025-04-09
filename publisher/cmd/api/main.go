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
	defer db.Close()

	if err := db.Ping(context.Background()); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Setup Kafka Publisher
	publisher, err := kafka.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		log.Fatalf("Failed to create Kafka publisher: %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("Error closing Kafka publisher: %v", err)
		}
	}()

	// Setup Authenticator Chain
	// 1. Create the base authenticator (Postgres)
	var baseAuthenticator auth.Authenticator
	baseAuthenticator = auth.NewPostgresAuthenticator(db.Pool, cfg.APIKeyTableName)

	// 2. Optionally wrap with the Caching Authenticator
	var finalAuthenticator auth.Authenticator
	if cfg.AuthCacheEnabled {
		cachingAuth, err := auth.NewCachingAuthenticator(baseAuthenticator, cfg.AuthCacheSize, cfg.AuthCacheTTL)
		if err != nil {
			// Decide how to handle cache init errors: log and continue without cache, or fail fast?
			log.Printf("WARNING: Failed to initialize auth cache: %v. Proceeding without caching.", err)
			finalAuthenticator = baseAuthenticator // Fallback to base
		} else {
			finalAuthenticator = cachingAuth // Use the caching layer
		}
	} else {
		log.Println("API key auth cache is disabled via configuration.")
		finalAuthenticator = baseAuthenticator // Caching explicitly disabled
	}

	// Setup API Handler (pass the final authenticator, which might be cached or not)
	apiHandler := api.NewAPIHandler(finalAuthenticator, publisher)

	// Setup Router
	router := mux.NewRouter()
	router.HandleFunc("/ingest", apiHandler.IngestHandler).Methods(http.MethodPost)
	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	}).Methods(http.MethodGet)

	// Setup HTTP Server (rest is the same)
	server := &http.Server{
		Addr:         cfg.ListenAddress,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("Server starting on %s", cfg.ListenAddress)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s: %v\n", cfg.ListenAddress, err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting")
}
