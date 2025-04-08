Okay, let's build this Go service step-by-step, including the Docker setup and tests.

**Project Structure:**

```
.
├── cmd
│   └── api
│       └── main.go          # Main application entry point
├── internal
│   ├── api                  # HTTP handlers and routing
│   │   ├── handler.go
│   │   └── handler_test.go
│   ├── auth                 # API key authentication logic
│   │   ├── auth.go
│   │   └── auth_test.go
│   ├── config               # Configuration loading
│   │   └── config.go
│   ├── kafka                # Kafka publishing logic
│   │   ├── publisher.go
│   │   └── publisher_test.go  # (Mocked tests here)
│   └── storage              # Database interaction (simplified)
│       └── postgres.go
├── testdata                 # Test fixtures (e.g., sample JSON)
│   └── payload.json
├── migrations               # Simple SQL for DB setup
│   └── 001_create_api_keys.up.sql
├── Dockerfile               # Application Dockerfile
├── docker-compose.yml       # Docker Compose setup
├── go.mod
├── go.sum
└── README.md                # Instructions
```

---

**1. Go Module Initialization**

```bash
mkdir go-kafka-ingest
cd go-kafka-ingest
go mod init go-kafka-ingest # Replace with your actual module path if needed
```

---

**2. Dependencies**

```bash
go get github.com/gorilla/mux            # HTTP Router
go get github.com/jackc/pgx/v4/pgxpool # PostgreSQL Driver
go get github.com/segmentio/kafka-go   # Kafka Client
go get github.com/kelseyhightower/envconfig # Env var config loading
go get github.com/stretchr/testify/assert # Testing assertions
go get github.com/stretchr/testify/require # Testing requirements
go get github.com/stretchr/testify/mock   # Mocking library for tests
```

---

**3. Configuration (`internal/config/config.go`)**

```go
package config

import (
	"log"

	"github.com/kelseyhightower/envconfig"
)

// Config holds the application configuration
type Config struct {
	ListenAddress   string `envconfig:"LISTEN_ADDRESS" default:":8080"`
	PostgresURL     string `envconfig:"POSTGRES_URL" required:"true"`
	KafkaBrokers    string `envconfig:"KAFKA_BROKERS" required:"true"` // Comma-separated list
	KafkaTopic      string `envconfig:"KAFKA_TOPIC" required:"true"`
	APIKeyTableName string `envconfig:"API_KEY_TABLE_NAME" default:"api_keys"`
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}
	log.Printf("Configuration loaded: %+v", cfg) // Be careful logging sensitive info like URLs in production
	return &cfg, nil
}
```

---

**4. Database Interaction (`internal/storage/postgres.go`)**

```go
package storage

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

// DB represents the database connection pool.
type DB struct {
	Pool *pgxpool.Pool
}

// NewDB creates a new database connection pool.
func NewDB(databaseURL string) (*DB, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database URL: %w", err)
	}

	// Optional: Configure pool settings
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	log.Println("Database connection pool established")
	return &DB{Pool: pool}, nil
}

// Close closes the database connection pool.
func (db *DB) Close() {
	if db.Pool != nil {
		db.Pool.Close()
		log.Println("Database connection pool closed")
	}
}

// Ping checks the database connection.
func (db *DB) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return db.Pool.Ping(ctx)
}
```

---

**5. Authentication (`internal/auth/auth.go`)**

```go
package auth

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Authenticator defines the interface for API key validation.
type Authenticator interface {
	ValidateAPIKey(ctx context.Context, apiKey string) (bool, error)
}

// PostgresAuthenticator validates API keys against a PostgreSQL database.
type PostgresAuthenticator struct {
	dbPool    *pgxpool.Pool
	tableName string
}

// NewPostgresAuthenticator creates a new authenticator.
func NewPostgresAuthenticator(pool *pgxpool.Pool, tableName string) *PostgresAuthenticator {
	if tableName == "" {
		tableName = "api_keys" // Default table name
	}
	return &PostgresAuthenticator{dbPool: pool, tableName: tableName}
}

// ValidateAPIKey checks if the given API key exists and is active in the database.
func (a *PostgresAuthenticator) ValidateAPIKey(ctx context.Context, apiKey string) (bool, error) {
	if apiKey == "" {
		return false, nil // No key provided
	}

	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE api_key = $1 AND is_active = TRUE)", a.tableName)
	log.Printf("Executing auth query for key: %s...", apiKey[:min(5, len(apiKey))]) // Log prefix for security

	var exists bool
	err := a.dbPool.QueryRow(ctx, query, apiKey).Scan(&exists)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Printf("API key not found or inactive: %s...", apiKey[:min(5, len(apiKey))])
			return false, nil // Key does not exist or is not active
		}
		log.Printf("Error validating API key: %v", err)
		return false, fmt.Errorf("database error during API key validation: %w", err) // Internal error
	}

	if !exists {
		log.Printf("API key validation failed (key inactive or not found): %s...", apiKey[:min(5, len(apiKey))])
	} else {
		log.Printf("API key validated successfully: %s...", apiKey[:min(5, len(apiKey))])
	}
	return exists, nil
}

// Helper function to avoid index out of range
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

---

**6. Kafka Publisher (`internal/kafka/publisher.go`)**

```go
package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher defines the interface for publishing messages.
type Publisher interface {
	Publish(ctx context.Context, key []byte, value []byte) error
	Close() error
}

// KafkaPublisher sends messages to a Kafka topic.
type KafkaPublisher struct {
	writer *kafka.Writer
	topic  string
}

// NewKafkaPublisher creates a new Kafka publisher.
func NewKafkaPublisher(brokers string, topic string) (*KafkaPublisher, error) {
	brokerList := strings.Split(brokers, ",")
	if len(brokerList) == 0 || brokerList[0] == "" {
		return nil, fmt.Errorf("kafka brokers list is empty or invalid")
	}
	log.Printf("Connecting to Kafka brokers: %v", brokerList)

	// Configure the Kafka writer
	// Adjust settings based on your Kafka setup and requirements
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafka.RequireOne, // Or kafka.RequireAll for higher durability
		Async:        false,            // Set to true for higher throughput, handle errors differently
	}

	log.Printf("Kafka writer configured for topic '%s'", topic)
	return &KafkaPublisher{writer: writer, topic: topic}, nil
}

// Publish sends a message to the configured Kafka topic.
func (p *KafkaPublisher) Publish(ctx context.Context, key []byte, value []byte) error {
	msg := kafka.Message{
		Key:   key,   // Can be nil if partitioning is not key-based
		Value: value, // The JSON payload
	}

	log.Printf("Publishing message to Kafka topic '%s', size: %d bytes", p.topic, len(value))
	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("Failed to write message to Kafka: %v", err)
		return fmt.Errorf("kafka write error: %w", err)
	}
	log.Printf("Message successfully published to Kafka topic '%s'", p.topic)
	return nil
}

// Close closes the Kafka writer connection.
func (p *KafkaPublisher) Close() error {
	log.Println("Closing Kafka writer...")
	return p.writer.Close()
}
```

---

**7. HTTP Handler (`internal/api/handler.go`)**

```go
package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"go-kafka-ingest/internal/auth"
	"go-kafka-ingest/internal/kafka"
)

const apiKeyHeader = "X-API-Key"

// APIHandler holds dependencies for the API handlers.
type APIHandler struct {
	auth      auth.Authenticator
	publisher kafka.Publisher
}

// NewAPIHandler creates a new API handler instance.
func NewAPIHandler(a auth.Authenticator, p kafka.Publisher) *APIHandler {
	return &APIHandler{
		auth:      a,
		publisher: p,
	}
}

// IngestHandler handles incoming data ingestion requests.
func (h *APIHandler) IngestHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Check Method
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 2. Get API Key
	apiKey := r.Header.Get(apiKeyHeader)
	if apiKey == "" {
		log.Println("API key missing from header")
		http.Error(w, "Unauthorized: API key required", http.StatusUnauthorized)
		return
	}

	// 3. Authenticate API Key
	isValid, err := h.auth.ValidateAPIKey(r.Context(), apiKey)
	if err != nil {
		log.Printf("Error validating API key: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	if !isValid {
		log.Printf("Invalid API key provided: %s...", apiKey[:min(5, len(apiKey))])
		http.Error(w, "Unauthorized: Invalid API key", http.StatusUnauthorized)
		return
	}

	// 4. Read Request Body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Bad Request: Cannot read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(body) == 0 {
		log.Println("Received empty request body")
		http.Error(w, "Bad Request: Empty body", http.StatusBadRequest)
		return
	}

	// 5. Validate JSON (optional but recommended)
	if !json.Valid(body) {
		log.Println("Received invalid JSON payload")
		http.Error(w, "Bad Request: Invalid JSON", http.StatusBadRequest)
		return
	}

	// 6. Publish to Kafka
	err = h.publisher.Publish(r.Context(), nil, body) // Using nil key for simplicity
	if err != nil {
		log.Printf("Error publishing message to Kafka: %v", err)
		// Consider specific error handling (e.g., retries, circuit breaker)
		http.Error(w, "Internal Server Error: Failed to queue message", http.StatusInternalServerError)
		return
	}

	// 7. Respond Success
	w.WriteHeader(http.StatusAccepted) // 202 Accepted is suitable for async processing
	w.Write([]byte("Payload accepted\n"))
	log.Println("Ingest request processed successfully")
}

// Helper function from auth package (or redefine locally)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

---

**8. Main Application (`cmd/api/main.go`)**

```go
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

	// Ping DB to ensure connectivity
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

	// Setup Authenticator
	authenticator := auth.NewPostgresAuthenticator(db.Pool, cfg.APIKeyTableName)

	// Setup API Handler
	apiHandler := api.NewAPIHandler(authenticator, publisher)

	// Setup Router
	router := mux.NewRouter()
	router.HandleFunc("/ingest", apiHandler.IngestHandler).Methods(http.MethodPost)
	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Simple health check, can be expanded to check DB/Kafka health
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

	// Graceful Shutdown Handling
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Create a context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting")
}
```

---

**9. Database Migration (`migrations/001_create_api_keys.up.sql`)**

```sql
-- Simple table to store API keys
CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    api_key TEXT UNIQUE NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Optional: Index for faster key lookup
CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys (api_key);

-- Optional: Trigger to update 'updated_at' timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_api_keys_updated_at ON api_keys;
CREATE TRIGGER update_api_keys_updated_at
BEFORE UPDATE ON api_keys
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- You might want to insert a test key (ONLY FOR DEVELOPMENT/TESTING)
-- INSERT INTO api_keys (api_key, description, is_active) VALUES ('test-key-123', 'Default test key', TRUE) ON CONFLICT (api_key) DO NOTHING;
```

---

**10. Dockerfile (`Dockerfile`)**

```dockerfile
# Stage 1: Build the Go application
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
# -ldflags="-w -s" reduces binary size
# CGO_ENABLED=0 ensures static linking, necessary for scratch or distroless base images
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/server ./cmd/api/main.go


# Stage 2: Create the final lightweight image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/server /app/server

# Copy migrations (optional, if you want them in the container for some reason)
# COPY migrations /app/migrations

# Expose the port the app runs on
EXPOSE 8080

# Set the entrypoint command
ENTRYPOINT ["/app/server"]

# Optional: Add a non-root user for security
# RUN addgroup -S appgroup && adduser -S appuser -G appgroup
# USER appuser
```

---

**11. Docker Compose (`docker-compose.yml`)**

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0 # Use a specific stable version
    container_name: zookeeper
    hostname: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ['CMD', 'echo', 'ruok', '|', 'nc', 'localhost', '2181']
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - kafka-net

  kafka:
    image: confluentinc/cp-kafka:7.5.0 # Use a specific stable version
    container_name: kafka
    hostname: kafka
    ports:
      # Expose Kafka broker for external connections (e.g., from your host machine)
      - "9092:9092"
      # Port used for inter-broker communication and within Docker network
      # - "29092:29092" # Uncomment if needed for complex setups
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      # Listeners configuration:
      # LISTENER_INTERNAL: Used for communication within the Docker network
      # LISTENER_EXTERNAL: Used for communication from outside the Docker network (your host)
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: LISTENER_INTERNAL:PLAINTEXT,LISTENER_EXTERNAL:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: LISTENER_INTERNAL://kafka:29092,LISTENER_EXTERNAL://localhost:9092
      KAFKA_LISTENERS: LISTENER_INTERNAL://0.0.0.0:29092,LISTENER_EXTERNAL://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: LISTENER_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1 # For single node cluster
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true' # Auto-create topics if needed
      # KAFKA_CREATE_TOPICS: "ingest-topic:1:1" # Alternatively, explicitly create topics
    healthcheck:
       test: ["CMD-SHELL", "/usr/bin/kafka-topics --bootstrap-server localhost:29092 --list || exit 1"]
       interval: 15s
       timeout: 10s
       retries: 10
    networks:
      - kafka-net

  postgres:
    image: postgres:15-alpine # Use a specific stable version
    container_name: postgres_db
    hostname: postgres
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password # Use secrets in production!
      POSTGRES_DB: ingestdb
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
      # Optional: Mount init scripts to create tables/keys on startup
      - ./migrations:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d ingestdb"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - kafka-net

  app:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: ingest_api
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy # Wait for Kafka to be somewhat ready
    environment:
      LISTEN_ADDRESS: ":8080"
      POSTGRES_URL: "postgres://user:password@postgres:5432/ingestdb?sslmode=disable" # Use secrets!
      KAFKA_BROKERS: "kafka:29092" # Internal listener for app <-> kafka communication
      KAFKA_TOPIC: "ingest-topic" # Make sure this topic exists or auto-creation is enabled
      API_KEY_TABLE_NAME: "api_keys"
    restart: unless-stopped
    networks:
      - kafka-net

networks:
  kafka-net:
    driver: bridge

volumes:
  pg_data:
    driver: local
```

---

**12. Tests (`internal/api/handler_test.go`)**

We'll use mocks for dependencies (`Authenticator`, `Publisher`) in the handler tests for isolation.

```go
package api

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mock Definitions ---

// MockAuthenticator is a mock type for the auth.Authenticator interface
type MockAuthenticator struct {
	mock.Mock
}

func (m *MockAuthenticator) ValidateAPIKey(ctx context.Context, apiKey string) (bool, error) {
	args := m.Called(ctx, apiKey)
	return args.Bool(0), args.Error(1)
}

// MockPublisher is a mock type for the kafka.Publisher interface
type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Publish(ctx context.Context, key []byte, value []byte) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

func (m *MockPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

// --- Test Cases ---

func TestIngestHandler(t *testing.T) {
	validApiKey := "valid-key-123"
	invalidApiKey := "invalid-key-456"
	samplePayload := `{"message": "hello", "value": 42}`
	invalidPayload := `{"message": "hello",` // Invalid JSON

	tests := []struct {
		name               string
		apiKey             string
		requestBody        string
		mockAuthSetup      func(*MockAuthenticator)
		mockPublisherSetup func(*MockPublisher)
		expectedStatusCode int
		expectPublishCall  bool // Whether we expect the publisher's Publish method to be called
	}{
		{
			name:        "Success",
			apiKey:      validApiKey,
			requestBody: samplePayload,
			mockAuthSetup: func(ma *MockAuthenticator) {
				ma.On("ValidateAPIKey", mock.Anything, validApiKey).Return(true, nil)
			},
			mockPublisherSetup: func(mp *MockPublisher) {
				mp.On("Publish", mock.Anything, mock.Anything, []byte(samplePayload)).Return(nil)
			},
			expectedStatusCode: http.StatusAccepted,
			expectPublishCall:  true,
		},
		{
			name:        "Unauthorized - Missing API Key",
			apiKey:      "", // No key provided
			requestBody: samplePayload,
			mockAuthSetup: func(ma *MockAuthenticator) {
				// ValidateAPIKey should not be called if header is missing
			},
			mockPublisherSetup: func(mp *MockPublisher) {},
			expectedStatusCode: http.StatusUnauthorized,
			expectPublishCall:  false,
		},
		{
			name:        "Unauthorized - Invalid API Key",
			apiKey:      invalidApiKey,
			requestBody: samplePayload,
			mockAuthSetup: func(ma *MockAuthenticator) {
				ma.On("ValidateAPIKey", mock.Anything, invalidApiKey).Return(false, nil)
			},
			mockPublisherSetup: func(mp *MockPublisher) {},
			expectedStatusCode: http.StatusUnauthorized,
			expectPublishCall:  false,
		},
		{
			name:        "Unauthorized - Auth DB Error",
			apiKey:      validApiKey,
			requestBody: samplePayload,
			mockAuthSetup: func(ma *MockAuthenticator) {
				ma.On("ValidateAPIKey", mock.Anything, validApiKey).Return(false, errors.New("db connection failed"))
			},
			mockPublisherSetup: func(mp *MockPublisher) {},
			expectedStatusCode: http.StatusInternalServerError, // Or StatusUnauthorized depending on desired behavior
			expectPublishCall:  false,
		},
		{
			name:        "Bad Request - Empty Body",
			apiKey:      validApiKey,
			requestBody: "",
			mockAuthSetup: func(ma *MockAuthenticator) {
				ma.On("ValidateAPIKey", mock.Anything, validApiKey).Return(true, nil)
			},
			mockPublisherSetup: func(mp *MockPublisher) {},
			expectedStatusCode: http.StatusBadRequest,
			expectPublishCall:  false,
		},
		{
			name:        "Bad Request - Invalid JSON",
			apiKey:      validApiKey,
			requestBody: invalidPayload,
			mockAuthSetup: func(ma *MockAuthenticator) {
				ma.On("ValidateAPIKey", mock.Anything, validApiKey).Return(true, nil)
			},
			mockPublisherSetup: func(mp *MockPublisher) {},
			expectedStatusCode: http.StatusBadRequest,
			expectPublishCall:  false,
		},
		{
			name:        "Internal Server Error - Kafka Publish Fails",
			apiKey:      validApiKey,
			requestBody: samplePayload,
			mockAuthSetup: func(ma *MockAuthenticator) {
				ma.On("ValidateAPIKey", mock.Anything, validApiKey).Return(true, nil)
			},
			mockPublisherSetup: func(mp *MockPublisher) {
				mp.On("Publish", mock.Anything, mock.Anything, []byte(samplePayload)).Return(errors.New("kafka broker unavailable"))
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectPublishCall:  true, // Publish was attempted
		},
		// Add test for MethodNotAllowed if needed (though router usually handles this)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			mockAuth := new(MockAuthenticator)
			mockPub := new(MockPublisher)
			tc.mockAuthSetup(mockAuth)
			tc.mockPublisherSetup(mockPub)

			handler := NewAPIHandler(mockAuth, mockPub)
			server := httptest.NewServer(http.HandlerFunc(handler.IngestHandler))
			defer server.Close()

			req, err := http.NewRequest(http.MethodPost, server.URL+"/ingest", bytes.NewBufferString(tc.requestBody))
			require.NoError(t, err)

			if tc.apiKey != "" {
				req.Header.Set(apiKeyHeader, tc.apiKey)
			}
			req.Header.Set("Content-Type", "application/json")

			// Act
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// Assert
			assert.Equal(t, tc.expectedStatusCode, resp.StatusCode, "Status code mismatch")
			mockAuth.AssertExpectations(t) // Verify auth mock calls
			mockPub.AssertExpectations(t)   // Verify publisher mock calls

			// Optional: Check if Publish was called the expected number of times
			if tc.expectPublishCall {
				mockPub.AssertNumberOfCalls(t, "Publish", 1)
			} else {
				mockPub.AssertNumberOfCalls(t, "Publish", 0)
			}
		})
	}
}

// You can add similar tests for auth.go and publisher.go using mocks or
// potentially integration tests that spin up real dependencies if needed.
```

---

**13. README.md (Example)**

```markdown
# Go Kafka Ingest Service

A simple Go HTTP service that accepts JSON payloads, validates an API key against a PostgreSQL database, and publishes the payload to a Kafka topic.

## Features

*   HTTP POST endpoint `/ingest`
*   Requires `X-API-Key` header for authentication.
*   Validates API key against a PostgreSQL database table (`api_keys` by default).
*   Validates that the request body is valid JSON.
*   Publishes the raw JSON payload to a configured Kafka topic.
*   Includes Dockerfile and Docker Compose setup for easy deployment.
*   Includes unit/integration tests for the API handler.

## Prerequisites

*   Go (version 1.21 or later recommended)
*   Docker & Docker Compose
*   Make (optional, for convenience commands)

## Setup and Running

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd go-kafka-ingest
    ```

2.  **Environment Variables:**
    The application uses environment variables for configuration. The `docker-compose.yml` file sets these for the `app` service. For local development without Docker, you might need to export them:
    ```bash
    export POSTGRES_URL="postgres://user:password@localhost:5432/ingestdb?sslmode=disable"
    export KAFKA_BROKERS="localhost:9092"
    export KAFKA_TOPIC="ingest-topic"
    # export LISTEN_ADDRESS=":8080" # Optional, defaults to :8080
    # export API_KEY_TABLE_NAME="api_keys" # Optional, defaults to api_keys
    ```
    **Important:** Use secure methods (like Docker secrets or environment management tools) for sensitive information like `POSTGRES_URL` and potentially API keys in production.

3.  **Database Setup:**
    The `docker-compose.yml` includes a PostgreSQL service. The `migrations/001_create_api_keys.up.sql` script will be run automatically when the `postgres` container starts for the first time, creating the necessary `api_keys` table.

    *   **To add an API key:** Connect to the database (e.g., using `psql` or a GUI tool) and insert a key:
        ```sql
        -- Connect via docker compose:
        -- docker compose exec -it postgres_db psql -U user -d ingestdb

        -- Inside psql:
        INSERT INTO api_keys (api_key, description, is_active) VALUES ('your-secret-api-key', 'Key for my client', TRUE);
        ```

4.  **Running with Docker Compose (Recommended):**
    This command builds the Go application image and starts all services (app, postgres, kafka, zookeeper).
    ```bash
    docker compose up --build -d
    ```
    *   `-d` runs in detached mode.
    *   `--build` forces a rebuild of the `app` image if code changes.
    *   To see logs: `docker compose logs -f app` (or other service names)
    *   To stop: `docker compose down`

5.  **Running Locally (Without Docker):**
    *   Ensure Kafka and PostgreSQL are running and accessible.
    *   Set the environment variables as described in step 2.
    *   Run the application:
        ```bash
        go run ./cmd/api/main.go
        ```

## Testing

*   **Run Unit/Integration Tests:**
    ```bash
    go test ./... -v
    ```
    (This runs the handler tests with mocks).

*   **Manual Testing (using curl):**
    Ensure the service is running (e.g., via `docker compose up`).

    *   **Valid Request:**
        Replace `your-secret-api-key` with the key you added to the database.
        ```bash
        curl -v -X POST http://localhost:8080/ingest \
        -H "Content-Type: application/json" \
        -H "X-API-Key: your-secret-api-key" \
        -d '{"sensor_id": "XYZ-123", "temperature": 25.5, "timestamp": "2023-10-27T10:00:00Z"}'
        ```
        Expected Response: `HTTP/1.1 202 Accepted` and `Payload accepted` in the body.

    *   **Invalid API Key:**
        ```bash
        curl -v -X POST http://localhost:8080/ingest \
        -H "Content-Type: application/json" \
        -H "X-API-Key: wrong-key" \
        -d '{"data": "test"}'
        ```
        Expected Response: `HTTP/1.1 401 Unauthorized`.

    *   **Missing API Key:**
        ```bash
        curl -v -X POST http://localhost:8080/ingest \
        -H "Content-Type: application/json" \
        -d '{"data": "test"}'
        ```
        Expected Response: `HTTP/1.1 401 Unauthorized`.

    *   **Invalid JSON:**
        ```bash
        curl -v -X POST http://localhost:8080/ingest \
        -H "Content-Type: application/json" \
        -H "X-API-Key: your-secret-api-key" \
        -d '{"data": "test"' # Missing closing brace
        ```
        Expected Response: `HTTP/1.1 400 Bad Request`.

## Checking Kafka

You can use Kafka command-line tools (or a GUI like Kafdrop/Offset Explorer) to verify messages arrive in the `ingest-topic`.

```bash
# Run inside the Kafka container
docker compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic ingest-topic \
    --from-beginning
```

## TODO / Improvements

*   Implement more robust health checks (`/healthz`) checking DB and Kafka connectivity.
*   Add request/payload validation beyond just checking if it's valid JSON (e.g., using schemas).
*   Implement structured logging (e.g., using `slog` in Go 1.21+ or libraries like `zerolog`/`zap`).
*   Add request tracing (e.g., OpenTelemetry).
*   Implement metrics (e.g., Prometheus).
*   Use Docker secrets for sensitive environment variables.
*   Consider asynchronous Kafka publishing (`Async: true` in `kafka-go`) for higher throughput, requires careful error handling (using `Writer.Stats`).
*   Add rate limiting.
*   Use HTTPS for the API endpoint.
```