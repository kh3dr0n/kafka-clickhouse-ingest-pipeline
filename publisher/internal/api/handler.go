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

	// 6. Publish to Kafka (Asynchronously)
	err = h.publisher.Publish(r.Context(), nil, body) // Using nil key for simplicity
	if err != nil {
		// This error in async mode is likely an immediate config error or context issue.
		log.Printf("Error queuing message to Kafka (async publish): %v", err)
		http.Error(w, "Internal Server Error: Failed to queue message", http.StatusInternalServerError)
		return
	}

	// 7. Respond Success - Message is accepted and *queued* for sending.
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Payload accepted\n"))
	log.Println("Ingest request processed successfully, payload queued for Kafka.") // Updated log
}

// Helper function from auth package (or redefine locally)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
