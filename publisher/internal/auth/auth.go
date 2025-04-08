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