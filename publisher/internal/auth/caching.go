package auth

import (
	"context"
	"log"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable" // Use expirable LRU for TTL
)

// cachedAuthResult stores the result of an authentication check.
// Using a struct allows storing more info later if needed (e.g., user ID associated with key).
type cachedAuthResult struct {
	isValid bool
	// Add other fields if needed in the future
}

// CachingAuthenticator wraps another Authenticator with an LRU cache.
type CachingAuthenticator struct {
	next  Authenticator                            // The actual authenticator (e.g., PostgresAuthenticator)
	cache *expirable.LRU[string, cachedAuthResult] // LRU cache (string key -> auth result)
	ttl   time.Duration                            // Cache entry TTL
}

// NewCachingAuthenticator creates a new caching authenticator decorator.
func NewCachingAuthenticator(next Authenticator, size int, ttl time.Duration) (Authenticator, error) {
	if size <= 0 {
		log.Printf("Auth cache size is zero or negative, caching disabled.")
		return next, nil // Caching disabled, return the original authenticator
	}
	if ttl <= 0 {
		log.Printf("Auth cache TTL is zero or negative, using non-expiring cache.")
		// Fallback to a non-expiring cache if TTL is invalid, although expirable is generally better
		// For simplicity, we'll require a positive TTL for the expirable cache.
		// If you need a non-expiring LRU, use lru.New[string, cachedAuthResult](size)
		ttl = 5 * time.Minute // Default TTL if config is bad? Or return error? Let's enforce positive TTL.
		// return nil, fmt.Errorf("AuthCacheTTL must be positive duration")
		log.Printf("Using default TTL of %v", ttl)

	}

	// Use expirable LRU for automatic TTL handling
	cache := expirable.NewLRU[string, cachedAuthResult](size, nil, ttl) // nil onEvict func

	log.Printf("Initialized API key auth cache with size %d and TTL %v", size, ttl)

	return &CachingAuthenticator{
		next:  next,
		cache: cache,
		ttl:   ttl, // Store TTL for logging/potential future use
	}, nil
}

// ValidateAPIKey checks the cache first, then delegates to the underlying authenticator if necessary.
func (ca *CachingAuthenticator) ValidateAPIKey(ctx context.Context, apiKey string) (bool, error) {
	if apiKey == "" {
		return false, nil // Don't cache or check DB for empty keys
	}

	// 1. Check Cache
	if result, ok := ca.cache.Get(apiKey); ok {
		log.Printf("Auth cache hit for key: %s...", apiKey[:min(5, len(apiKey))])
		return result.isValid, nil // Return cached result
	}

	// 2. Cache Miss - Delegate to the next authenticator
	log.Printf("Auth cache miss for key: %s... querying source", apiKey[:min(5, len(apiKey))])
	isValid, err := ca.next.ValidateAPIKey(ctx, apiKey)
	if err != nil {
		// Do not cache errors, as they might be transient DB issues
		log.Printf("Error during underlying auth check for key %s...: %v", apiKey[:min(5, len(apiKey))], err)
		return false, err
	}

	// 3. Cache the successful result (valid or invalid)
	log.Printf("Caching auth result (%t) for key: %s...", isValid, apiKey[:min(5, len(apiKey))])
	ca.cache.Add(apiKey, cachedAuthResult{isValid: isValid})

	return isValid, nil
}

// Helper function (could be moved to a common utils package)
// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }
