package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mock Underlying Authenticator ---
type MockUnderlyingAuthenticator struct {
	mock.Mock
}

func (m *MockUnderlyingAuthenticator) ValidateAPIKey(ctx context.Context, apiKey string) (bool, error) {
	args := m.Called(ctx, apiKey)
	return args.Bool(0), args.Error(1)
}

// --- Test Cases ---

func TestCachingAuthenticator(t *testing.T) {
	ctx := context.Background()
	key1 := "key-one"
	key2 := "key-two"
	key3 := "key-three" // To test eviction

	cacheSize := 2
	cacheTTL := 100 * time.Millisecond // Short TTL for testing expiry

	t.Run("Cache Hit", func(t *testing.T) {
		mockNext := new(MockUnderlyingAuthenticator)
		cachingAuth, err := NewCachingAuthenticator(mockNext, cacheSize, cacheTTL)
		require.NoError(t, err)

		// Prime the cache
		mockNext.On("ValidateAPIKey", ctx, key1).Return(true, nil).Once()
		valid, err := cachingAuth.ValidateAPIKey(ctx, key1)
		require.NoError(t, err)
		require.True(t, valid)
		mockNext.AssertExpectations(t) // Ensure underlying was called once

		// Cache hit - underlying should NOT be called again
		valid, err = cachingAuth.ValidateAPIKey(ctx, key1)
		require.NoError(t, err)
		require.True(t, valid)
		mockNext.AssertExpectations(t) // Still called only once in total
	})

	t.Run("Cache Miss", func(t *testing.T) {
		mockNext := new(MockUnderlyingAuthenticator)
		cachingAuth, err := NewCachingAuthenticator(mockNext, cacheSize, cacheTTL)
		require.NoError(t, err)

		// First call (miss)
		mockNext.On("ValidateAPIKey", ctx, key1).Return(false, nil).Once()
		valid, err := cachingAuth.ValidateAPIKey(ctx, key1)
		require.NoError(t, err)
		require.False(t, valid)
		mockNext.AssertExpectations(t)

		// Second call (hit)
		valid, err = cachingAuth.ValidateAPIKey(ctx, key1)
		require.NoError(t, err)
		require.False(t, valid)
		mockNext.AssertExpectations(t) // Still called only once in total
	})

	t.Run("Cache Error Handling", func(t *testing.T) {
		mockNext := new(MockUnderlyingAuthenticator)
		cachingAuth, err := NewCachingAuthenticator(mockNext, cacheSize, cacheTTL)
		require.NoError(t, err)
		dbError := errors.New("database is down")

		// First call (miss, error from underlying)
		mockNext.On("ValidateAPIKey", ctx, key1).Return(false, dbError).Once()
		valid, err := cachingAuth.ValidateAPIKey(ctx, key1)
		require.ErrorIs(t, err, dbError)
		require.False(t, valid)
		mockNext.AssertExpectations(t)

		// Second call (should still miss, as errors aren't cached)
		mockNext.On("ValidateAPIKey", ctx, key1).Return(false, dbError).Once()
		valid, err = cachingAuth.ValidateAPIKey(ctx, key1)
		require.ErrorIs(t, err, dbError)
		require.False(t, valid)
		mockNext.AssertExpectations(t) // Called twice now
	})

	t.Run("Cache Eviction (LRU)", func(t *testing.T) {
		mockNext := new(MockUnderlyingAuthenticator)
		cachingAuth, err := NewCachingAuthenticator(mockNext, cacheSize, 5*time.Minute) // Longer TTL
		require.NoError(t, err)

		// Fill the cache (size 2)
		mockNext.On("ValidateAPIKey", ctx, key1).Return(true, nil).Once()
		_, _ = cachingAuth.ValidateAPIKey(ctx, key1)

		mockNext.On("ValidateAPIKey", ctx, key2).Return(false, nil).Once()
		_, _ = cachingAuth.ValidateAPIKey(ctx, key2)

		// Access key1 again to make it recently used
		_, _ = cachingAuth.ValidateAPIKey(ctx, key1)

		// Add key3, which should evict key2 (least recently used)
		mockNext.On("ValidateAPIKey", ctx, key3).Return(true, nil).Once()
		_, _ = cachingAuth.ValidateAPIKey(ctx, key3)
		mockNext.AssertExpectations(t)

		// Check key1 (should be a hit)
		_, _ = cachingAuth.ValidateAPIKey(ctx, key1)
		mockNext.AssertExpectations(t) // Underlying not called again for key1

		// Check key2 (should be a miss, underlying called again)
		mockNext.On("ValidateAPIKey", ctx, key2).Return(false, nil).Once()
		_, _ = cachingAuth.ValidateAPIKey(ctx, key2)
		mockNext.AssertExpectations(t) // Underlying called again for key2
	})

	t.Run("Cache TTL Expiry", func(t *testing.T) {
		mockNext := new(MockUnderlyingAuthenticator)
		cachingAuth, err := NewCachingAuthenticator(mockNext, cacheSize, cacheTTL) // Short TTL
		require.NoError(t, err)

		// Prime the cache
		mockNext.On("ValidateAPIKey", ctx, key1).Return(true, nil).Once()
		_, _ = cachingAuth.ValidateAPIKey(ctx, key1)
		mockNext.AssertExpectations(t)

		// Wait for TTL to expire
		time.Sleep(cacheTTL + 20*time.Millisecond)

		// Check again (should be a miss due to expiry)
		mockNext.On("ValidateAPIKey", ctx, key1).Return(true, nil).Once()
		_, _ = cachingAuth.ValidateAPIKey(ctx, key1)
		mockNext.AssertExpectations(t) // Underlying called again
	})

	t.Run("Empty API Key", func(t *testing.T) {
		mockNext := new(MockUnderlyingAuthenticator)
		cachingAuth, err := NewCachingAuthenticator(mockNext, cacheSize, cacheTTL)
		require.NoError(t, err)

		// Empty key should return immediately, no cache interaction, no underlying call
		valid, err := cachingAuth.ValidateAPIKey(ctx, "")
		assert.NoError(t, err)
		assert.False(t, valid)
		mockNext.AssertNotCalled(t, "ValidateAPIKey", mock.Anything, mock.Anything)
	})

	t.Run("Cache Disabled", func(t *testing.T) {
		mockNext := new(MockUnderlyingAuthenticator)
		// Size 0 disables cache
		cachingAuth, err := NewCachingAuthenticator(mockNext, 0, cacheTTL)
		require.NoError(t, err)
		// The returned authenticator should be the original mockNext
		require.Equal(t, mockNext, cachingAuth)

		mockNext.On("ValidateAPIKey", ctx, key1).Return(true, nil).Twice() // Expect call every time

		_, _ = cachingAuth.ValidateAPIKey(ctx, key1)
		_, _ = cachingAuth.ValidateAPIKey(ctx, key1)

		mockNext.AssertExpectations(t)
	})
}
