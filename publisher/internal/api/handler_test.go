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
			mockPub.AssertExpectations(t)  // Verify publisher mock calls

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
