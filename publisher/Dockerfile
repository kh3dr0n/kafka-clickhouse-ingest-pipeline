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