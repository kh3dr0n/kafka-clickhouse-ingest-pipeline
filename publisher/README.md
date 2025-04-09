# Go Kafka Ingest Service

A simple Go HTTP service that accepts JSON payloads, validates an API key against a PostgreSQL database, and publishes the payload to a Kafka topic.

## Features

*   HTTP POST endpoint `/ingest`
*   Requires `X-API-Key` header for authentication.
*   Validates API key against a PostgreSQL database table (`api_keys` by default).
*   Validates that the request body is valid JSON.
*   Publishes the raw JSON payload to a configured Kafka topic.
*   Includes Dockerfile and Docker Compose setup for easy deployment.
*   In-memory LRU caching for API key validation results to reduce database load (configurable).

## Configuration

The application uses environment variables:

| Variable             | Description                                         | Default       | Required | Example                                                      |
| :------------------- | :-------------------------------------------------- | :------------ | :------- | :----------------------------------------------------------- |
| `LISTEN_ADDRESS`     | Address and port to listen on                       | `:8080`       | No       | `:8080`                                                      |
| `POSTGRES_URL`       | PostgreSQL connection URL                           |               | Yes      | `postgres://user:password@host:5432/dbname?sslmode=disable` |
| `KAFKA_BROKERS`      | Comma-separated list of Kafka broker addresses      |               | Yes      | `kafka1:9092,kafka2:9092`                                    |
| `KAFKA_TOPIC`        | Kafka topic to publish messages to                  |               | Yes      | `ingest-topic`                                               |
| `API_KEY_TABLE_NAME` | Name of the table storing API keys                  | `api_keys`    | No       | `api_keys`                                                   |
| `AUTH_CACHE_ENABLED` | Enable (`true`) or disable (`false`) API key cache | `true`        | No       | `true`                                                       |
| `AUTH_CACHE_SIZE`    | Maximum number of entries in the API key cache      | `1000`        | No       | `5000`                                                       |
| `AUTH_CACHE_TTL`     | Time-to-live for cached API key entries             | `5m` (5 mins) | No       | `10m`, `1h`                                                  |

**Important:** Use secure methods ... (rest of security note) ...

## Setup and Running
... (rest of setup) ...

## Caching Behavior

*   When `AUTH_CACHE_ENABLED` is `true`, the service uses an in-memory LRU (Least Recently Used) cache.
*   Successful validation results (both valid and invalid keys found in the DB) are cached.
*   Database errors during validation are *not* cached.
*   Cached entries expire after `AUTH_CACHE_TTL` to mitigate issues with stale data (e.g., if a key is deactivated in the database, the cache will eventually reflect this after the TTL expires).
*   If the cache reaches `AUTH_CACHE_SIZE`, the least recently used entry is evicted.

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
        -- docker compose exec -it postgres psql -U user -d ingestdb

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
docker compose exec kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --create \
    --topic ingest-topic \
    --partitions 1 \
    --replication-factor 1

```