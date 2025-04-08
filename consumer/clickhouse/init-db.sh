#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

CLICKHOUSE_DB="${CLICKHOUSE_DATABASE:-default}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}" # Read password if set

# Build the connection options based on whether a password is provided
OPTS=()
OPTS+=("--database" "$CLICKHOUSE_DB")
OPTS+=("--user" "$CLICKHOUSE_USER")
if [ -n "$CLICKHOUSE_PASSWORD" ]; then
    OPTS+=("--password" "$CLICKHOUSE_PASSWORD")
fi
OPTS+=("--multiquery") # Allow multiple statements

# Use clickhouse-client to execute SQL
# Define your table schema here. Adjust columns and types based on your JSON payload.
clickhouse-client "${OPTS[@]}" <<-EOSQL
    CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.ingest_data (
        -- Example columns - ADJUST THESE TO MATCH YOUR JSON AND NEEDS
        timestamp DateTime64(3),     -- High precision timestamp if available
        sensor_id String,
        temperature Float64,
        humidity Nullable(Float64), -- Example nullable field
        location String,
        -- Optional: Add metadata columns
        _raw_data String,          -- Store the original JSON?
        _received_at DateTime DEFAULT now() -- When the row was inserted

        -- Add other columns based on your JSON structure...

    ) ENGINE = MergeTree()           -- Choose appropriate engine (MergeTree is common)
    ORDER BY (sensor_id, timestamp)  -- Define sorting/primary key
    -- PARTITION BY toYYYYMM(timestamp) -- Optional: Add partitioning for large tables
    SETTINGS index_granularity = 8192;

    -- You can add more CREATE TABLE or other initialization statements here
    -- Example: CREATE DATABASE IF NOT EXISTS my_other_db;

EOSQL

echo "ClickHouse initialization script completed."