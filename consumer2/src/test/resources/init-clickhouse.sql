-- This script will be executed by Testcontainers when the ClickHouse container starts
-- Adjust types based on your IngestedData class and needs
CREATE DATABASE IF NOT EXISTS testdb;

CREATE TABLE IF NOT EXISTS testdb.ingested_data (
    `sensorId` Nullable(String),
    `temperature` Nullable(Float64),
    `timestamp` Nullable(String), -- Or DateTime/DateTime64 if you parse in Kotlin/CH
    `value` Nullable(Int32),
    `message` Nullable(String),
    `receivedAt` DateTime DEFAULT now() -- Track when the row was inserted
) ENGINE = MergeTree() -- Choose appropriate ClickHouse engine
ORDER BY (receivedAt, sensorId); -- Define an appropriate ORDER BY key