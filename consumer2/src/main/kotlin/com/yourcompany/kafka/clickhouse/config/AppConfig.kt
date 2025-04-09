package com.yourcompany.kafka.clickhouse.config

import arrow.core.computations.ResultEffect.bind
import arrow.core.getOrElse
import arrow.core.recover
import arrow.core.flatMap
import arrow.core.Either
import arrow.core.left
import arrow.core.right

data class KafkaConfig(
    val brokers: String,
    val topic: String,
    val groupId: String,
    val pollTimeoutMs: Long = 1000L, // How long to wait in poll()
    val autoOffsetReset: String = "earliest"
)

data class ClickHouseConfig(
    val jdbcUrl: String,
    val user: String,
    val pass: String,
    val database: String,
    val tableName: String = "ingested_data" // Default table name
)

data class AppConfig(
    val kafka: KafkaConfig,
    val clickhouse: ClickHouseConfig
)

// Helper to read env vars or provide default
private fun env(key: String, default: String? = null): Either<ConfigError, String> =
    System.getenv(key)?.right() ?: default?.right() ?: ConfigError.MissingEnvVar(key).left()

sealed class ConfigError(val message: String) {
    data class MissingEnvVar(val key: String) : ConfigError("Missing environment variable: $key")
    data class InvalidValue(val key: String, val value: String, val reason: String) :
        ConfigError("Invalid value for $key ('$value'): $reason")
}


// Load configuration using Arrow Result (Either)
fun loadConfig(): Either<ConfigError, AppConfig> = Either.catch {
    arrow.core.computations.result<ConfigError, AppConfig> { // Use result computation block
        val kafkaBrokers = env("KAFKA_BROKERS").bind()
        val kafkaTopic = env("KAFKA_TOPIC").bind()
        val kafkaGroupId = env("KAFKA_GROUP_ID", "kotlin-consumer-group").bind() // Default group id
        val kafkaPollTimeout = env("KAFKA_POLL_TIMEOUT_MS", "1000").bind()
            .flatMap { it.toLongOrNull()?.right() ?: ConfigError.InvalidValue("KAFKA_POLL_TIMEOUT_MS", it, "Not a number").left() }
            .bind()

        val chJdbcUrl = env("CLICKHOUSE_JDBC_URL").bind() // e.g., jdbc:ch://localhost:8123/default
        val chUser = env("CLICKHOUSE_USER", "default").bind()
        val chPass = env("CLICKHOUSE_PASSWORD", "").bind() // Default empty password
        val chDb = env("CLICKHOUSE_DATABASE", "default").bind()
        val chTable = env("CLICKHOUSE_TABLE", "ingested_data").bind()

        AppConfig(
            kafka = KafkaConfig(
                brokers = kafkaBrokers,
                topic = kafkaTopic,
                groupId = kafkaGroupId,
                pollTimeoutMs = kafkaPollTimeout,
                // autoOffsetReset can be configured via env var too if needed
            ),
            clickhouse = ClickHouseConfig(
                jdbcUrl = chJdbcUrl,
                user = chUser,
                pass = chPass,
                database = chDb,
                tableName = chTable
            )
        )
    }.bind() // Extract the value from the result block
}.mapLeft { throwable ->
    // Catch potential exceptions during env var access or parsing that weren't handled by Either
    ConfigError.InvalidValue("Unknown", "Unknown", throwable.message ?: "Unknown loading error")
}