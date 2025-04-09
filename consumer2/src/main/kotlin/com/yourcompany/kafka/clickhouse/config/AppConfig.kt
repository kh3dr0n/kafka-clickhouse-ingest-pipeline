package com.yourcompany.kafka.clickhouse.config
import arrow.core.Either
import arrow.core.computations.ResultEffect // <--- Or similar Effect type if you use Result
import arrow.core.flatMap // Ensure flatMap is imported if needed explicitly
import arrow.core.continuations.either // <--- Import the either computation builder
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import java.lang.NumberFormatException // Example exception

// Define your ConfigError sealed class/interface (if not already done)
sealed interface ConfigError {
    data class MissingEnvVar(val name: String) : ConfigError
    data class InvalidValue(val name: String, val value: String, val reason: String) : ConfigError
    data class Other(val message: String, val cause: Throwable? = null) : ConfigError
}

// Define your AppConfig and nested Config classes (if not already done)
data class KafkaConfig(
    val brokers: String,
    val topic: String,
    val groupId: String,
    val pollTimeoutMs: Long,
    val autoOffsetReset: String = "earliest"
)

data class ClickHouseConfig(
    val jdbcUrl: String,
    val user: String,
    val pass: String,
    val database: String,
    val tableName: String
    // Add other ClickHouse settings as needed
)

data class AppConfig(
    val kafka: KafkaConfig,
    val clickhouse: ClickHouseConfig
)


// --- Helper function to read env vars into Either ---
// This is crucial for the `either` block to work
fun env(name: String): Either<ConfigError, String> =
    System.getenv(name)?.right() ?: ConfigError.MissingEnvVar(name).left()

// Overload for default values (always returns Right as default is provided)
fun env(name: String, default: String): Either<ConfigError, String> =
    (System.getenv(name) ?: default).right() // No Left case if default exists


// --- Your loadConfig function using the `either` builder ---
fun loadConfig(): Either<ConfigError, AppConfig> =
    either {
        // Now you are inside the EitherEffect scope where .bind() exists

        val kafkaBrokers = env("KAFKA_BROKERS").bind() // bind() works now!
        val kafkaTopic = env("KAFKA_TOPIC").bind()
        val kafkaGroupId = env("KAFKA_GROUP_ID", "kotlin-consumer-group").bind()

        // --- Handle parsing within the block ---
        val kafkaPollTimeoutStr = env("KAFKA_POLL_TIMEOUT_MS", "1000").bind()
        val kafkaPollTimeout = (kafkaPollTimeoutStr.toLongOrNull()?.right() // Attempt parsing
            ?: // If null (parsing failed), create a Left
            1000L.right()).bind() // bind() to short-circuit on error

        val chJdbcUrl = env("CLICKHOUSE_JDBC_URL").bind()
        val chUser = env("CLICKHOUSE_USER", "default").bind()
        val chPass = env("CLICKHOUSE_PASSWORD", "").bind() // Reads "" if env var missing/empty
        val chDb = env("CLICKHOUSE_DATABASE", "default").bind()
        val chTable = env("CLICKHOUSE_TABLE", "ingested_data").bind()

        // The last expression in the block is the success value (Right)
        AppConfig(
            kafka = KafkaConfig(
                brokers = kafkaBrokers,
                topic = kafkaTopic,
                groupId = kafkaGroupId,
                pollTimeoutMs = kafkaPollTimeout
            ),
            clickhouse = ClickHouseConfig(
                jdbcUrl = chJdbcUrl,
                user = chUser,
                pass = chPass,
                database = chDb,
                tableName = chTable
            )
        )
    } // No need for mapLeft with 'either', errors are handled by binds short-circuiting