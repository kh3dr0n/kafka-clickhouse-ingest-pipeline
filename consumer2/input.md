Okay, let's build the Kotlin Kafka consumer service that writes to ClickHouse, using Arrow-kt and coroutines for a lightweight, functional approach.

**Project Structure (Gradle):**

```
.
├── build.gradle.kts
├── settings.gradle.kts
├── src
│   ├── main
│   │   ├── kotlin
│   │   │   └── com/yourcompany/kafka/clickhouse
│   │   │       ├── config         # Configuration loading
│   │   │       │   └── AppConfig.kt
│   │   │       ├── data           # Data classes for JSON payload
│   │   │       │   └── IngestedData.kt
│   │   │       ├── persistence    # ClickHouse interaction
│   │   │       │   └── ClickHouseRepository.kt
│   │   │       ├── processing     # Core consumption and processing logic
│   │   │       │   ├── KafkaMessageConsumer.kt
│   │   │       │   └── MessageProcessor.kt
│   │   │       └── App.kt         # Main application entry point
│   │   └── resources
│   │       └── logback.xml       # Logging configuration
│   └── test
│       ├── kotlin
│       │   └── com/yourcompany/kafka/clickhouse
│       │       ├── persistence
│       │       │   └── ClickHouseRepositoryIntegrationTest.kt
│       │       └── processing
│       │           └── MessageProcessorTest.kt
│       └── resources
│           └── init-clickhouse.sql # Schema for test container
├── Dockerfile
└── docker-compose.yml
```

---

**1. `settings.gradle.kts`**

```kotlin
rootProject.name = "kotlin-kafka-clickhouse"
```

---

**2. `build.gradle.kts`**

```kotlin
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar // Import for shadowJar

plugins {
    kotlin("jvm") version "1.9.21" // Use a recent Kotlin version
    kotlin("plugin.serialization") version "1.9.21"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1" // For creating fat JARs
}

group = "com.yourcompany"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Kotlin & Coroutines
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.7.3") // For CompletableFuture integration if needed

    // Arrow KT
    val arrowVersion = "1.2.1" // Check for the latest Arrow version
    implementation("io.arrow-kt:arrow-core:$arrowVersion")
    implementation("io.arrow-kt:arrow-fx-coroutines:$arrowVersion")
    // implementation("io.arrow-kt:arrow-fx-stm:$arrowVersion") // Optional, if using STM

    // Kafka Client
    implementation("org.apache.kafka:kafka-clients:3.6.1") // Check for latest compatible version

    // ClickHouse Client (JDBC)
    // Using JDBC for simplicity, wrapped in Dispatchers.IO
    // Consider clickhouse-java native client for potentially better async perf if needed.
    implementation("com.clickhouse:clickhouse-jdbc:0.5.0") // Check for latest version, 'http' classifier needed for HTTP protocol
    implementation("com.zaxxer:HikariCP:5.1.0") // Connection Pooling

    // JSON Serialization (Kotlinx)
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")

    // Logging (SLF4J + Logback)
    implementation("org.slf4j:slf4j-api:2.0.9")
    runtimeOnly("ch.qos.logback:logback-classic:1.4.11")

    // Testing - JUnit 5, AssertJ, MockK, Testcontainers
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.1")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("io.mockk:mockk:1.13.8")
    testImplementation("org.testcontainers:testcontainers:1.19.3")
    testImplementation("org.testcontainers:junit-jupiter:1.19.3")
    testImplementation("org.testcontainers:kafka:1.19.3")
    testImplementation("org.testcontainers:clickhouse:1.19.3")
}

application {
    mainClass.set("com.yourcompany.kafka.clickhouse.AppKt") // Set the main class
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11" // Target JVM 11 or higher
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

// Configure shadowJar task to create an executable fat JAR
tasks.withType<ShadowJar> {
    archiveBaseName.set("kotlin-kafka-clickhouse")
    archiveClassifier.set("") // No classifier like '-all'
    archiveVersion.set(project.version.toString())
    manifest {
        attributes(mapOf("Main-Class" to application.mainClass.get()))
    }
}

// Ensure shadowJar runs when building
tasks.named("build") {
    dependsOn(tasks.named("shadowJar"))
}
```

---

**3. Configuration (`src/main/kotlin/com/yourcompany/kafka/clickhouse/config/AppConfig.kt`)**

```kotlin
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
```

---

**4. Data Class (`src/main/kotlin/com/yourcompany/kafka/clickhouse/data/IngestedData.kt`)**

Define a structure that matches the *expected* JSON from Kafka. Make it flexible if the schema isn't strictly enforced.

```kotlin
package com.yourcompany.kafka.clickhouse.data

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class IngestedData(
    // Example fields - adjust to your actual JSON structure
    val sensorId: String? = null, // Make fields nullable if they might be missing
    val temperature: Double? = null,
    val timestamp: String? = null, // Keep as String for simplicity, ClickHouse can parse
    val value: Int? = null,
    val message: String? = null,
    // Add a field to store the raw JSON or other metadata if needed
    val rawData: JsonElement? = null // Or store the original String
)

// You might want a more specific ClickHouse table structure
// data class ClickHouseRow(
//     val eventTime: java.time.OffsetDateTime, // Use appropriate ClickHouse types
//     val sensor: String,
//     val temp: Float
// )
```

---

**5. ClickHouse Repository (`src/main/kotlin/com/yourcompany/kafka/clickhouse/persistence/ClickHouseRepository.kt`)**

```kotlin
package com.yourcompany.kafka.clickhouse.persistence

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.clickhouse.jdbc.ClickHouseDataSource
import com.yourcompany.kafka.clickhouse.config.ClickHouseConfig
import com.yourcompany.kafka.clickhouse.data.IngestedData
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.*
import javax.sql.DataSource

interface ClickHouseRepository {
    suspend fun insertBatch(data: List<IngestedData>): Either<Throwable, Unit>
    fun close()
}

class JdbcClickHouseRepository(
    private val config: ClickHouseConfig,
    // Inject dispatcher for better testability, default to IO
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO
) : ClickHouseRepository {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val dataSource: DataSource

    init {
        // Use HikariCP for connection pooling
        val hikariConfig = HikariConfig().apply {
            jdbcUrl = config.jdbcUrl // Should include the database e.g. jdbc:ch://host:port/db_name
            username = config.user
            password = config.pass
            driverClassName = "com.clickhouse.jdbc.ClickHouseDriver" // Specify ClickHouse driver
            maximumPoolSize = 10 // Adjust pool size as needed
            // ClickHouse specific settings might be needed via addDataSourceProperty
            addDataSourceProperty("ssl", "false") // Adjust based on your CH setup
            // addDataSourceProperty("compress", "lz4") // Example: enable compression
            poolName = "ClickHousePool"
        }
        dataSource = HikariDataSource(hikariConfig)
        logger.info("ClickHouse connection pool initialized for URL: ${config.jdbcUrl}")
        // Verify connection on init? Maybe a health check later.
    }

    // Adjust the INSERT statement based on your IngestedData and ClickHouse table schema
    // Using nullable types (?) and COALESCE/ifNull might be needed in SQL
    // Ensure your ClickHouse table (e.g., ingested_data) exists with these columns.
    private val insertSql = """
        INSERT INTO ${config.tableName} (sensorId, temperature, timestamp, value, message, receivedAt)
        VALUES (?, ?, ?, ?, ?, ?)
    """.trimIndent() // Add a timestamp column in CH for when data was received

    override suspend fun insertBatch(data: List<IngestedData>): Either<Throwable, Unit> = withContext(dispatcher) {
        if (data.isEmpty()) {
            return@withContext Unit.right() // Nothing to insert
        }

        Either.catch {
            dataSource.connection.use { connection ->
                connection.prepareStatement(insertSql).use { statement ->
                    data.forEach { item ->
                        // Use setString/setDouble etc. Handle potential nulls gracefully.
                        statement.setObject(1, item.sensorId) // setObject handles nulls
                        statement.setObject(2, item.temperature)
                        statement.setObject(3, item.timestamp) // Assuming CH table column is String/DateTime
                        statement.setObject(4, item.value)
                        statement.setObject(5, item.message)
                        statement.setObject(6, java.time.OffsetDateTime.now()) // Add received time

                        statement.addBatch()
                    }
                    val results = statement.executeBatch()
                    logger.debug("Executed batch insert for ${results.size} records. Result counts: ${results.contentToString()}")
                    // Basic check: Ensure all batch statements executed without error (-2 = Statement.SUCCESS_NO_INFO)
                    // More specific error checking might be needed depending on driver behavior
                    if (results.any { it < 0 && it != PreparedStatement.EXECUTE_FAILED }) {
                         // some drivers return negative values on success
                         logger.info("Batch insert successful for ${data.size} items.")
                    } else if (results.contains(PreparedStatement.EXECUTE_FAILED)) {
                        throw SQLException("One or more statements in the batch failed.")
                    }
                     Unit // Return Unit on success
                }
            }
        }.mapLeft { error ->
            logger.error("Failed to insert batch into ClickHouse ({} items): {}", data.size, error.message, error)
            // Wrap specific SQLExceptions if needed
            error // Return the caught throwable
        }
    }

    override fun close() {
        (dataSource as? HikariDataSource)?.close()
        logger.info("ClickHouse connection pool closed.")
    }
}
```

*Self-Correction during thought process:* Initially considered single inserts, but batching is crucial for ClickHouse performance. Switched `insertData` to `insertBatch`. Added HikariCP for connection pooling, which is standard practice for JDBC. Ensured `withContext(Dispatchers.IO)` is used for blocking JDBC calls. Added `receivedAt` timestamp.

---

**6. Kafka Consumer (`src/main/kotlin/com/yourcompany/kafka/clickhouse/processing/KafkaMessageConsumer.kt`)**

```kotlin
package com.yourcompany.kafka.clickhouse.processing

import arrow.core.Either
import arrow.core.flatMap
import arrow.fx.coroutines.resourceScope
import com.yourcompany.kafka.clickhouse.config.KafkaConfig
import kotlinx.coroutines.*
import kotlinx.coroutines.time.delay
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

class KafkaMessageConsumer(
    private val config: KafkaConfig,
    private val messageProcessor: MessageProcessor,
    private val consumerScope: CoroutineScope, // Scope for the consumer loop
    private val processingDispatcher: CoroutineDispatcher = Dispatchers.Default // Dispatcher for CPU-bound processing
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val consumer: KafkaConsumer<String, String>
    private val closed = AtomicBoolean(false)
    private val pollTimeoutDuration = Duration.ofMillis(config.pollTimeoutMs)
    private val batchSize = 100 // Configurable: How many records to process before committing
    private val batchTimeout = Duration.ofSeconds(5) // Configurable: Max time to wait before processing a partial batch

    init {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers)
            put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") // Disable auto-commit
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSize) // Poll up to batch size
            // Add other relevant Kafka consumer configs (security, etc.)
        }
        consumer = KafkaConsumer(props)
        logger.info("KafkaConsumer initialized for brokers: {}, topic: {}, group: {}", config.brokers, config.topic, config.groupId)
    }

    fun startConsuming() = consumerScope.launch { // Launch within the provided scope
        try {
            consumer.subscribe(Collections.singletonList(config.topic))
            logger.info("Subscribed to topic: ${config.topic}")

            val recordBuffer = mutableListOf<ConsumerRecord<String, String>>()
            var lastProcessTime = System.currentTimeMillis()

            while (isActive && !closed.get()) { // Check coroutine status and closed flag
                logger.trace("Polling Kafka for records...")
                val records = try {
                     // Run blocking poll in a cancellable way using withContext might be safer,
                     // but poll() is designed to be interruptible via wakeup()
                     // Ensure the dispatcher handles blocking IO appropriately if not Dispatchers.IO
                     withContext(Dispatchers.IO) { // Explicitly use IO dispatcher for poll
                         consumer.poll(pollTimeoutDuration)
                     }
                } catch (e: WakeupException) {
                    logger.info("Kafka consumer poll woken up, likely shutting down.")
                    break // Exit loop on wakeup
                } catch (e: Exception) {
                    logger.error("Error during Kafka poll: ${e.message}", e)
                    delay(Duration.ofSeconds(5)) // Wait before retrying poll on other errors
                    continue // Try polling again
                }


                if (!records.isEmpty) {
                    logger.debug("Polled ${records.count()} records.")
                    recordBuffer.addAll(records)
                }

                val timeSinceLastProcess = System.currentTimeMillis() - lastProcessTime
                // Process if batch is full OR timeout reached and buffer has records
                if (recordBuffer.size >= batchSize || (recordBuffer.isNotEmpty() && timeSinceLastProcess >= batchTimeout.toMillis())) {
                     val batchToProcess = ArrayList(recordBuffer) // Copy to avoid concurrent modification
                     recordBuffer.clear()

                     logger.info("Processing batch of ${batchToProcess.size} records.")
                     // Process the batch using the MessageProcessor
                     // Run processing potentially on a different dispatcher if CPU intensive
                     val processingResult = withContext(processingDispatcher) {
                         messageProcessor.processBatch(batchToProcess)
                     }

                     processingResult.fold(
                         ifLeft = { error ->
                             logger.error("Failed to process batch: $error. Offsets will not be committed for this batch.", error)
                             // Decide on error strategy: skip batch, retry, shutdown?
                             // For now, we log and skip committing this batch's offsets.
                             // The consumer will likely re-poll these messages later.
                             // Consider adding metrics for failed batches.
                         },
                         ifRight = {
                             // Commit offsets only if processing was successful
                             commitOffsets() // Use commitAsync for non-blocking
                             logger.info("Successfully processed and committed batch of ${batchToProcess.size} records.")
                         }
                     )
                     lastProcessTime = System.currentTimeMillis() // Reset timer after processing attempt
                 }
            }
        } catch (e: Exception) {
            // Catch exceptions during subscribe or the loop setup
            logger.error("Unhandled exception in consumer loop: ${e.message}", e)
        } finally {
            logger.info("Consumer loop finishing. Closing KafkaConsumer.")
            closeConsumer()
        }
    }

    private suspend fun commitOffsets() {
        try {
            withContext(Dispatchers.IO) { // Commit is blocking
                consumer.commitSync() // Using commitSync for simplicity in batching; commitAsync needs careful callback handling
            }
            logger.debug("Offsets committed successfully.")
        } catch (e: Exception) {
            logger.error("Failed to commit offsets: ${e.message}", e)
            // Handle commit failure (retry? log? metrics?)
        }
    }


    private fun closeConsumer() {
         // Important: Use try-finally or Arrow's Resource for guaranteed closing
         try {
             consumer.close(Duration.ofSeconds(10)) // Close with timeout
             logger.info("KafkaConsumer closed.")
         } catch (e: Exception) {
             logger.warn("Exception while closing KafkaConsumer: ${e.message}", e)
         }
    }

    // Method to trigger shutdown from outside
    fun shutdown() {
        logger.info("Shutdown requested for Kafka consumer.")
        closed.set(true)
        consumer.wakeup() // Interrupt the blocking poll() call
    }
}
```

*Self-Correction:* Added batching logic (`recordBuffer`, `batchSize`, `batchTimeout`) for efficiency with ClickHouse. Used `commitSync` after successful batch processing (safer starting point than `commitAsync`). Explicitly used `Dispatchers.IO` for `poll` and `commitSync`. Added `processingDispatcher` for potentially CPU-bound parsing/transformation. Implemented `shutdown` with `wakeup()`.

---

**7. Message Processor (`src/main/kotlin/com/yourcompany/kafka/clickhouse/processing/MessageProcessor.kt`)**

```kotlin
package com.yourcompany.kafka.clickhouse.processing

import arrow.core.Either
import arrow.core.computations.either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.yourcompany.kafka.clickhouse.data.IngestedData
import com.yourcompany.kafka.clickhouse.persistence.ClickHouseRepository
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

interface MessageProcessor {
    suspend fun processBatch(records: List<ConsumerRecord<String, String>>): Either<Throwable, Unit>
}

class DefaultMessageProcessor(
    private val repository: ClickHouseRepository,
    // Lenient Json parsing - ignores unknown keys, useful if payload schema varies
    private val jsonParser: Json = Json { ignoreUnknownKeys = true; isLenient = true }
) : MessageProcessor {

    private val logger = LoggerFactory.getLogger(javaClass)

    override suspend fun processBatch(records: List<ConsumerRecord<String, String>>): Either<Throwable, Unit> {
        logger.debug("Processing batch of ${records.size} records.")

        // Use Either computation block for cleaner error handling
        return either {
            // 1. Parse JSON records, collecting successes and logging failures
            val successfullyParsed = mutableListOf<IngestedData>()
            val failedRecordKeys = mutableListOf<String>() // Track failed keys for logging

            records.forEach { record ->
                parseRecord(record).fold(
                    ifLeft = { error ->
                        logger.warn("Failed to parse record key ${record.key() ?: "null"} offset ${record.offset()}: $error")
                        failedRecordKeys.add(record.key() ?: "offset-${record.offset()}") // Log key or offset
                    },
                    ifRight = { data ->
                        successfullyParsed.add(data)
                    }
                )
            }

            if (successfullyParsed.isEmpty()) {
                 logger.warn("No records were successfully parsed in this batch ({} total failures).", failedRecordKeys.size)
                 // Decide if this is an error for the whole batch. Returning Right to commit offsets for failed parsing.
                 // If parsing failure should prevent commit, return a Left here.
                 Unit.right() // Proceed to commit offsets even if all parsing failed
             } else {
                logger.debug("Successfully parsed ${successfullyParsed.size} records (failures: ${failedRecordKeys.size}). Inserting into ClickHouse.")
                // 2. Insert the successfully parsed batch into ClickHouse
                repository.insertBatch(successfullyParsed).bind() // bind() will short-circuit if insertion fails
                logger.info("Successfully inserted batch of ${successfullyParsed.size} records into ClickHouse.")
            }
             Unit // Final result if all steps succeeded
         }
    }


    private fun parseRecord(record: ConsumerRecord<String, String>): Either<Throwable, IngestedData> {
        return Either.catch {
            jsonParser.decodeFromString<IngestedData>(record.value())
        }.mapLeft { error ->
             // Provide more context on parsing error
             Exception("JSON parsing error for offset ${record.offset()}: ${error.message}", error)
        }
    }
}
```
*Self-Correction:* Changed `process` to `processBatch`. The processor now attempts to parse all records in the batch, inserts the successful ones, and logs failures. It returns success even if some parsing failed, allowing the consumer to commit offsets (this prevents endlessly retrying poison pill messages). If *insertion* fails, the `bind()` short-circuits and returns `Left`, preventing the commit.

---

**8. Main Application (`src/main/kotlin/com/yourcompany/kafka/clickhouse/App.kt`)**

```kotlin
package com.yourcompany.kafka.clickhouse

import arrow.core.flatMap
import arrow.fx.coroutines.resourceScope
import com.yourcompany.kafka.clickhouse.config.loadConfig
import com.yourcompany.kafka.clickhouse.persistence.JdbcClickHouseRepository
import com.yourcompany.kafka.clickhouse.processing.DefaultMessageProcessor
import com.yourcompany.kafka.clickhouse.processing.KafkaMessageConsumer
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

val logger = LoggerFactory.getLogger("com.yourcompany.kafka.clickhouse.AppKt")!! // Top-level logger

fun main(): Unit = runBlocking { // Use runBlocking for the main entry point
    logger.info("Starting Kafka-to-ClickHouse Consumer Application...")

    // Load configuration
    val config = loadConfig().flatMap { cfg ->
        logger.info("Configuration loaded successfully.")
        // Perform any additional validation if needed
        cfg.right() // Keep it Either<ConfigError, AppConfig>
    }.getOrElse { error ->
        logger.error("Failed to load configuration: ${error.message}")
        exitProcess(1) // Exit if config fails
    }


    // Create a main scope for the application lifetime
    val appScope = CoroutineScope(Dispatchers.Default + SupervisorJob()) // Use SupervisorJob

    var consumer: KafkaMessageConsumer? = null // Hold reference for shutdown
    var repository: JdbcClickHouseRepository? = null

    try {
        resourceScope { // Use resourceScope for automatic resource management
            // Setup ClickHouse Repository (will be closed by resourceScope)
            repository = install({
                JdbcClickHouseRepository(config.clickhouse)
            }) { repo, exitCase ->
                 logger.info("Closing ClickHouse Repository (ExitCase: $exitCase)...")
                 repo.close() // Ensure close is called
                 logger.info("ClickHouse Repository closed.")
            }

            // Setup Message Processor
            val messageProcessor = DefaultMessageProcessor(repository!!) // repository is guaranteed non-null here

            // Setup Kafka Consumer
            // Pass the appScope so the consumer's lifecycle is tied to it
            consumer = KafkaMessageConsumer(config.kafka, messageProcessor, appScope)

            // Register shutdown hook
            Runtime.getRuntime().addShutdownHook(Thread {
                logger.info("Shutdown hook triggered. Initiating graceful shutdown...")
                consumer?.shutdown() // Request consumer to stop polling
                // Give some time for the consumer loop to finish processing current batch and close
                // runBlocking is okay in shutdown hook if needed for suspend functions, but try to keep it short.
                runBlocking { delay(5000) } // Adjust delay as needed
                appScope.cancel() // Cancel all coroutines in the scope
                // Repository closing is handled by resourceScope
                logger.info("Shutdown sequence complete.")
            })

            // Start consuming (runs asynchronously within appScope)
            consumer!!.startConsuming()

            logger.info("Application started successfully. Consumer is running.")
            // Keep the main thread alive until the scope is cancelled (e.g., by shutdown hook)
            // Or use awaitCancellation() if main doesn't need to do anything else
            appScope.coroutineContext[Job]?.join() // Wait for the main job to complete

        } // resourceScope ensures repository is closed even on exceptions
        logger.info("Application scope finished.")

    } catch (e: Exception) {
         logger.error("Application failed with unhandled exception: ${e.message}", e)
         appScope.cancel() // Cancel scope on error
         exitProcess(1)
     } finally {
         // Ensure cancellation in case of errors outside resourceScope or normal exit
         if (appScope.isActive) {
             logger.info("Performing final cancellation of application scope.")
             appScope.cancel()
         }
         logger.info("Application exiting.")
     }
}
```
*Self-Correction:* Used `resourceScope` for managing the `ClickHouseRepository` lifecycle reliably. Created a dedicated `appScope` with a `SupervisorJob`. Ensured the `KafkaMessageConsumer` is started within this scope. Improved shutdown hook logic to signal the consumer and then cancel the scope. Added `join()` to keep `main` alive.

---

**9. Logging Config (`src/main/resources/logback.xml`)**

```xml
<configuration>

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{YYYY-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- Set level to INFO for less noise, DEBUG for more detail -->
    <root level="INFO">
        <appender-ref ref="STDOUT" />
    </root>

    <!-- Example: Set specific logger levels -->
    <logger name="org.apache.kafka" level="WARN"/>
    <logger name="com.zaxxer.hikari" level="INFO"/>
    <logger name="com.yourcompany.kafka.clickhouse" level="DEBUG"/> <!-- More detail for your app -->

</configuration>
```

---

**10. ClickHouse Test Schema (`src/test/resources/init-clickhouse.sql`)**

```sql
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
```

---

**11. Tests**

**a) Message Processor Unit Test (`src/test/kotlin/com/yourcompany/kafka/clickhouse/processing/MessageProcessorTest.kt`)**

```kotlin
package com.yourcompany.kafka.clickhouse.processing

import arrow.core.Either
import arrow.core.right
import com.yourcompany.kafka.clickhouse.data.IngestedData
import com.yourcompany.kafka.clickhouse.persistence.ClickHouseRepository
import io.mockk.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MessageProcessorTest {

    private lateinit var mockRepository: ClickHouseRepository
    private lateinit var messageProcessor: DefaultMessageProcessor

    // Test data classes - simpler for verification
    @kotlinx.serialization.Serializable data class SimpleData(val id: String, val count: Int)

    @BeforeEach
    fun setUp() {
        mockRepository = mockk()
        messageProcessor = DefaultMessageProcessor(mockRepository, Json { ignoreUnknownKeys = true }) // Use same Json config
    }

    @Test
    fun `processBatch should parse valid records and call repository insertBatch`() = runTest {
        // Arrange
        val validJson1 = """{"sensorId": "A1", "temperature": 25.5}"""
        val validJson2 = """{"sensorId": "B2", "message": "OK"}"""
        val records = listOf(
            ConsumerRecord("test-topic", 0, 100, "key1", validJson1),
            ConsumerRecord("test-topic", 0, 101, "key2", validJson2)
        )

        val expectedData = listOf(
            IngestedData(sensorId = "A1", temperature = 25.5),
            IngestedData(sensorId = "B2", message = "OK")
        )

        // Mock repository interaction (suspend functions need coEvery)
        coEvery { mockRepository.insertBatch(any()) } returns Unit.right() // Mock successful insertion

        // Act
        val result = messageProcessor.processBatch(records)

        // Assert
        assertThat(result.isRight()).isTrue() // Expect overall success

        // Verify repository was called with the correctly parsed data
        val capturedData = slot<List<IngestedData>>()
        coVerify(exactly = 1) { mockRepository.insertBatch(capture(capturedData)) }

        // Use AssertJ's recursive comparison (add dependency if needed, or compare manually)
        // Ensure the order doesn't matter if your insert logic doesn't depend on it
        assertThat(capturedData.captured).containsExactlyInAnyOrderElementsOf(expectedData)
    }

    @Test
    fun `processBatch should handle mixed valid and invalid records and insert valid ones`() = runTest {
        // Arrange
        val validJson = """{"sensorId": "C3", "value": 99}"""
        val invalidJson = """{"sensorId": "D4", "value":}""" // Invalid JSON
        val records = listOf(
            ConsumerRecord("test-topic", 0, 102, "key3", validJson),
            ConsumerRecord("test-topic", 0, 103, "key4", invalidJson)
        )

        val expectedInsertedData = listOf(
            IngestedData(sensorId = "C3", value = 99)
        )

        coEvery { mockRepository.insertBatch(any()) } returns Unit.right()

        // Act
        val result = messageProcessor.processBatch(records)

        // Assert
        assertThat(result.isRight()).isTrue() // Still successful overall (parsing errors logged)

        val capturedData = slot<List<IngestedData>>()
        coVerify(exactly = 1) { mockRepository.insertBatch(capture(capturedData)) }
        assertThat(capturedData.captured).containsExactlyElementsOf(expectedInsertedData)
    }

    @Test
    fun `processBatch should return Left if repository insertion fails`() = runTest {
        // Arrange
        val validJson = """{"sensorId": "E5"}"""
        val records = listOf(ConsumerRecord("test-topic", 0, 104, "key5", validJson))
        val dbError = RuntimeException("ClickHouse connection failed")

        // Mock repository failure
        coEvery { mockRepository.insertBatch(any()) } returns Either.Left(dbError)

        // Act
        val result = messageProcessor.processBatch(records)

        // Assert
        assertThat(result.isLeft()).isTrue()
        assertThat(result.leftOrNull()).isEqualTo(dbError)

        // Verify insertion was attempted
        coVerify(exactly = 1) { mockRepository.insertBatch(any()) }
    }

     @Test
    fun `processBatch should return Right and not call insert if all records fail parsing`() = runTest {
        // Arrange
        val invalidJson1 = """{"sensorId": "F6",,}"""
        val invalidJson2 = """{"sensorId": "G7"}invalid"""
        val records = listOf(
            ConsumerRecord("test-topic", 0, 105, "key6", invalidJson1),
            ConsumerRecord("test-topic", 0, 106, "key7", invalidJson2)
        )

        // Act
        val result = messageProcessor.processBatch(records)

        // Assert
        assertThat(result.isRight()).isTrue() // Successful overall, offsets should be committed

        // Verify repository was NOT called
        coVerify(exactly = 0) { mockRepository.insertBatch(any()) }
    }
}
```

**b) ClickHouse Repository Integration Test (`src/test/kotlin/com/yourcompany/kafka/clickhouse/persistence/ClickHouseRepositoryIntegrationTest.kt`)**

```kotlin
package com.yourcompany.kafka.clickhouse.persistence

import arrow.core.getOrElse
import com.yourcompany.kafka.clickhouse.config.ClickHouseConfig
import com.yourcompany.kafka.clickhouse.data.IngestedData
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.ClickHouseContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager

@Testcontainers // Enable Testcontainers support for JUnit 5
class ClickHouseRepositoryIntegrationTest {

    companion object {
        private const val TEST_DB = "testdb"
        private const val TEST_TABLE = "ingested_data"

        @Container // Define the ClickHouse container
        @JvmStatic // Needs to be static for @Container
        val clickhouseContainer: ClickHouseContainer = ClickHouseContainer("clickhouse/clickhouse-server:latest")
            // .withUsername("testuser") // Optional: set credentials
            // .withPassword("testpass")
            .withDatabaseName(TEST_DB)
            .withInitScript("init-clickhouse.sql") // Load schema from classpath

        private lateinit var repository: ClickHouseRepository
        private lateinit var config: ClickHouseConfig

        @BeforeAll // Runs once before all tests in the class
        @JvmStatic
        fun setup() {
            clickhouseContainer.start() // Start the container manually or let JUnit Jupiter handle it

            config = ClickHouseConfig(
                jdbcUrl = clickhouseContainer.jdbcUrl, // Get dynamic JDBC URL
                user = clickhouseContainer.username,
                pass = clickhouseContainer.password,
                database = TEST_DB, // Use the specific database
                tableName = TEST_TABLE
            )
            // Use the actual implementation with the test container config
            repository = JdbcClickHouseRepository(config, Dispatchers.IO) // Use IO dispatcher
        }

        @AfterAll // Runs once after all tests in the class
        @JvmStatic
        fun teardown() {
            repository.close() // Close repository resources
            clickhouseContainer.stop() // Stop the container
        }
    }

    // Helper function to count rows in the test table
    private fun countTableRows(): Int {
        DriverManager.getConnection(config.jdbcUrl, config.user, config.pass).use { connection ->
            connection.createStatement().use { statement ->
                val rs = statement.executeQuery("SELECT count() FROM $TEST_TABLE")
                rs.next()
                return rs.getInt(1)
            }
        }
    }

     // Optional: Clean table before each test if needed
     @BeforeEach
     fun cleanTable() {
         DriverManager.getConnection(config.jdbcUrl, config.user, config.pass).use { connection ->
             connection.createStatement().use { statement ->
                 // TRUNCATE is faster than DELETE for full table clear if needed
                 statement.execute("TRUNCATE TABLE IF EXISTS $TEST_TABLE")
             }
         }
         assertThat(countTableRows()).isEqualTo(0) // Verify table is empty
     }


    @Test
    fun `insertBatch should insert multiple records into ClickHouse`() = runTest {
        // Arrange
        val data = listOf(
            IngestedData(sensorId = "sensor1", temperature = 10.1, timestamp = "2023-01-01T10:00:00Z"),
            IngestedData(sensorId = "sensor2", value = 50, message = "Data point"),
            IngestedData(sensorId = "sensor1", temperature = 10.5, timestamp = "2023-01-01T10:01:00Z")
        )

        // Act
        val result = repository.insertBatch(data)

        // Assert
        assertThat(result.isRight()).isTrue()
        assertThat(countTableRows()).isEqualTo(data.size) // Verify rows were inserted

        // Optional: Query and verify specific data (more complex)
        DriverManager.getConnection(config.jdbcUrl, config.user, config.pass).use { connection ->
            connection.createStatement().use { statement ->
                val rs = statement.executeQuery("SELECT sensorId, temperature FROM $TEST_TABLE WHERE sensorId = 'sensor1' ORDER BY receivedAt")
                assertThat(rs.next()).isTrue()
                assertThat(rs.getString("sensorId")).isEqualTo("sensor1")
                assertThat(rs.getDouble("temperature")).isEqualTo(10.1)
                assertThat(rs.next()).isTrue()
                 assertThat(rs.getString("sensorId")).isEqualTo("sensor1")
                 assertThat(rs.getDouble("temperature")).isEqualTo(10.5)
                assertThat(rs.next()).isFalse() // Should be only two sensor1 rows
            }
        }
    }

    @Test
    fun `insertBatch should handle empty list gracefully`() = runTest {
        // Arrange
        val data = emptyList<IngestedData>()

        // Act
        val result = repository.insertBatch(data)

        // Assert
        assertThat(result.isRight()).isTrue()
        assertThat(countTableRows()).isEqualTo(0)
    }

    @Test
    fun `insertBatch should handle records with null values`() = runTest {
         // Arrange
         val data = listOf(
             IngestedData(sensorId = "sensor_null", temperature = null, message = "Temp missing"),
             IngestedData(sensorId = null, value = 123) // Sensor ID missing
         )

         // Act
         val result = repository.insertBatch(data)

         // Assert
         assertThat(result.isRight()).isTrue()
         assertThat(countTableRows()).isEqualTo(data.size)

         // Verify nulls were inserted correctly
         DriverManager.getConnection(config.jdbcUrl, config.user, config.pass).use { connection ->
             connection.createStatement().use { statement ->
                 val rs = statement.executeQuery("SELECT sensorId, temperature, value FROM $TEST_TABLE ORDER BY receivedAt")
                 assertThat(rs.next()).isTrue()
                 assertThat(rs.getString("sensorId")).isEqualTo("sensor_null")
                 assertThat(rs.getObject("temperature")).isNull() // Check for SQL NULL
                 assertThat(rs.getString("message")).isEqualTo("Temp missing")

                 assertThat(rs.next()).isTrue()
                 assertThat(rs.getString("sensorId")).isNull()
                 assertThat(rs.getInt("value")).isEqualTo(123)

                 assertThat(rs.next()).isFalse()
             }
         }
    }
}
```
*Self-Correction:* Added `@Testcontainers` and `@Container`. Used `@BeforeAll`/`@AfterAll` for container lifecycle. Fetched dynamic JDBC URL/credentials from the container. Included `withInitScript`. Added a helper to count rows and basic data verification query. Added table truncation in `@BeforeEach`.

---

**12. Dockerfile (`Dockerfile`)**

```dockerfile
# Stage 1: Build the application using Gradle
FROM gradle:8.5-jdk17-alpine AS builder

WORKDIR /app

# Copy only necessary files first to leverage Docker cache
COPY build.gradle.kts settings.gradle.kts ./
COPY gradle ./gradle
COPY gradlew .

# Download dependencies (cacheable layer)
RUN gradle --no-daemon dependencies || true # Allow failure? Maybe just build? Let's try build directly.
RUN gradle --no-daemon :shadowJar --info # Build the shadow JAR

# Copy the rest of the source code
COPY src ./src

# Build the shadow JAR (this will use cached dependencies if possible)
# Ensure shadowJar task is run
RUN gradle --no-daemon clean :shadowJar --info


# Stage 2: Create the final lightweight image
FROM amazoncorretto:17-alpine-jdk

WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /app/build/libs/kotlin-kafka-clickhouse-*.jar ./app.jar

# Expose port if your app had an HTTP interface (not needed for this consumer)
# EXPOSE 8080

# Set the entrypoint command to run the application
# Use exec form for proper signal handling
ENTRYPOINT ["java", "-jar", "./app.jar"]

# Optional: Add a non-root user for security
# RUN addgroup -S appgroup && adduser -S appuser -G appgroup
# USER appuser
```
*Self-Correction:* Used multi-stage build. Fixed gradle command to ensure `shadowJar` runs correctly. Used `amazoncorretto:17-alpine-jdk` for a smaller JRE.

---

**13. Docker Compose (`docker-compose.yml`)**

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    hostname: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - app-net

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    hostname: kafka
    ports:
      - "9092:9092" # External access if needed from host
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: LISTENER_INTERNAL:PLAINTEXT,LISTENER_EXTERNAL:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: LISTENER_INTERNAL://kafka:29092,LISTENER_EXTERNAL://localhost:9092
      KAFKA_LISTENERS: LISTENER_INTERNAL://0.0.0.0:29092,LISTENER_EXTERNAL://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: LISTENER_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      # Ensure the topic from the Go service exists
      KAFKA_CREATE_TOPICS: "ingest-topic:1:1"
    networks:
      - app-net

  clickhouse:
    image: clickhouse/clickhouse-server:latest # Or specific version e.g., 23.8
    container_name: clickhouse_db
    hostname: clickhouse
    ports:
      - "8123:8123" # HTTP interface
      - "9000:9000" # Native TCP interface
    environment:
      # Default user is 'default' with no password
      # CLICKHOUSE_USER: user # Optional: Set user/pass/db if needed
      # CLICKHOUSE_PASSWORD: password
      CLICKHOUSE_DB: default # Or create a specific one
    volumes:
      # Mount init script to create table on startup
      - ./src/test/resources/init-clickhouse.sql:/docker-entrypoint-initdb.d/init.sql
      # Optional: Persist data
      - clickhouse_data:/var/lib/clickhouse
    ulimits: # Recommended for ClickHouse
      nofile:
        soft: 262144
        hard: 262144
    networks:
      - app-net
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8123/ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  # The Kotlin Consumer Application
  app-consumer:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: kotlin_consumer
    depends_on:
      kafka:
        condition: service_started # Basic check, Kafka healthcheck is better
      clickhouse:
        condition: service_healthy # Wait for ClickHouse healthcheck
    environment:
      # Kafka Config
      KAFKA_BROKERS: "kafka:29092" # Internal listener
      KAFKA_TOPIC: "ingest-topic" # MUST match the producer's topic
      KAFKA_GROUP_ID: "clickhouse-inserter-group" # Consumer group ID
      # KAFKA_POLL_TIMEOUT_MS: 1000 # Optional: Override default

      # ClickHouse Config (using default user/pass/db defined in clickhouse service)
      CLICKHOUSE_JDBC_URL: "jdbc:clickhouse://clickhouse:8123/default" # Use service name, HTTP port
      # CLICKHOUSE_USER: "default" # Default
      # CLICKHOUSE_PASSWORD: ""    # Default
      CLICKHOUSE_DATABASE: "default" # DB name included in URL is often enough
      CLICKHOUSE_TABLE: "ingested_data" # Table name (must match init script)

      # Optional: Java options for tuning
      # JAVA_OPTS: "-Xmx512m -Xms256m"
    restart: unless-stopped
    networks:
      - app-net

networks:
  app-net:
    driver: bridge

volumes:
  clickhouse_data:
    driver: local
```

---

**How to Run:**

1.  **Prerequisites:** Docker and Docker Compose installed.
2.  **Build & Run:**
    ```bash
    docker compose up --build -d
    ```
    *   `--build` forces a rebuild of the `app-consumer` image.
    *   `-d` runs in detached mode.
3.  **Check Logs:**
    ```bash
    docker compose logs -f app-consumer
    docker compose logs -f clickhouse
    ```
4.  **Produce Data:** Use the Go service (or a simple Kafka CLI producer) to send JSON messages to the `ingest-topic`.
5.  **Verify in ClickHouse:** Connect to ClickHouse (e.g., using `docker compose exec clickhouse_db clickhouse-client`) and query the `ingested_data` table:
    ```sql
    SELECT * FROM ingested_data LIMIT 10;
    SELECT count() FROM ingested_data;
    ```
6.  **Stop:**
    ```bash
    docker compose down
    ```

This setup provides a lightweight, non-blocking (where feasible with JDBC), functional Kotlin consumer with proper error handling using Arrow-kt, along with testing and deployment infrastructure. Remember to adapt the `IngestedData` class and the ClickHouse table schema (`init-clickhouse.sql` and repository `insertSql`) to your exact JSON payload structure.