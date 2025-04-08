Okay, let's create a Kotlin/JVM service using Spring Boot to consume from the Kafka topic `ingest-topic` and write the data to ClickHouse in batches.

**Project Structure (Gradle):**

```
.
├── build.gradle.kts
├── gradlew
├── gradlew.bat
├── settings.gradle.kts
├── src
│   ├── main
│   │   ├── kotlin
│   │   │   └── com/yourcompany/ingest
│   │   │       ├── ClickhouseIngestApplication.kt  # Main Application
│   │   │       ├── config
│   │   │       │   ├── AppProperties.kt            # Configuration Properties
│   │   │       │   ├── ClickHouseConfig.kt         # ClickHouse DataSource Bean
│   │   │       │   └── KafkaConsumerConfig.kt      # Kafka Consumer Configuration
│   │   │       ├── consumer
│   │   │       │   └── IngestKafkaListener.kt      # The Kafka @KafkaListener
│   │   │       └── service
│   │   │           └── ClickHouseWriterService.kt  # Service for ClickHouse writes
│   │   └── resources
│   │       └── application.yml                     # Configuration file
│   └── test
│       └── kotlin/...                            # Tests (Optional for now)
├── Dockerfile                                      # Dockerfile for the service
└── docker-compose.yml                              # Docker Compose for dependencies
```

---

**1. `build.gradle.kts`**

```kotlin
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "3.2.4" // Use a recent stable Spring Boot version
    id("io.spring.dependency-management") version "1.1.4"
    kotlin("jvm") version "1.9.23" // Use a recent stable Kotlin version
    kotlin("plugin.spring") version "1.9.23"
}

group = "com.yourcompany"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17 // Or newer (21 recommended)
}

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot Core
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-actuator") // For health checks etc.

    // Kotlin
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib") // Explicit stdlib dependency

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")

    // JSON Processing (Jackson - included transitively by spring-boot-starter)
    // implementation("com.fasterxml.jackson.module:jackson-module-kotlin") // Included by default now

    // ClickHouse & JDBC
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    // Use the official recommended ClickHouse JDBC driver
    // implementation("ru.yandex.clickhouse:clickhouse-jdbc:0.5.1") // Older driver
    implementation("com.clickhouse:clickhouse-jdbc:0.6.0") // Newer driver (use 'http' classifier for basic http interface) {
    {
       // classifier = "http" // If using HTTP interface, else omit for native TCP
    }


    // Configuration Processor (optional, helps IDE with application.yml)
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    // Testing (optional but recommended)
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
}

dependencyManagement {
    imports {
        mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES)
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs += "-Xjsr305=strict"
        jvmTarget = "17" // Match java.sourceCompatibility
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// Make sure Spring Boot builds an executable JAR
tasks.getByName<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = true
}

tasks.getByName<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    enabled = true
}
```

**2. `settings.gradle.kts`**

```kotlin
rootProject.name = "clickhouse-ingest"
```

---

**3. `src/main/resources/application.yml`**

```yaml
server:
  port: 8090 # Different port than the Go service

spring:
  application:
    name: clickhouse-ingest-service

  # Kafka Consumer Configuration
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092} # Get from env var or default
    consumer:
      group-id: clickhouse-ingest-group # Unique consumer group ID
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer # Consume raw JSON strings
      auto-offset-reset: earliest # Start consuming from the beginning if no offset found
      properties:
        # Optional: Adjust isolation level if needed (read_committed requires transactional producer)
        # isolation.level: read_committed
        # Optional: Increase poll timeout if processing takes longer
        # max.poll.interval.ms: 600000 # 10 minutes
        fetch.min.bytes: 1
        fetch.max.wait.ms: 500
    listener:
      # Enable batch listening
      type: batch
      # Number of concurrent consumers (threads) processing partitions for the topic
      concurrency: ${KAFKA_CONSUMER_CONCURRENCY:1}
      # Optional: Adjust ack mode if needed (default is BATCH)
      # ack-mode: BATCH

  # ClickHouse DataSource Configuration
  datasource:
    clickhouse: # Custom namespace to avoid conflict if using other datasources
      # Get from env var or default (use 'clickhouse' hostname from docker-compose)
      url: jdbc:clickhouse://${CLICKHOUSE_HOST:localhost}:${CLICKHOUSE_PORT:8123}/${CLICKHOUSE_DATABASE:default}?${CLICKHOUSE_PARAMS:} # JDBC URL
      username: ${CLICKHOUSE_USER:default}
      password: ${CLICKHOUSE_PASSWORD:} # Provide password via env var or keep empty if none
      driver-class-name: com.clickhouse.jdbc.ClickHouseDriver # For the newer com.clickhouse driver
      # driver-class-name: ru.yandex.clickhouse.ClickHouseDriver # For the older ru.yandex driver
      # Optional connection pool settings (using HikariCP by default via spring-boot-starter-jdbc)
      hikari:
        maximum-pool-size: ${CLICKHOUSE_POOL_SIZE:10}
        connection-timeout: 30000 # 30 seconds
        idle-timeout: 600000 # 10 minutes
        max-lifetime: 1800000 # 30 minutes

# Application Specific Properties
app:
  kafka:
    ingest-topic: ${KAFKA_INGEST_TOPIC:ingest-topic}
  clickhouse:
    ingest-table: ${CLICKHOUSE_INGEST_TABLE:ingest_data} # Target ClickHouse table name
    batch-size: ${CLICKHOUSE_BATCH_SIZE:100} # Number of records to batch insert

management:
  endpoints:
    web:
      exposure:
        include: "health,info,prometheus" # Expose useful actuator endpoints
  endpoint:
    health:
      show-details: when_authorized
```

---

**4. `src/main/kotlin/com/yourcompany/ingest/config/AppProperties.kt`**

```kotlin
package com.yourcompany.ingest.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import org.springframework.validation.annotation.Validated
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank

@Component
@ConfigurationProperties(prefix = "app")
@Validated
data class AppProperties(
    val kafka: KafkaAppProperties = KafkaAppProperties(),
    val clickhouse: ClickHouseAppProperties = ClickHouseAppProperties()
)

data class KafkaAppProperties(
    @field:NotBlank
    var ingestTopic: String = "ingest-topic"
)

data class ClickHouseAppProperties(
    @field:NotBlank
    var ingestTable: String = "ingest_data",

    @field:Min(1)
    var batchSize: Int = 100
)
```

*Note: You might need to add `implementation("org.springframework.boot:spring-boot-starter-validation")` to `build.gradle.kts` for `@Validated` annotations.*

---

**5. `src/main/kotlin/com/yourcompany/ingest/config/ClickHouseConfig.kt`**

```kotlin
package com.yourcompany.ingest.config

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import javax.sql.DataSource

@Configuration
class ClickHouseConfig {

    // Define properties specifically for the ClickHouse datasource
    @Bean
    @Primary // Mark this as the primary DataSourceProperties if you only have one DB
    @ConfigurationProperties("spring.datasource.clickhouse")
    fun clickhouseDataSourceProperties(): DataSourceProperties {
        return DataSourceProperties()
    }

    // Create the DataSource bean using the properties
    @Bean
    @Primary // Mark this as the primary DataSource if you only have one DB
    @ConfigurationProperties("spring.datasource.clickhouse.hikari") // Apply Hikari props
    fun clickhouseDataSource(clickhouseDataSourceProperties: DataSourceProperties): DataSource {
        return clickhouseDataSourceProperties
            .initializeDataSourceBuilder()
            // .type(HikariDataSource::class.java) // Usually auto-detected
            .build()
    }

    // Create a JdbcTemplate bean specifically for ClickHouse
    @Bean
    fun clickhouseJdbcTemplate(clickhouseDataSource: DataSource): JdbcTemplate {
        return JdbcTemplate(clickhouseDataSource)
    }
}
```

---

**6. `src/main/kotlin/com/yourcompany/ingest/config/KafkaConsumerConfig.kt`**

```kotlin
package com.yourcompany.ingest.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.DefaultErrorHandler // Or use CommonErrorHandler for more control
import org.springframework.util.backoff.FixedBackOff // For retry policies


@EnableKafka // Necessary to detect @KafkaListener annotations
@Configuration
class KafkaConsumerConfig(private val kafkaProperties: KafkaProperties) {

    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = DefaultKafkaConsumerFactory(consumerConfigs())
        factory.isBatchListener = true // IMPORTANT: Enable batch listening
        factory.setCommonErrorHandler(kafkaErrorHandler()) // Configure error handling
        // factory.containerProperties.ackMode = ContainerProperties.AckMode.BATCH // Default
        // Optional: Set concurrency based on application.yml or default
        factory.setConcurrency(kafkaProperties.listener.concurrency ?: 1)
        log.info("Configured Kafka Listener Container Factory with concurrency: {}", factory.concurrency)
        return factory
    }

    @Bean
    fun consumerConfigs(): Map<String, Any> {
        // Start with Spring Boot's auto-configured properties
        val props = HashMap(kafkaProperties.buildConsumerProperties(null)) // Pass null for default SslBundleRegistry
        // Override or add specific properties if needed (some might be redundant if set in application.yml)
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = kafkaProperties.consumer.autoOffsetReset ?: "earliest"
        props[ConsumerConfig.GROUP_ID_CONFIG] = kafkaProperties.consumer.groupId ?: "default-clickhouse-group"
        // Ensure batch size hint aligns somewhat with ClickHouse batch size (doesn't strictly enforce poll size)
        // props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = appProperties.clickhouse.batchSize * (kafkaProperties.listener.concurrency ?: 1)

        log.info("Kafka Consumer Properties: {}", props.filterKeys { it != ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG }) // Don't log brokers usually
        return props
    }

    @Bean
    fun kafkaErrorHandler(): CommonErrorHandler {
        // Configure an error handler. This example uses a default that logs errors
        // and stops processing the batch (offsets won't be committed) upon failure.
        // You can add retries with backoff or send to a Dead Letter Topic (DLT).

        // Example: Simple logging error handler (DefaultErrorHandler is often sufficient)
        // return DefaultErrorHandler() // Simplest option, logs and seeks the batch for reprocessing on next poll

        // Example: Retry with fixed backoff (use with caution, can block consumer thread)
        // val backOff = FixedBackOff(1000L, 3L) // 1 sec interval, max 3 attempts
        // val errorHandler = DefaultErrorHandler(backOff)
        // errorHandler.setRetryListeners(KafkaRetryListener()) // Add custom listener actions on retry/failure
        // return errorHandler

        // For this example, we'll use the simpler DefaultErrorHandler
        log.info("Using DefaultErrorHandler for Kafka batch listener.")
        return DefaultErrorHandler()
    }

    // Optional: Custom listener for retry attempts
    /*
    class KafkaRetryListener : RetryListener {
        private val retryLog = LoggerFactory.getLogger(KafkaRetryListener::class.java)
        override fun KfailedDelivery(record: ConsumerRecord<*, *>?, ex: Exception?, deliveryAttempt: Int) {
            retryLog.warn("Kafka record delivery failed. Attempt: {}. Record: {}, Exception: {}", deliveryAttempt, record?.value(), ex?.message)
        }
         override fun recovered(record: ConsumerRecord<*, *>?, ex: Exception?) {
              retryLog.info("Kafka record delivery recovered successfully. Record: {}", record?.value())
         }
         override fun recoveryFailed(record: ConsumerRecord<*, *>?, original: Exception?, failure: Exception?) {
              retryLog.error("Kafka record recovery failed after retries. Record: {}, Original Exception: {}, Failure Exception: {}", record?.value(), original?.message, failure?.message)
              // Consider sending to DLT here
         }
    }
    */
}
```

---

**7. `src/main/kotlin/com/yourcompany/ingest/service/ClickHouseWriterService.kt`**

```kotlin
package com.yourcompany.ingest.service

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.yourcompany.ingest.config.AppProperties
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional // Optional: If complex logic needs transactions

@Service
class ClickHouseWriterService(
    private val jdbcTemplate: JdbcTemplate,
    private val objectMapper: ObjectMapper, // Auto-configured by Spring Boot
    private val appProperties: AppProperties
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapTypeRef = object : TypeReference<Map<String, Any>>() {}

    // --- Configuration ---
    // Lazily build the insert statement only once
    private val insertSql: String by lazy { buildInsertSql() }
    private val columnNames: List<String> by lazy { extractColumnNamesFromData(sampleDataForColumns()) }


    /**
     * Inserts a batch of data into ClickHouse. Assumes data is a list of Maps where keys match ClickHouse columns.
     * Throws DataAccessException if the batch insert fails.
     */
    fun writeBatch(jsonDataList: List<String>) {
        if (jsonDataList.isEmpty()) {
            log.debug("Received empty list, nothing to insert.")
            return
        }

        val dataMaps = parseJsonBatch(jsonDataList)
        if (dataMaps.isEmpty()) {
             log.warn("No valid JSON data parsed from the batch of size {}, skipping insert.", jsonDataList.size)
             return
        }


        if (columnNames.isEmpty()) {
            log.error("Could not determine ClickHouse column names. Skipping insert.")
            // Or potentially throw an exception based on desired behavior
            return
        }

        log.info("Attempting to batch insert {} records into ClickHouse table '{}'", dataMaps.size, appProperties.clickhouse.ingestTable)

        try {
            val batchArgs = dataMaps.map { map ->
                // Ensure values are extracted in the same order as columnNames
                columnNames.map { colName -> map[colName] }.toTypedArray()
            }

            // Use batchUpdate for efficient batching
            val rowsAffected = jdbcTemplate.batchUpdate(insertSql, batchArgs)

            val totalAffected = rowsAffected.sum()
            log.info("Successfully inserted batch. Total rows affected: {}", totalAffected)
            if(totalAffected != dataMaps.size) {
                log.warn("Number of rows affected ({}) does not match batch size ({}). Check data.", totalAffected, dataMaps.size)
            }

        } catch (e: DataAccessException) {
            log.error("Error during ClickHouse batch insert for table '{}': {}", appProperties.clickhouse.ingestTable, e.message, e)
            // Re-throw the exception so the Kafka listener's error handler can manage it (e.g., prevent offset commit)
            throw e
        } catch (e: Exception) {
             log.error("Unexpected error preparing data for ClickHouse batch insert: {}", e.message, e)
             // Wrap in DataAccessException or a custom exception to signal failure
             throw DataAccessException("Data preparation error for ClickHouse insert", e) {}
        }
    }

    private fun parseJsonBatch(jsonDataList: List<String>): List<Map<String, Any>> {
         return jsonDataList.mapNotNull { jsonString ->
            try {
                 objectMapper.readValue(jsonString, mapTypeRef)
            } catch (e: Exception) {
                log.warn("Failed to parse JSON, skipping record. Error: {}, JSON: {}", e.message, jsonString.take(200)) // Log truncated JSON
                null // Return null for invalid records, mapNotNull will filter them out
            }
        }
    }

    // --- Helper methods for SQL generation ---

    // !! IMPORTANT !!
    // Dynamically determining columns from the *first* message can be fragile.
    // It's STRONGLY recommended to either:
    // 1. Have a predefined list of expected columns.
    // 2. Query ClickHouse schema (`system.columns`) on startup (more robust).
    // This example uses a simplified approach assuming the first record is representative.
    private fun sampleDataForColumns(): Map<String, Any>? {
        // Ideally, get this from configuration or schema introspection
        // For now, parse the first non-null map we can find in a sample batch
        // This is just a placeholder - replace with a better mechanism!
         log.warn("Attempting to infer column names from sample data. Consider defining columns explicitly.")
         // A better approach would be a configured list or schema query.
         // Placeholder: return mapOf("sensor_id" to "", "temperature" to 0.0, "timestamp" to "")
         return null // Indicate that columns should be defined elsewhere or queried.
    }

     private fun extractColumnNamesFromData(sampleData: Map<String, Any>?): List<String> {
         // Define your columns EXPLICITLY here for robustness
         val predefinedColumns = listOf(
            "sensor_id",
            "temperature",
            "timestamp",
            // Add ALL columns you expect in your ClickHouse table IN ORDER
            "humidity",
            "location"
            // ... etc
         )
        log.info("Using predefined column list for ClickHouse insert: {}", predefinedColumns)
        return predefinedColumns

        // // Fallback / Alternative: Infer from sample (less robust)
        // if (sampleData != null && sampleData.isNotEmpty()) {
        //     val columns = sampleData.keys.sorted() // Sort for consistent order
        //     log.info("Inferred column names from sample data: {}", columns)
        //     return columns
        // } else {
        //     log.error("Cannot determine column names for ClickHouse insert. No sample data or predefined list available.")
        //     return emptyList()
        // }
    }

    private fun buildInsertSql(): String {
         if (columnNames.isEmpty()) {
             log.error("Cannot build INSERT SQL: Column names are not determined.")
             // Return a non-functional SQL or throw an exception
             return "SELECT 1" // Placeholder to avoid null issues, but indicates failure
         }
        val cols = columnNames.joinToString(", ")
        val placeholders = columnNames.map { "?" }.joinToString(", ")
        val sql = "INSERT INTO ${appProperties.clickhouse.ingestTable} ($cols) VALUES ($placeholders)"
        log.info("Built ClickHouse INSERT statement: {}", sql)
        return sql
    }
}
```

**Important:** The `ClickHouseWriterService` has a placeholder for determining column names. **You absolutely must replace the sample/inference logic with a reliable method:**
    *   **Best:** Define the list of column names explicitly within the service or load them from configuration. This ensures the order matches your `INSERT` statement and ClickHouse table. (The code above now includes this as the primary approach).
    *   **Good:** Query `system.columns` for your target table in ClickHouse on application startup to get the exact columns and their order.

---

**8. `src/main/kotlin/com/yourcompany/ingest/consumer/IngestKafkaListener.kt`**

```kotlin
package com.yourcompany.ingest.consumer

import com.yourcompany.ingest.config.AppProperties
import com.yourcompany.ingest.service.ClickHouseWriterService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component
class IngestKafkaListener(
    private val clickHouseWriterService: ClickHouseWriterService,
    private val appProperties: AppProperties
) {

    private val log = LoggerFactory.getLogger(javaClass)

    // Configure the listener to use the batch factory and listen to the correct topic
    @KafkaListener(
        topics = ["#{appProperties.kafka.ingestTopic}"], // Use SpEL to get topic from config
        groupId = "\${spring.kafka.consumer.group-id}", // Use property placeholder
        containerFactory = "kafkaListenerContainerFactory" // Reference the bean name from KafkaConsumerConfig
    )
    fun handleIngestBatch(
        messages: List<String>, // Receive batch of raw JSON strings
        @Header(KafkaHeaders.RECEIVED_TOPIC) topics: List<String>,
        @Header(KafkaHeaders.OFFSET) offsets: List<Long>,
        @Header(KafkaHeaders.GROUP_ID) groupId: String
    ) {
        val batchSize = messages.size
        if (batchSize == 0) {
            log.debug("Received empty batch from Kafka.")
            return
        }

        val firstOffset = offsets.firstOrNull() ?: -1
        val lastOffset = offsets.lastOrNull() ?: -1
        val topic = topics.firstOrNull() ?: "unknown" // Should generally be the same topic

        log.info(
            "Received Kafka batch. Group='{}', Topic='{}', Size={}, Offset Range=[{}-{}]",
            groupId, topic, batchSize, firstOffset, lastOffset
        )

        try {
            // Delegate the actual writing (including parsing) to the service
            clickHouseWriterService.writeBatch(messages)
            log.info("Successfully processed Kafka batch. Topic='{}', Size={}, Offset Range=[{}-{}]", topic, batchSize, firstOffset, lastOffset)
        } catch (e: Exception) {
            // Error should have been logged in ClickHouseWriterService
            log.error(
                "Failed to process Kafka batch due to error from writer service. Topic='{}', Size={}, Offset Range=[{}-{}]. Error: {}",
                topic, batchSize, firstOffset, lastOffset, e.message
            )
            // IMPORTANT: Throw the exception! This signals to Spring Kafka's ErrorHandler
            // that processing failed, preventing offsets from being committed for this batch.
            throw e // Let the configured CommonErrorHandler handle this failure.
        }
    }
}
```

---

**9. `src/main/kotlin/com/yourcompany/ingest/ClickhouseIngestApplication.kt`**

```kotlin
package com.yourcompany.ingest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ClickhouseIngestApplication

fun main(args: Array<String>) {
    runApplication<ClickhouseIngestApplication>(*args)
}
```

---

**10. `Dockerfile`**

```dockerfile
# Stage 1: Build the application using Gradle
FROM gradle:8.5.0-jdk17 AS builder

WORKDIR /app

# Copy only necessary files for dependency download first
COPY build.gradle.kts settings.gradle.kts gradlew ./
COPY gradle ./gradle

# Download dependencies (this layer is cached if build files don't change)
# RUN gradle build --no-daemon || return 0  # Allow build to continue to copy source
RUN gradle dependencies --no-daemon

# Copy the source code
COPY src ./src

# Build the application JAR
RUN gradle bootJar --no-daemon


# Stage 2: Create the final lightweight image
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Copy the executable JAR from the builder stage
COPY --from=builder /app/build/libs/*.jar /app/app.jar

# Expose the application port
EXPOSE 8090

# Environment variables (can be overridden at runtime)
ENV KAFKA_BROKERS="kafka:29092"
ENV CLICKHOUSE_HOST="clickhouse"
ENV CLICKHOUSE_PORT="8123"
ENV CLICKHOUSE_DATABASE="default"
ENV CLICKHOUSE_USER="default"
ENV CLICKHOUSE_PASSWORD="" # Provide via docker run -e or docker-compose
ENV KAFKA_INGEST_TOPIC="ingest-topic"
ENV CLICKHOUSE_INGEST_TABLE="ingest_data"
ENV CLICKHOUSE_BATCH_SIZE="100"
ENV KAFKA_CONSUMER_CONCURRENCY="1"
# Add any other necessary environment variables

# Run the application
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# Optional: Add health check
# HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
#   CMD wget --quiet --tries=1 --spider http://localhost:8090/actuator/health || exit 1

```

---

**11. `docker-compose.yml`**

This file sets up Zookeeper, Kafka, ClickHouse, and your new Kotlin service.

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
      - ingest-net

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    hostname: kafka
    ports:
      - "9092:9092" # External listener for host access (e.g. Go producer)
      # - "29092:29092" # Internal listener (used by Kotlin consumer)
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
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 100 # Short delay for single broker
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true' # Ensure ingest-topic is created
      # Optional: Explicitly create the topic
      # KAFKA_CREATE_TOPICS: "ingest-topic:1:1"
    networks:
      - ingest-net

  clickhouse:
    # Choose an official image. ':latest' is okay for dev, pin version for prod.
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    hostname: clickhouse
    ports:
      - "8123:8123" # HTTP interface
      - "9000:9000" # Native TCP interface (optional)
    ulimits: # Recommended by ClickHouse docs
      nofile:
        soft: 262144
        hard: 262144
    volumes:
      # Mount a custom config to disable default user password prompt (if needed)
      # - ./clickhouse/config.xml:/etc/clickhouse-server/config.xml
      # Persist data (optional for dev)
      - clickhouse_data:/var/lib/clickhouse
      # Mount init script to create table
      - ./clickhouse/init-db.sh:/docker-entrypoint-initdb.d/init-db.sh
    environment:
      # Set password if needed (corresponds to <password> in users.xml or default user)
      CLICKHOUSE_DEFAULT_USER: ${CLICKHOUSE_USER:-default} # Can be overridden
      CLICKHOUSE_DEFAULT_PASSWORD: ${CLICKHOUSE_PASSWORD:-} # Set via host env or keep empty
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1 # Required if setting user/password via env vars
    networks:
      - ingest-net
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8123/ping"]
      interval: 10s
      timeout: 5s
      retries: 3

  kotlin-consumer:
    build:
      context: . # Assumes docker-compose.yml is in the project root
      dockerfile: Dockerfile
    container_name: kotlin_consumer
    hostname: kotlin-consumer
    ports:
      - "8090:8090" # Expose the app's port
    depends_on:
      kafka:
        condition: service_started # Or use a healthcheck if Kafka image has one
      clickhouse:
        condition: service_healthy
    environment:
      # Override defaults from application.yml or Dockerfile ENV if needed
      SPRING_PROFILES_ACTIVE: docker # Optional: activate specific profile
      KAFKA_BROKERS: "kafka:29092" # Use internal Kafka listener
      CLICKHOUSE_HOST: "clickhouse"
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-} # Pass password from host env
      # Add any other necessary environment variables from application.yml
      APP_CLICKHOUSE_INGEST-TABLE: "ingest_data" # Note Spring Boot env var format
      APP_KAFKA_INGEST-TOPIC: "ingest-topic"
      # JAVA_OPTS: "-Xmx512m -Xms256m" # Optional: Tune JVM memory
    restart: unless-stopped
    networks:
      - ingest-net

networks:
  ingest-net:
    driver: bridge

volumes:
  clickhouse_data:
    driver: local
```

---

**12. ClickHouse Initialization (`clickhouse/init-db.sh`)**

Create a directory `clickhouse` in your project root and add this file. Make sure it's executable (`chmod +x clickhouse/init-db.sh`).

```bash
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

```

**Explanation:**

1.  **Spring Boot Setup:** A standard Spring Boot 3 application using Kotlin.
2.  **Dependencies:** Includes `spring-kafka`, `spring-jdbc`, and the ClickHouse JDBC driver.
3.  **Configuration (`application.yml`):** Defines Kafka connection details, consumer group ID, batch listening, ClickHouse connection details, and application-specific settings like topic name and batch size. Uses environment variables with defaults for flexibility.
4.  **`AppProperties`:** Type-safe configuration class.
5.  **`ClickHouseConfig`:** Creates the `DataSource` and `JdbcTemplate` beans specifically configured for ClickHouse.
6.  **`KafkaConsumerConfig`:** Configures the Kafka consumer factory and listener container factory, enabling batch listening (`isBatchListener = true`) and setting up a basic error handler (`DefaultErrorHandler`).
7.  **`ClickHouseWriterService`:**
    *   Injects `JdbcTemplate`, `ObjectMapper` (for JSON parsing), and `AppProperties`.
    *   Parses incoming JSON strings into `Map<String, Any>`. Handles parsing errors gracefully by logging and skipping invalid records.
    *   **Crucially**, defines the `INSERT` SQL and the expected column names. **You must customize the `extractColumnNamesFromData` method.**
    *   Uses `jdbcTemplate.batchUpdate` to perform efficient batch inserts.
    *   Catches `DataAccessException` and re-throws it to signal failure to the Kafka listener.
8.  **`IngestKafkaListener`:**
    *   Uses `@KafkaListener` to subscribe to the configured topic using the batch container factory.
    *   Receives a `List<String>` of messages.
    *   Calls `clickHouseWriterService.writeBatch`.
    *   If `writeBatch` throws an exception, the listener re-throws it. Spring Kafka's `DefaultErrorHandler` will then typically prevent the offsets for that failed batch from being committed, allowing for reprocessing on the next poll (or retries if configured).
9.  **`Dockerfile`:** Multi-stage build to create a lean container image for the Kotlin service.
10. **`docker-compose.yml`:** Orchestrates Zookeeper, Kafka, ClickHouse, and the Kotlin consumer service, linking them on a network and providing necessary environment variables. Includes a health check for ClickHouse.
11. **`init-db.sh`:** A script automatically run by the ClickHouse container on first start to create the target table (`ingest_data`). **Customize the `CREATE TABLE` statement** to match your data structure precisely.

**To Run:**

1.  Make sure Docker and Docker Compose are installed.
2.  Create the `clickhouse/init-db.sh` file and make it executable.
3.  Define the ClickHouse table schema correctly in `init-db.sh`.
4.  Define the expected ClickHouse columns explicitly in `ClickHouseWriterService.kt`.
5.  (Optional) Set environment variables on your host if you don't want to use the defaults (e.g., `export CLICKHOUSE_PASSWORD=your_secret`).
6.  Run from the project root: `docker compose up --build -d`
7.  Your Go producer service can now send data to `http://localhost:8080/ingest`.
8.  The Kotlin consumer service will start, connect to Kafka (`kafka:29092`), consume messages from `ingest-topic`, and insert them into the `ingest_data` table in ClickHouse (`clickhouse:8123`).
9.  Check logs: `docker compose logs -f kotlin-consumer`
10. Connect to ClickHouse to verify data: `docker compose exec clickhouse clickhouse-client --password your_secret -q "SELECT * FROM ingest_data LIMIT 10"` (adjust password if set).