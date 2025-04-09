package com.yourcompany.kafka.clickhouse

import arrow.core.flatMap
import arrow.core.getOrElse
import arrow.core.right
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
        logger.error("Failed to load configuration: ${error}")
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