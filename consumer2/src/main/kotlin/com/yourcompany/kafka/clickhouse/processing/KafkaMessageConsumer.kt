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