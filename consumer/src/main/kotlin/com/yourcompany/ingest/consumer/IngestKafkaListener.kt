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