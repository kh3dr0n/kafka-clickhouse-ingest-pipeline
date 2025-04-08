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