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