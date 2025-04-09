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