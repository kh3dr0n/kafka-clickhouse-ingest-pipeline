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