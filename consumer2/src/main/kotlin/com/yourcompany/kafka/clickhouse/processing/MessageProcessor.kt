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