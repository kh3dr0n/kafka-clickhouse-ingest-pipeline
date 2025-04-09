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