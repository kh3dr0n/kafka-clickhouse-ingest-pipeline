package com.yourcompany.kafka.clickhouse.persistence

import arrow.core.getOrElse
import com.yourcompany.kafka.clickhouse.config.ClickHouseConfig
import com.yourcompany.kafka.clickhouse.data.IngestedData
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.ClickHouseContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager

@Testcontainers // Enable Testcontainers support for JUnit 5
class ClickHouseRepositoryIntegrationTest {

    companion object {
        private const val TEST_DB = "testdb"
        private const val TEST_TABLE = "ingested_data"

        @Container // Define the ClickHouse container
        @JvmStatic // Needs to be static for @Container
        val clickhouseContainer: ClickHouseContainer = ClickHouseContainer("clickhouse/clickhouse-server:latest")
            // .withUsername("testuser") // Optional: set credentials
            // .withPassword("testpass")
            .withDatabaseName(TEST_DB)
            .withInitScript("init-clickhouse.sql") // Load schema from classpath

        private lateinit var repository: ClickHouseRepository
        private lateinit var config: ClickHouseConfig

        @BeforeAll // Runs once before all tests in the class
        @JvmStatic
        fun setup() {
            clickhouseContainer.start() // Start the container manually or let JUnit Jupiter handle it

            config = ClickHouseConfig(
                jdbcUrl = clickhouseContainer.jdbcUrl, // Get dynamic JDBC URL
                user = clickhouseContainer.username,
                pass = clickhouseContainer.password,
                database = TEST_DB, // Use the specific database
                tableName = TEST_TABLE
            )
            // Use the actual implementation with the test container config
            repository = JdbcClickHouseRepository(config, Dispatchers.IO) // Use IO dispatcher
        }

        @AfterAll // Runs once after all tests in the class
        @JvmStatic
        fun teardown() {
            repository.close() // Close repository resources
            clickhouseContainer.stop() // Stop the container
        }
    }

    // Helper function to count rows in the test table
    private fun countTableRows(): Int {
        DriverManager.getConnection(config.jdbcUrl, config.user, config.pass).use { connection ->
            connection.createStatement().use { statement ->
                val rs = statement.executeQuery("SELECT count() FROM $TEST_TABLE")
                rs.next()
                return rs.getInt(1)
            }
        }
    }

     // Optional: Clean table before each test if needed
     @BeforeEach
     fun cleanTable() {
         DriverManager.getConnection(config.jdbcUrl, config.user, config.pass).use { connection ->
             connection.createStatement().use { statement ->
                 // TRUNCATE is faster than DELETE for full table clear if needed
                 statement.execute("TRUNCATE TABLE IF EXISTS $TEST_TABLE")
             }
         }
         assertThat(countTableRows()).isEqualTo(0) // Verify table is empty
     }


    @Test
    fun `insertBatch should insert multiple records into ClickHouse`() = runTest {
        // Arrange
        val data = listOf(
            IngestedData(sensorId = "sensor1", temperature = 10.1, timestamp = "2023-01-01T10:00:00Z"),
            IngestedData(sensorId = "sensor2", value = 50, message = "Data point"),
            IngestedData(sensorId = "sensor1", temperature = 10.5, timestamp = "2023-01-01T10:01:00Z")
        )

        // Act
        val result = repository.insertBatch(data)

        // Assert
        assertThat(result.isRight()).isTrue()
        assertThat(countTableRows()).isEqualTo(data.size) // Verify rows were inserted

        // Optional: Query and verify specific data (more complex)
        DriverManager.getConnection(config.jdbcUrl, config.user, config.pass).use { connection ->
            connection.createStatement().use { statement ->
                val rs = statement.executeQuery("SELECT sensorId, temperature FROM $TEST_TABLE WHERE sensorId = 'sensor1' ORDER BY receivedAt")
                assertThat(rs.next()).isTrue()
                assertThat(rs.getString("sensorId")).isEqualTo("sensor1")
                assertThat(rs.getDouble("temperature")).isEqualTo(10.1)
                assertThat(rs.next()).isTrue()
                 assertThat(rs.getString("sensorId")).isEqualTo("sensor1")
                 assertThat(rs.getDouble("temperature")).isEqualTo(10.5)
                assertThat(rs.next()).isFalse() // Should be only two sensor1 rows
            }
        }
    }

    @Test
    fun `insertBatch should handle empty list gracefully`() = runTest {
        // Arrange
        val data = emptyList<IngestedData>()

        // Act
        val result = repository.insertBatch(data)

        // Assert
        assertThat(result.isRight()).isTrue()
        assertThat(countTableRows()).isEqualTo(0)
    }

    @Test
    fun `insertBatch should handle records with null values`() = runTest {
         // Arrange
         val data = listOf(
             IngestedData(sensorId = "sensor_null", temperature = null, message = "Temp missing"),
             IngestedData(sensorId = null, value = 123) // Sensor ID missing
         )

         // Act
         val result = repository.insertBatch(data)

         // Assert
         assertThat(result.isRight()).isTrue()
         assertThat(countTableRows()).isEqualTo(data.size)

         // Verify nulls were inserted correctly
         DriverManager.getConnection(config.jdbcUrl, config.user, config.pass).use { connection ->
             connection.createStatement().use { statement ->
                 val rs = statement.executeQuery("SELECT sensorId, temperature, value FROM $TEST_TABLE ORDER BY receivedAt")
                 assertThat(rs.next()).isTrue()
                 assertThat(rs.getString("sensorId")).isEqualTo("sensor_null")
                 assertThat(rs.getObject("temperature")).isNull() // Check for SQL NULL
                 assertThat(rs.getString("message")).isEqualTo("Temp missing")

                 assertThat(rs.next()).isTrue()
                 assertThat(rs.getString("sensorId")).isNull()
                 assertThat(rs.getInt("value")).isEqualTo(123)

                 assertThat(rs.next()).isFalse()
             }
         }
    }
}