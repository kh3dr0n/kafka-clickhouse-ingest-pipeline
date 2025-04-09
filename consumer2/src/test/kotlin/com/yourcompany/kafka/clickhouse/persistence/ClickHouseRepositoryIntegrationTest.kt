package com.yourcompany.kafka.clickhouse.persistence

// ... (keep other imports)
import com.yourcompany.kafka.clickhouse.config.ClickHouseConfig
import com.yourcompany.kafka.clickhouse.data.IngestedData
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.ClickHouseContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName // Use this for image names
import java.sql.DriverManager
import java.time.Duration // For startup timeout
import kotlin.test.Test

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClickHouseRepositoryIntegrationTest {

    companion object {
        private const val TEST_DB = "testdb"
        private const val TEST_TABLE = "ingested_data"
        // Use DockerImageName for better parsing and stability
        private val CLICKHOUSE_IMAGE = DockerImageName.parse("clickhouse/clickhouse-server:23.8")
            .asCompatibleSubstituteFor("clickhouse/clickhouse-server")
    }

    @Container
    val clickhouseContainer: ClickHouseContainer = ClickHouseContainer(CLICKHOUSE_IMAGE)
        // Optional: Increase startup timeout if needed, especially on slower machines
        .withStartupTimeout(Duration.ofSeconds(120))
        // REMOVED .withDatabaseName(TEST_DB)
        .withInitScript("init-clickhouse.sql") // Ensure this script runs correctly

    private lateinit var repository: ClickHouseRepository
    private lateinit var config: ClickHouseConfig
    private lateinit var testDbJdbcUrl: String

    @BeforeAll
    fun setup() {
        // Container is started automatically by @Testcontainers before @BeforeAll

        // --- Get connection parameters ---
        // Note: We access username/password here, which *don't* depend on the mapped port
        // and should be available even if the container isn't fully mapped yet.
        val username = clickhouseContainer.username
        val password = clickhouseContainer.password
        var defaultJdbcUrl = "" // Initialize variable

        // --- STEP 1: Ensure container is connectable by creating the database ---
        // This action implicitly waits for the container to be ready enough for a basic connection.
        try {
            // It's safer to get the JDBC URL *after* ensuring connectability.
            // However, we need *some* URL to connect initially to create the DB.
            // Let's try getting it now, hoping the startup process has progressed enough.
            // If this still fails, we might need a more robust wait strategy.
            defaultJdbcUrl = clickhouseContainer.jdbcUrl // Get URL for default DB connection attempt

            println("Attempting connection to default DB: $defaultJdbcUrl") // Logging
            DriverManager.getConnection(defaultJdbcUrl, username, password).use { connection ->
                println("Connection successful. Creating database '$TEST_DB'...") // Logging
                connection.createStatement().use { statement ->
                    statement.execute("CREATE DATABASE IF NOT EXISTS $TEST_DB")
                    println("Database '$TEST_DB' created or already exists.")
                }
            }
        } catch (e: Exception) {
            System.err.println("ERROR: Failed initial connection or database creation for '$TEST_DB'.")
            System.err.println("JDBC URL used: $defaultJdbcUrl") // Log the URL used
            System.err.println("Container running: ${clickhouseContainer.isRunning}") // Check container status
            // Try getting logs if possible (might require adding dependency like slf4j-nop)
            // System.err.println("Container logs: ${clickhouseContainer.logs}")
            e.printStackTrace() // Print stack trace for detailed diagnostics
            throw IllegalStateException("Failed to setup test database in ClickHouse container", e) // Fail fast
        }

        // --- STEP 2: Construct the final JDBC URL for the *test* database ---
        // Now that we know the container is connectable and the DB exists,
        // getting the URL again *should* be safe, and we can construct the final one.
        // Re-getting the default URL ensures we have the *correct* mapped port info.
        val currentDefaultJdbcUrl = clickhouseContainer.jdbcUrl // Get potentially updated URL info
        val baseUrl = currentDefaultJdbcUrl.substringBeforeLast('/')
        testDbJdbcUrl = "$baseUrl/$TEST_DB"

        println("Using JDBC URL for tests: $testDbJdbcUrl")

        // --- STEP 3: Initialize config and repository ---
        config = ClickHouseConfig(
            jdbcUrl = testDbJdbcUrl,
            user = username,
            pass = password,
            database = TEST_DB,
            tableName = TEST_TABLE
        )
        repository = JdbcClickHouseRepository(config, Dispatchers.IO)

        println("Test setup complete.")
    }

    // Helper function now uses the correctly configured testDbJdbcUrl
    private fun countTableRows(): Int {
        DriverManager.getConnection(testDbJdbcUrl, config.user, config.pass).use { connection ->
            connection.createStatement().use { statement ->
                // Fully qualify table name just to be safe, or ensure USE TEST_DB happened
                val rs = statement.executeQuery("SELECT count() FROM $TEST_DB.$TEST_TABLE")
                rs.next()
                return rs.getInt(1)
            }
        }
    }

    // Clean table before each test (uses correct config/URL now)
    @BeforeEach
    fun cleanTable() {
        try {
            DriverManager.getConnection(testDbJdbcUrl, config.user, config.pass).use { connection ->
                connection.createStatement().use { statement ->
                    // TRUNCATE is faster than DELETE for full table clear
                    statement.execute("TRUNCATE TABLE IF EXISTS $TEST_DB.$TEST_TABLE")
                }
            }
            assertThat(countTableRows()).isEqualTo(0) // Verify table is empty
        } catch (e: Exception) {
            System.err.println("ERROR: Failed to truncate table '$TEST_DB.$TEST_TABLE'. Error: ${e.message}")
            throw e
        }
    }

    // ... (Rest of your test class: @AfterAll, helper methods, @Test methods remain the same) ...

    @AfterAll
    fun teardown() {
        // Check if repository was initialized before trying to close
        if (::repository.isInitialized) {
            repository.close()
        }
        // Container stopped by @Testcontainers
    }

    // ... (countTableRows, cleanTable, @Test methods) ...

    @Test
    fun `insertBatch should insert multiple records into ClickHouse`() = runTest {
        // Arrange
        val data = listOf(
            // Adjust IngestedData constructor based on its actual definition
            IngestedData(sensorId = "sensor1", temperature = 10.1, timestamp = "2023-01-01T10:00:00Z"),
            IngestedData(sensorId = "sensor2", value = 50, message = "Data point"),
            IngestedData(sensorId = "sensor1", temperature = 10.5, timestamp = "2023-01-01T10:01:00Z")
        )

        // Act
        val result = repository.insertBatch(data)

        // Assert
        assertThat(result.isRight()).isTrue
        assertThat(countTableRows()).isEqualTo(data.size) // Verify rows were inserted

        // Optional: Query and verify specific data (more complex)
        DriverManager.getConnection(testDbJdbcUrl, config.user, config.pass).use { connection ->
            connection.createStatement().use { statement ->
                // Query from the specific test table
                val rs = statement.executeQuery("SELECT sensorId, temperature FROM $TEST_DB.$TEST_TABLE WHERE sensorId = 'sensor1' ORDER BY timestamp") // Ensure correct order key
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
        DriverManager.getConnection(testDbJdbcUrl, config.user, config.pass).use { connection ->
            connection.createStatement().use { statement ->
                // Query from the specific test table
                val rs = statement.executeQuery("SELECT sensorId, temperature, value, message FROM $TEST_DB.$TEST_TABLE ORDER BY receivedAt") // Assuming receivedAt exists and helps order
                assertThat(rs.next()).isTrue()
                // Order might depend on insertion timing, check both possibilities or make data distinct
                // Assuming first row is sensor_null based on potential insertion order
                if (rs.getString("sensorId") == "sensor_null") {
                    assertThat(rs.getObject("temperature")).isNull() // Check for SQL NULL
                    assertThat(rs.getString("message")).isEqualTo("Temp missing")

                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getString("sensorId")).isNull()
                    assertThat(rs.getInt("value")).isEqualTo(123)
                } else { // The other row came first
                    assertThat(rs.getString("sensorId")).isNull()
                    assertThat(rs.getInt("value")).isEqualTo(123)

                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getString("sensorId")).isEqualTo("sensor_null")
                    assertThat(rs.getObject("temperature")).isNull() // Check for SQL NULL
                    assertThat(rs.getString("message")).isEqualTo("Temp missing")
                }

                assertThat(rs.next()).isFalse()
            }
        }
    }
}