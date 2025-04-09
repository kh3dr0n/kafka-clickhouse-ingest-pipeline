package com.yourcompany.kafka.clickhouse.processing

import arrow.core.Either
import arrow.core.right
import com.yourcompany.kafka.clickhouse.data.IngestedData
import com.yourcompany.kafka.clickhouse.persistence.ClickHouseRepository
import io.mockk.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MessageProcessorTest {

    private lateinit var mockRepository: ClickHouseRepository
    private lateinit var messageProcessor: DefaultMessageProcessor

    // Test data classes - simpler for verification
    @kotlinx.serialization.Serializable data class SimpleData(val id: String, val count: Int)

    @BeforeEach
    fun setUp() {
        mockRepository = mockk()
        messageProcessor = DefaultMessageProcessor(mockRepository, Json { ignoreUnknownKeys = true }) // Use same Json config
    }

    @Test
    fun `processBatch should parse valid records and call repository insertBatch`() = runTest {
        // Arrange
        val validJson1 = """{"sensorId": "A1", "temperature": 25.5}"""
        val validJson2 = """{"sensorId": "B2", "message": "OK"}"""
        val records = listOf(
            ConsumerRecord("test-topic", 0, 100, "key1", validJson1),
            ConsumerRecord("test-topic", 0, 101, "key2", validJson2)
        )

        val expectedData = listOf(
            IngestedData(sensorId = "A1", temperature = 25.5),
            IngestedData(sensorId = "B2", message = "OK")
        )

        // Mock repository interaction (suspend functions need coEvery)
        coEvery { mockRepository.insertBatch(any()) } returns Unit.right() // Mock successful insertion

        // Act
        val result = messageProcessor.processBatch(records)

        // Assert
        assertThat(result.isRight()).isTrue() // Expect overall success

        // Verify repository was called with the correctly parsed data
        val capturedData = slot<List<IngestedData>>()
        coVerify(exactly = 1) { mockRepository.insertBatch(capture(capturedData)) }

        // Use AssertJ's recursive comparison (add dependency if needed, or compare manually)
        // Ensure the order doesn't matter if your insert logic doesn't depend on it
        assertThat(capturedData.captured).containsExactlyInAnyOrderElementsOf(expectedData)
    }

    @Test
    fun `processBatch should handle mixed valid and invalid records and insert valid ones`() = runTest {
        // Arrange
        val validJson = """{"sensorId": "C3", "value": 99}"""
        val invalidJson = """{"sensorId": "D4", "value":}""" // Invalid JSON
        val records = listOf(
            ConsumerRecord("test-topic", 0, 102, "key3", validJson),
            ConsumerRecord("test-topic", 0, 103, "key4", invalidJson)
        )

        val expectedInsertedData = listOf(
            IngestedData(sensorId = "C3", value = 99)
        )

        coEvery { mockRepository.insertBatch(any()) } returns Unit.right()

        // Act
        val result = messageProcessor.processBatch(records)

        // Assert
        assertThat(result.isRight()).isTrue() // Still successful overall (parsing errors logged)

        val capturedData = slot<List<IngestedData>>()
        coVerify(exactly = 1) { mockRepository.insertBatch(capture(capturedData)) }
        assertThat(capturedData.captured).containsExactlyElementsOf(expectedInsertedData)
    }

    @Test
    fun `processBatch should return Left if repository insertion fails`() = runTest {
        // Arrange
        val validJson = """{"sensorId": "E5"}"""
        val records = listOf(ConsumerRecord("test-topic", 0, 104, "key5", validJson))
        val dbError = RuntimeException("ClickHouse connection failed")

        // Mock repository failure
        coEvery { mockRepository.insertBatch(any()) } returns Either.Left(dbError)

        // Act
        val result = messageProcessor.processBatch(records)

        // Assert
        assertThat(result.isLeft()).isTrue()
        assertThat(result.leftOrNull()).isEqualTo(dbError)

        // Verify insertion was attempted
        coVerify(exactly = 1) { mockRepository.insertBatch(any()) }
    }

     @Test
    fun `processBatch should return Right and not call insert if all records fail parsing`() = runTest {
        // Arrange
        val invalidJson1 = """{"sensorId": "F6",,}"""
        val invalidJson2 = """{"sensorId": "G7"}invalid"""
        val records = listOf(
            ConsumerRecord("test-topic", 0, 105, "key6", invalidJson1),
            ConsumerRecord("test-topic", 0, 106, "key7", invalidJson2)
        )

        // Act
        val result = messageProcessor.processBatch(records)

        // Assert
        assertThat(result.isRight()).isTrue() // Successful overall, offsets should be committed

        // Verify repository was NOT called
        coVerify(exactly = 0) { mockRepository.insertBatch(any()) }
    }
}