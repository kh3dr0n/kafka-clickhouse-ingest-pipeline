package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher defines the interface for publishing messages.
type Publisher interface {
	Publish(ctx context.Context, key []byte, value []byte) error
	Close() error
}

// KafkaPublisher sends messages to a Kafka topic.
type KafkaPublisher struct {
	writer *kafka.Writer
	topic  string
}

// NewKafkaPublisher creates a new Kafka publisher.
func NewKafkaPublisher(brokers string, topic string) (*KafkaPublisher, error) {
	brokerList := strings.Split(brokers, ",")
	if len(brokerList) == 0 || brokerList[0] == "" {
		return nil, fmt.Errorf("kafka brokers list is empty or invalid")
	}
	log.Printf("Connecting to Kafka brokers: %v", brokerList)

	// Configure the Kafka writer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafka.RequireOne,
		// ******* CHANGE HERE *******
		Async: true, // Make publishing asynchronous
		// ***************************
		// Optional: Configure batching for higher throughput in async mode
		BatchSize:    100,             // Number of messages per batch
		BatchTimeout: 1 * time.Second, // Max time to wait before sending a batch
		// Optional: Add error logging for async errors (if you *do* want to see them)
		// ErrorLogger: kafka.LoggerFunc(log.Printf), // Log async errors
	}

	// Note: In async mode, WriteMessages rarely returns an error unless configuration is bad.
	// Actual send errors happen in the background. Close() handles flushing.

	log.Printf("Kafka writer configured for ASYNCHRONOUS publishing to topic '%s'", topic)
	return &KafkaPublisher{writer: writer, topic: topic}, nil
}

// Publish sends a message to the configured Kafka topic asynchronously.
func (p *KafkaPublisher) Publish(ctx context.Context, key []byte, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
	}

	// In async mode, this call returns quickly. Errors (like network issues)
	// might occur later and are typically handled during Close() or via Stats/ErrorLogger.
	// We are intentionally *not* handling async errors here per the request ("not care").
	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		// This error is likely a configuration issue or context cancellation,
		// not a typical broker/network error which happens in the background.
		log.Printf("Immediate error during async WriteMessages (rare): %v", err)
		// We still return the error here, as it might indicate a fundamental problem.
		return fmt.Errorf("kafka immediate write error: %w", err)
	}
	// Log that we queued it, not that it was necessarily sent successfully yet.
	log.Printf("Message queued for async publishing to Kafka topic '%s', size: %d bytes", p.topic, len(value))
	return nil
}

// Close closes the Kafka writer connection.
// For async writers, Close() attempts to flush all buffered messages.
func (p *KafkaPublisher) Close() error {
	log.Println("Closing Kafka writer (attempting to flush buffered messages)...")
	startTime := time.Now()
	err := p.writer.Close()
	duration := time.Since(startTime)
	if err != nil {
		log.Printf("Error closing Kafka writer (flushing may have failed): %v (duration: %s)", err, duration)
		return err // Propagate the error
	}
	log.Printf("Kafka writer closed successfully. (duration: %s)", duration)
	return nil
}
