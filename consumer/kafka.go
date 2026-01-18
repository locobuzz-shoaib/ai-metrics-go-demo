// Package consumer provides Kafka consumer functionality for token usage events.
package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
	"github.com/segmentio/kafka-go"
)

// TokenUsageEvent represents the token usage event from the AI Feature Service
type TokenUsageEvent struct {
	EventType        string  `json:"event_type"`
	Timestamp        string  `json:"timestamp"`
	Provider         string  `json:"provider"`
	ModelID          string  `json:"model_id"`
	EndpointName     string  `json:"endpoint_name"`
	InputTokens      int     `json:"input_tokens"`
	OutputTokens     int     `json:"output_tokens"`
	TotalTokens      int     `json:"total_tokens"`
	CachedTokens     int     `json:"cached_tokens"`
	ProcessingTimeMs float64 `json:"processing_time_ms"`
	TraceID          string  `json:"trace_id"`
	Environment      string  `json:"environment"`
}

// KafkaConsumer consumes token usage events from Kafka
type KafkaConsumer struct {
	reader *kafka.Reader
	cfg    *config.Config
}

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(cfg *config.Config) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaGroupID,
		MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	return &KafkaConsumer{
		reader: reader,
		cfg:    cfg,
	}
}

// ReadEvent reads a single event from Kafka
func (c *KafkaConsumer) ReadEvent(ctx context.Context) (*TokenUsageEvent, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	var event TokenUsageEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("Failed to unmarshal event: %v", err)
		return nil, err
	}

	return &event, nil
}

// Close closes the Kafka reader
func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}
