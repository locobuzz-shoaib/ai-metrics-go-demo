// Package consumer provides Kafka consumer functionality for token usage events.
package consumer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/locobuzz-shoaib/cv-go-common-package/pkg/logger"
	"github.com/segmentio/kafka-go"

	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
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
	logger *logger.Logger
}

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(cfg *config.Config, log *logger.Logger) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaGroupID,
		MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	if log != nil {
		log.Info("Kafka consumer initialized",
			"brokers", cfg.KafkaBrokers,
			"topic", cfg.KafkaTopic,
			"group_id", cfg.KafkaGroupID,
		)
	}

	return &KafkaConsumer{
		reader: reader,
		cfg:    cfg,
		logger: log,
	}
}

// ReadEvent reads a single event from Kafka
func (c *KafkaConsumer) ReadEvent(ctx context.Context) (*TokenUsageEvent, error) {
	start := time.Now()
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	var event TokenUsageEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		if c.logger != nil {
			c.logger.Warn("Failed to unmarshal Kafka message",
				"error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
		return nil, err
	}

	// Log Kafka operation using common logger
	if c.logger != nil {
		c.logger.LogKafkaOp(ctx, "consume", msg.Topic, int32(msg.Partition), msg.Offset,
			"duration_ms", time.Since(start).Milliseconds(),
			"trace_id", event.TraceID,
		)
	}

	return &event, nil
}

// Close closes the Kafka reader
func (c *KafkaConsumer) Close() error {
	if c.logger != nil {
		c.logger.Info("Closing Kafka consumer")
	}
	return c.reader.Close()
}

// Stats returns consumer statistics
func (c *KafkaConsumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}
