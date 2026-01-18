// Package storage provides ClickHouse storage functionality for token usage events.
package storage

import (
	"context"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/consumer"
)

// ClickHouseStorage handles storage of token usage events in ClickHouse
type ClickHouseStorage struct {
	conn clickhouse.Conn
	cfg  *config.Config
}

// NewClickHouseStorage creates a new ClickHouse storage connection
func NewClickHouseStorage(cfg *config.Config) (*ClickHouseStorage, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr()},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, err
	}

	// Verify connection
	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	log.Printf("Connected to ClickHouse at %s", cfg.ClickHouseAddr())
	return &ClickHouseStorage{conn: conn, cfg: cfg}, nil
}

// InsertEvent inserts a single token usage event into ClickHouse
func (s *ClickHouseStorage) InsertEvent(ctx context.Context, event *consumer.TokenUsageEvent) error {
	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		// Try alternative ISO format
		timestamp, err = time.Parse("2006-01-02T15:04:05.000Z", event.Timestamp)
		if err != nil {
			timestamp = time.Now()
		}
	}

	return s.conn.Exec(ctx, `
		INSERT INTO ai_token_usage
		(timestamp, provider, model_id, endpoint_name, input_tokens, output_tokens,
		 total_tokens, cached_tokens, processing_time_ms, trace_id, environment)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		timestamp,
		event.Provider,
		event.ModelID,
		event.EndpointName,
		event.InputTokens,
		event.OutputTokens,
		event.TotalTokens,
		event.CachedTokens,
		event.ProcessingTimeMs,
		event.TraceID,
		event.Environment,
	)
}

// InsertBatch inserts a batch of events into ClickHouse
func (s *ClickHouseStorage) InsertBatch(ctx context.Context, events []*consumer.TokenUsageEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO ai_token_usage
		(timestamp, provider, model_id, endpoint_name, input_tokens, output_tokens,
		 total_tokens, cached_tokens, processing_time_ms, trace_id, environment)
	`)
	if err != nil {
		return err
	}

	for _, event := range events {
		// Parse timestamp
		timestamp, err := time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			timestamp, err = time.Parse("2006-01-02T15:04:05.000Z", event.Timestamp)
			if err != nil {
				timestamp = time.Now()
			}
		}

		err = batch.Append(
			timestamp,
			event.Provider,
			event.ModelID,
			event.EndpointName,
			event.InputTokens,
			event.OutputTokens,
			event.TotalTokens,
			event.CachedTokens,
			event.ProcessingTimeMs,
			event.TraceID,
			event.Environment,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}

// Close closes the ClickHouse connection
func (s *ClickHouseStorage) Close() error {
	return s.conn.Close()
}
