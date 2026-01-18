// Package storage provides ClickHouse storage functionality for token usage events.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/consumer"
)

// ClickHouseStorage handles storage of token usage events in ClickHouse
type ClickHouseStorage struct {
	db  *sql.DB
	cfg *config.Config
}

// NewClickHouseStorage creates a new ClickHouse storage connection
func NewClickHouseStorage(cfg *config.Config) (*ClickHouseStorage, error) {
	// Use OpenDB with HTTP protocol (works with port 18123)
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:     []string{cfg.ClickHouseAddr()},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 10 * time.Second,
	})

	// Configure connection pool via sql.DB methods
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	log.Printf("Connected to ClickHouse at %s", cfg.ClickHouseAddr())
	return &ClickHouseStorage{db: db, cfg: cfg}, nil
}

// InsertEvent inserts a single token usage event into ClickHouse
func (s *ClickHouseStorage) InsertEvent(ctx context.Context, event *consumer.TokenUsageEvent) error {
	// Parse timestamp
	timestamp := parseTimestamp(event.Timestamp)

	_, err := s.db.ExecContext(ctx, `
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
	return err
}

// InsertBatch inserts a batch of events into ClickHouse
func (s *ClickHouseStorage) InsertBatch(ctx context.Context, events []*consumer.TokenUsageEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Build batch INSERT statement
	query := `INSERT INTO ai_token_usage
		(timestamp, provider, model_id, endpoint_name, input_tokens, output_tokens,
		 total_tokens, cached_tokens, processing_time_ms, trace_id, environment)
		VALUES `

	var values []string
	var args []interface{}

	for _, event := range events {
		timestamp := parseTimestamp(event.Timestamp)
		values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args,
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

	query += strings.Join(values, ", ")

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// Close closes the ClickHouse connection
func (s *ClickHouseStorage) Close() error {
	return s.db.Close()
}

// parseTimestamp parses a timestamp string into time.Time
func parseTimestamp(ts string) time.Time {
	timestamp, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		// Try alternative ISO format
		timestamp, err = time.Parse("2006-01-02T15:04:05.000Z", ts)
		if err != nil {
			// Try format without timezone
			timestamp, err = time.Parse("2006-01-02T15:04:05", ts)
			if err != nil {
				timestamp = time.Now()
			}
		}
	}
	return timestamp
}

// GetDB returns the underlying database connection (for testing)
func (s *ClickHouseStorage) GetDB() *sql.DB {
	return s.db
}

// HealthCheck verifies the database connection is alive
func (s *ClickHouseStorage) HealthCheck(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// GetVersion returns the ClickHouse server version
func (s *ClickHouseStorage) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := s.db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get version: %w", err)
	}
	return version, nil
}
