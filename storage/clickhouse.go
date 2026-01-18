// Package storage provides ClickHouse storage functionality for token usage events.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/locobuzz-shoaib/cv-go-common-package/pkg/logger"

	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/consumer"
)

// ClickHouseStorage handles storage of token usage events in ClickHouse
type ClickHouseStorage struct {
	db     *sql.DB
	cfg    *config.Config
	logger *logger.Logger
}

// NewClickHouseStorage creates a new ClickHouse storage connection
func NewClickHouseStorage(cfg *config.Config, log *logger.Logger) (*ClickHouseStorage, error) {
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

	if log != nil {
		log.Info("Connected to ClickHouse",
			"address", cfg.ClickHouseAddr(),
			"database", cfg.ClickHouseDatabase,
		)
	}

	return &ClickHouseStorage{db: db, cfg: cfg, logger: log}, nil
}

// InsertEvent inserts a single token usage event into ClickHouse
func (s *ClickHouseStorage) InsertEvent(ctx context.Context, event *consumer.TokenUsageEvent) error {
	// Parse timestamp - fail if invalid
	timestamp, err := parseTimestamp(event.Timestamp)
	if err != nil {
		return fmt.Errorf("invalid timestamp for event trace_id=%s: %w", event.TraceID, err)
	}

	_, err = s.db.ExecContext(ctx, `
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
// Events with invalid timestamps are skipped and logged
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
	var skippedCount int

	for _, event := range events {
		timestamp, err := parseTimestamp(event.Timestamp)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("Skipping event with invalid timestamp",
					"trace_id", event.TraceID,
					"error", err,
				)
			}
			skippedCount++
			continue
		}

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

	if skippedCount > 0 && s.logger != nil {
		s.logger.Warn("Skipped events with invalid timestamps",
			"skipped_count", skippedCount,
			"total_count", len(events),
		)
	}

	// If all events were invalid, nothing to insert
	if len(values) == 0 {
		return fmt.Errorf("all %d events had invalid timestamps, nothing to insert", len(events))
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
// Returns an error if parsing fails - NO silent fallback to time.Now()
func parseTimestamp(ts string) (time.Time, error) {
	if ts == "" {
		return time.Time{}, fmt.Errorf("empty timestamp string")
	}

	// Try RFC3339 first (most common)
	if timestamp, err := time.Parse(time.RFC3339, ts); err == nil {
		return timestamp, nil
	}

	// Try alternative ISO format with milliseconds
	if timestamp, err := time.Parse("2006-01-02T15:04:05.000Z", ts); err == nil {
		return timestamp, nil
	}

	// Try format without timezone
	if timestamp, err := time.Parse("2006-01-02T15:04:05", ts); err == nil {
		return timestamp, nil
	}

	// Try space-separated format
	if timestamp, err := time.Parse("2006-01-02 15:04:05", ts); err == nil {
		return timestamp, nil
	}

	return time.Time{}, fmt.Errorf("failed to parse timestamp: %s", ts)
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
