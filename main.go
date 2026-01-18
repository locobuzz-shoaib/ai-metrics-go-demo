// CreatorVerse AI Token Metrics Service
//
// A Go service that consumes AI token usage events from Kafka
// and writes them to ClickHouse for analytics and cost tracking.
//
// Configuration is loaded from appsettings.json (similar to Python services).
//
// Features:
//   - Kafka consumer with consumer group support
//   - Batch inserts to ClickHouse for efficiency
//   - Graceful shutdown handling
//   - Structured logging with trace IDs
//
// Usage:
//
//	./creatorverse-ai-token-metrics-service
//	# Or with a different config file:
//	./creatorverse-ai-token-metrics-service -config /path/to/appsettings.json
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/locobuzz-shoaib/cv-go-common-package/pkg/logger"

	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/backoff"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/consumer"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/storage"
)

var log *logger.Logger

func main() {
	// Parse command line flags
	configFile := flag.String("config", "appsettings.json", "Path to configuration file")
	flag.Parse()

	// Load configuration from appsettings.json
	cfg, err := config.LoadConfigFromFile(*configFile)
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize structured logger from common package
	log = logger.New(logger.Config{
		ServiceName: cfg.ServiceName,
		Environment: cfg.Environ,
		Level:       logger.ParseLevel(cfg.LogLevel),
		LogType:     logger.LogTypeJSON,
	})
	logger.SetDefault(log)

	// Create context with trace ID
	ctx := context.Background()
	ctx, traceID := logger.EnsureTraceID(ctx, "aitm")

	log.Info("Starting service",
		"trace_id", traceID,
		"environment", cfg.Environ,
		"kafka_brokers", cfg.KafkaBrokers,
		"kafka_topic", cfg.KafkaTopic,
		"clickhouse_addr", cfg.ClickHouseAddr(),
		"clickhouse_database", cfg.ClickHouseDatabase,
		"batch_size", cfg.BatchSize,
		"batch_timeout_ms", cfg.BatchTimeoutMs,
	)

	// Initialize ClickHouse storage
	store, err := storage.NewClickHouseStorage(cfg, log)
	if err != nil {
		log.Error("Failed to connect to ClickHouse", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// Initialize Kafka consumer
	kafkaConsumer := consumer.NewKafkaConsumer(cfg, log)
	defer kafkaConsumer.Close()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Event batch for bulk inserts
	var (
		batch      []*consumer.TokenUsageEvent
		batchMutex sync.Mutex
		batchTimer = time.NewTimer(time.Duration(cfg.BatchTimeoutMs) * time.Millisecond)
	)

	// Flush batch function with retry logic
	flushBatch := func() {
		batchMutex.Lock()
		if len(batch) == 0 {
			batchMutex.Unlock()
			return
		}
		toFlush := batch
		batch = make([]*consumer.TokenUsageEvent, 0, cfg.BatchSize)
		batchMutex.Unlock()

		// Use a fresh context for flush (not the cancelled main context)
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer flushCancel()

		// Add trace ID to flush context
		flushCtx, flushTraceID := logger.EnsureTraceID(flushCtx, "flush")

		// Retry with exponential backoff
		retryBackoff := backoff.New(backoff.RetryConfig())
		var lastErr error

		for attempt := 1; attempt <= 4; attempt++ { // 1 initial + 3 retries
			start := time.Now()
			if err := store.InsertBatch(flushCtx, toFlush); err != nil {
				lastErr = err
				log.Warn("Batch insert failed",
					"trace_id", flushTraceID,
					"attempt", attempt,
					"max_attempts", 4,
					"event_count", len(toFlush),
					"error", err,
				)

				if attempt < 4 {
					delay := retryBackoff.Duration()
					log.Debug("Retrying batch insert",
						"trace_id", flushTraceID,
						"delay", delay,
					)
					if !retryBackoff.Wait(flushCtx) {
						log.Warn("Retry cancelled or timed out", "trace_id", flushTraceID)
						break
					}
				}
				continue
			}

			// Success - log with performance metrics
			log.LogPerformance(flushCtx, "batch_insert", time.Since(start),
				"event_count", len(toFlush),
			)
			return
		}

		// All retries failed - log critical error
		log.Error("CRITICAL: Failed to insert batch after all retries",
			"trace_id", flushTraceID,
			"event_count", len(toFlush),
			"error", lastErr,
		)
		// Events are lost at this point - in production, consider writing to dead-letter file
	}

	// WaitGroup for graceful shutdown
	var wg sync.WaitGroup

	// Background batch flusher
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-batchTimer.C:
				flushBatch()
				batchTimer.Reset(time.Duration(cfg.BatchTimeoutMs) * time.Millisecond)
			}
		}
	}()

	// Main consume loop with proper error handling and backoff
	log.Info("Starting message consumption")
	messagesProcessed := 0
	kafkaBackoff := backoff.New(backoff.DefaultConfig())

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Read with timeout
			readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
			event, err := kafkaConsumer.ReadEvent(readCtx)
			readCancel()

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				// Log error with context
				if errors.Is(err, context.DeadlineExceeded) {
					// Timeout is normal when no messages - reset backoff but don't log every time
					kafkaBackoff.Reset()
				} else {
					// Real error - log and apply backoff
					log.Warn("Kafka read error",
						"attempt", kafkaBackoff.Attempts()+1,
						"error", err,
					)

					if !kafkaBackoff.Wait(ctx) {
						// Context cancelled during backoff
						return
					}
				}
				continue
			}

			// Success - reset backoff
			kafkaBackoff.Reset()

			// Add to batch
			batchMutex.Lock()
			batch = append(batch, event)
			shouldFlush := len(batch) >= cfg.BatchSize
			batchMutex.Unlock()

			if shouldFlush {
				flushBatch()
				batchTimer.Reset(time.Duration(cfg.BatchTimeoutMs) * time.Millisecond)
			}

			messagesProcessed++
			if messagesProcessed%1000 == 0 {
				log.Info("Processing progress",
					"messages_processed", messagesProcessed,
				)
			}
		}
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	log.Info("Received shutdown signal, initiating graceful shutdown",
		"signal", sig.String(),
	)

	// Cancel context to signal goroutines to stop
	cancel()

	// Wait for consumer goroutine to exit with timeout
	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		log.Info("All goroutines stopped")
	case <-time.After(10 * time.Second):
		log.Warn("Timeout waiting for goroutines to stop")
	}

	// Stop the batch timer
	batchTimer.Stop()

	// Flush remaining events (uses fresh context internally)
	log.Info("Flushing remaining events")
	flushBatch()

	log.Info("Service stopped",
		"total_messages_processed", messagesProcessed,
	)
}
