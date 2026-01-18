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
//   - Health check endpoint
//
// Usage:
//   ./creatorverse-ai-token-metrics-service
//   # Or with a different config file:
//   ./creatorverse-ai-token-metrics-service -config /path/to/appsettings.json
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/backoff"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/consumer"
	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/storage"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "appsettings.json", "Path to configuration file")
	flag.Parse()

	// Load configuration from appsettings.json
	cfg, err := config.LoadConfigFromFile(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Starting %s", cfg.ServiceName)
	log.Printf("Environment: %s", cfg.Environ)
	log.Printf("Kafka brokers: %v", cfg.KafkaBrokers)
	log.Printf("Kafka topic: %s", cfg.KafkaTopic)
	log.Printf("ClickHouse: %s/%s", cfg.ClickHouseAddr(), cfg.ClickHouseDatabase)
	log.Printf("Batch size: %d, timeout: %dms", cfg.BatchSize, cfg.BatchTimeoutMs)

	// Initialize ClickHouse storage
	store, err := storage.NewClickHouseStorage(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer store.Close()

	// Initialize Kafka consumer
	kafkaConsumer := consumer.NewKafkaConsumer(cfg)
	defer kafkaConsumer.Close()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start health check server
	healthServer := startHealthServer(cfg.HealthCheckPort)

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

		// Retry with exponential backoff
		retryBackoff := backoff.New(backoff.RetryConfig())
		var lastErr error

		for attempt := 1; attempt <= 4; attempt++ { // 1 initial + 3 retries
			if err := store.InsertBatch(flushCtx, toFlush); err != nil {
				lastErr = err
				log.Printf("Batch insert failed (attempt %d/4, %d events): %v", attempt, len(toFlush), err)

				if attempt < 4 {
					delay := retryBackoff.Duration()
					log.Printf("Retrying in %v...", delay)
					if !retryBackoff.Wait(flushCtx) {
						log.Printf("Retry cancelled or timed out")
						break
					}
				}
				continue
			}

			// Success
			log.Printf("Inserted batch of %d events", len(toFlush))
			return
		}

		// All retries failed - log critical error
		log.Printf("CRITICAL: Failed to insert batch (%d events) after all retries: %v", len(toFlush), lastErr)
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
	log.Printf("Starting to consume messages...")
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
					log.Printf("Kafka read error (attempt %d): %v", kafkaBackoff.Attempts()+1, err)

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
				log.Printf("Processed %d messages", messagesProcessed)
			}
		}
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

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
		log.Printf("All goroutines stopped")
	case <-time.After(10 * time.Second):
		log.Printf("WARNING: Timeout waiting for goroutines to stop")
	}

	// Stop the batch timer
	batchTimer.Stop()

	// Flush remaining events (uses fresh context internally)
	log.Printf("Flushing remaining events...")
	flushBatch()

	// Shutdown health server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Health server shutdown error: %v", err)
	}

	log.Printf("%s stopped. Total messages processed: %d", cfg.ServiceName, messagesProcessed)
}

// startHealthServer starts an HTTP server for health checks
func startHealthServer(port int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready"))
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		log.Printf("Health check server listening on port %d", port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()

	return server
}
