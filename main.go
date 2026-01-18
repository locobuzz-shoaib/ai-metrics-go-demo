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
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

	// Flush batch function
	flushBatch := func() {
		batchMutex.Lock()
		if len(batch) == 0 {
			batchMutex.Unlock()
			return
		}
		toFlush := batch
		batch = make([]*consumer.TokenUsageEvent, 0, cfg.BatchSize)
		batchMutex.Unlock()

		if err := store.InsertBatch(ctx, toFlush); err != nil {
			log.Printf("Failed to insert batch (%d events): %v", len(toFlush), err)
		} else {
			log.Printf("Inserted batch of %d events", len(toFlush))
		}
	}

	// Background batch flusher
	go func() {
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

	// Main consume loop
	log.Printf("Starting to consume messages...")
	messagesProcessed := 0

	go func() {
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
				continue
			}

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
	log.Printf("Received signal %v, shutting down...", sig)
	cancel()

	// Flush remaining events
	flushBatch()

	// Shutdown health server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	healthServer.Shutdown(shutdownCtx)

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
