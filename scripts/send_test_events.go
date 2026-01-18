//go:build ignore
// +build ignore

// Send test token usage events to Kafka for testing the pipeline.
//
// Usage:
//   go run scripts/send_test_events.go -count 5
//   go run scripts/send_test_events.go -count 100 -provider google_gemini
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
)

// TokenUsageEvent matches the consumer struct
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

var (
	providers = []string{"aws_bedrock", "google_gemini"}
	models    = map[string][]string{
		"aws_bedrock":   {"anthropic.claude-3-sonnet-20240229-v1:0", "anthropic.claude-3-haiku-20240307-v1:0"},
		"google_gemini": {"gemini-1.5-pro", "gemini-1.5-flash"},
	}
	endpoints = []string{"generate_content", "analyze_sentiment", "summarize", "translate"}
)

func main() {
	// Parse flags
	count := flag.Int("count", 5, "Number of events to send")
	provider := flag.String("provider", "", "Specific provider (aws_bedrock or google_gemini)")
	configFile := flag.String("config", "appsettings.json", "Path to config file")
	flag.Parse()

	// Load config
	cfg, err := config.LoadConfigFromFile(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Connecting to Kafka brokers: %v", cfg.KafkaBrokers)
	log.Printf("Topic: %s", cfg.KafkaTopic)

	// Create Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Generate and send events
	log.Printf("Sending %d test events...", *count)
	testRunID := uuid.New().String()[:8]

	for i := 0; i < *count; i++ {
		event := generateEvent(*provider, cfg.Environ, testRunID, i)

		data, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal event: %v", err)
			continue
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.TraceID),
			Value: data,
		})
		if err != nil {
			log.Printf("Failed to send event %d: %v", i, err)
		} else {
			log.Printf("Sent event %d: trace_id=%s, provider=%s, tokens=%d",
				i+1, event.TraceID, event.Provider, event.TotalTokens)
		}
	}

	log.Printf("Done! Sent %d events with test_run_id prefix: %s", *count, testRunID)
	log.Printf("Use this to query: SELECT * FROM ai_token_usage WHERE trace_id LIKE '%s%%'", testRunID)
}

func generateEvent(providerOverride, environment, testRunID string, index int) TokenUsageEvent {
	// Select provider
	selectedProvider := providerOverride
	if selectedProvider == "" {
		selectedProvider = providers[rand.Intn(len(providers))]
	}

	// Select model for provider
	providerModels := models[selectedProvider]
	selectedModel := providerModels[rand.Intn(len(providerModels))]

	// Generate random token counts
	inputTokens := rand.Intn(500) + 50
	outputTokens := rand.Intn(1000) + 100
	cachedTokens := 0
	if rand.Float32() < 0.3 { // 30% chance of cached tokens
		cachedTokens = rand.Intn(inputTokens / 2)
	}

	return TokenUsageEvent{
		EventType:        "token_usage",
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		Provider:         selectedProvider,
		ModelID:          selectedModel,
		EndpointName:     endpoints[rand.Intn(len(endpoints))],
		InputTokens:      inputTokens,
		OutputTokens:     outputTokens,
		TotalTokens:      inputTokens + outputTokens,
		CachedTokens:     cachedTokens,
		ProcessingTimeMs: float64(rand.Intn(3000)) + rand.Float64()*1000,
		TraceID:          fmt.Sprintf("%s-%d-%s", testRunID, index, uuid.New().String()[:8]),
		Environment:      environment,
	}
}
