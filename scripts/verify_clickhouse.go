//go:build ignore
// +build ignore

// Verify data in ClickHouse after sending test events.
//
// Usage:
//   go run scripts/verify_clickhouse.go
//   go run scripts/verify_clickhouse.go -trace "abc123"
//   go run scripts/verify_clickhouse.go -limit 20
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/locobuzz-solutions/creatorverse-ai-token-metrics-service/config"
)

func main() {
	// Parse flags
	tracePrefix := flag.String("trace", "", "Filter by trace_id prefix")
	limit := flag.Int("limit", 10, "Number of records to show")
	configFile := flag.String("config", "appsettings.json", "Path to config file")
	flag.Parse()

	// Load config
	cfg, err := config.LoadConfigFromFile(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Connecting to ClickHouse: %s/%s", cfg.ClickHouseAddr(), cfg.ClickHouseDatabase)

	// Connect using OpenDB (HTTP protocol for port 18123)
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:     []string{cfg.ClickHouseAddr()},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		DialTimeout: 10 * time.Second,
	})
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Ping to verify connection
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	log.Println("Connected to ClickHouse!")

	// Get table count
	var totalCount int
	err = db.QueryRowContext(ctx, "SELECT count() FROM ai_token_usage").Scan(&totalCount)
	if err != nil {
		log.Printf("Warning: Could not get total count: %v", err)
	} else {
		log.Printf("Total records in ai_token_usage: %d", totalCount)
	}

	// Build query
	query := fmt.Sprintf(`
		SELECT
			timestamp,
			provider,
			model_id,
			endpoint_name,
			input_tokens,
			output_tokens,
			total_tokens,
			cached_tokens,
			processing_time_ms,
			trace_id,
			environment
		FROM ai_token_usage
		%s
		ORDER BY timestamp DESC
		LIMIT %d
	`, buildWhereClause(*tracePrefix), *limit)

	log.Printf("Query: %s", query)

	// Execute query
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Print results
	fmt.Println("\n" + "=" + repeat("=", 120))
	fmt.Printf("%-25s | %-12s | %-40s | %-15s | %6s | %6s | %6s | %s\n",
		"Timestamp", "Provider", "Model", "Endpoint", "In", "Out", "Total", "TraceID")
	fmt.Println(repeat("-", 121))

	count := 0
	for rows.Next() {
		var (
			timestamp        time.Time
			provider         string
			modelID          string
			endpointName     string
			inputTokens      int
			outputTokens     int
			totalTokens      int
			cachedTokens     int
			processingTimeMs float64
			traceID          string
			environment      string
		)

		err := rows.Scan(
			&timestamp,
			&provider,
			&modelID,
			&endpointName,
			&inputTokens,
			&outputTokens,
			&totalTokens,
			&cachedTokens,
			&processingTimeMs,
			&traceID,
			&environment,
		)
		if err != nil {
			log.Printf("Row scan error: %v", err)
			continue
		}

		// Truncate long strings for display
		if len(modelID) > 40 {
			modelID = modelID[:37] + "..."
		}
		if len(traceID) > 20 {
			traceID = traceID[:17] + "..."
		}

		fmt.Printf("%-25s | %-12s | %-40s | %-15s | %6d | %6d | %6d | %s\n",
			timestamp.Format("2006-01-02 15:04:05"),
			provider,
			modelID,
			endpointName,
			inputTokens,
			outputTokens,
			totalTokens,
			traceID,
		)
		count++
	}

	fmt.Println(repeat("=", 121))
	fmt.Printf("Showing %d records\n", count)

	if *tracePrefix != "" {
		// Show count for this trace prefix
		var traceCount int
		countQuery := fmt.Sprintf("SELECT count() FROM ai_token_usage WHERE trace_id LIKE '%s%%'", *tracePrefix)
		err = db.QueryRowContext(ctx, countQuery).Scan(&traceCount)
		if err == nil {
			fmt.Printf("Total records matching trace prefix '%s': %d\n", *tracePrefix, traceCount)
		}
	}
}

func buildWhereClause(tracePrefix string) string {
	if tracePrefix == "" {
		return ""
	}
	return fmt.Sprintf("WHERE trace_id LIKE '%s%%'", tracePrefix)
}

func repeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
