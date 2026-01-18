-- Migration: Create ai_token_usage table for token usage tracking
-- Database: creatorverse
-- Created: 2026-01-16
--
-- This table stores AI token usage events from the AI Feature Service.
-- Events are published via Kafka and consumed by the Token Metrics Service.
--
-- Schema Design:
--   - LowCardinality for low-cardinality string columns (provider, model_id, endpoint_name, environment)
--   - Partitioned by month for efficient data management and TTL
--   - Ordered by (environment, provider, model_id, endpoint_name, timestamp) for common query patterns
--   - 90-day TTL for automatic data retention management
--
-- Common Queries:
--   1. Token usage by model: GROUP BY model_id
--   2. Token usage by endpoint: GROUP BY endpoint_name
--   3. Token usage by environment: WHERE environment = 'production'
--   4. Daily aggregations: toStartOfDay(timestamp)
--
-- Usage:
--   clickhouse-client --host <host> --port <port> --user <user> --password <password> \
--     --database creatorverse < migrations/001_create_ai_token_usage.sql

-- Create the table
CREATE TABLE IF NOT EXISTS creatorverse.ai_token_usage
(
    -- Timestamp with millisecond precision
    timestamp DateTime64(3),

    -- Provider: 'aws_bedrock' or 'google_gemini'
    provider LowCardinality(String),

    -- Model identifier (e.g., 'anthropic.claude-3-sonnet-20240229-v1:0', 'gemini-2.0-flash-exp')
    model_id LowCardinality(String),

    -- API endpoint that made the request (e.g., 'caption_generate', '/v1/image/validate')
    endpoint_name LowCardinality(String),

    -- Token counts
    input_tokens UInt32,
    output_tokens UInt32,
    total_tokens UInt32,
    cached_tokens UInt32 DEFAULT 0,

    -- Processing time in milliseconds
    processing_time_ms Float32,

    -- Request trace ID for correlation (e.g., 'CVAI-abc123')
    trace_id String,

    -- Environment: 'staging', 'production', etc.
    environment LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (environment, provider, model_id, endpoint_name, timestamp)
TTL toDate(timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Optional: Create materialized view for daily aggregations
-- Uncomment if you need pre-aggregated daily stats
/*
CREATE MATERIALIZED VIEW IF NOT EXISTS creatorverse.ai_token_usage_daily_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (environment, provider, model_id, endpoint_name, day)
TTL day + INTERVAL 365 DAY
AS SELECT
    toStartOfDay(timestamp) AS day,
    environment,
    provider,
    model_id,
    endpoint_name,
    sum(input_tokens) AS total_input_tokens,
    sum(output_tokens) AS total_output_tokens,
    sum(total_tokens) AS total_tokens_sum,
    count() AS request_count,
    avg(processing_time_ms) AS avg_processing_time_ms
FROM creatorverse.ai_token_usage
GROUP BY day, environment, provider, model_id, endpoint_name;
*/
