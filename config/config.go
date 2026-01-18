// Package config provides configuration management using appsettings.json.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config holds all configuration for the service
type Config struct {
	ServiceName string `json:"service_name"`
	Environ     string `json:"environ"`

	// Kafka configuration
	KafkaBrokers         []string
	KafkaTopic           string
	KafkaGroupID         string
	KafkaSecurityProtocol string

	// ClickHouse configuration
	ClickHouseHost     string
	ClickHousePort     int
	ClickHouseDatabase string
	ClickHouseUser     string
	ClickHousePassword string

	// Batch configuration
	BatchSize      int
	BatchTimeoutMs int

	// Service configuration
	LogLevel string
}

// AppSettings represents the JSON structure of appsettings.json
type AppSettings struct {
	ServiceName string `json:"service_name"`
	Environ     string `json:"environ"`
	Kafka       struct {
		BootstrapServers string `json:"bootstrap_servers"`
		Topic            string `json:"topic"`
		GroupID          string `json:"group_id"`
		SecurityProtocol string `json:"security_protocol"`
	} `json:"kafka"`
	ClickHouse struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		Database string `json:"database"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"clickhouse"`
	Batch struct {
		Size      int `json:"size"`
		TimeoutMs int `json:"timeout_ms"`
	} `json:"batch"`
	LogLevel string `json:"log_level"`
}

// LoadConfig loads configuration from appsettings.json
func LoadConfig() (*Config, error) {
	return LoadConfigFromFile("appsettings.json")
}

// LoadConfigFromFile loads configuration from a specific file path
func LoadConfigFromFile(filePath string) (*Config, error) {
	// Read the file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	// Parse JSON
	var settings AppSettings
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Build config
	cfg := &Config{
		ServiceName: settings.ServiceName,
		Environ:     settings.Environ,

		// Kafka
		KafkaBrokers:         strings.Split(settings.Kafka.BootstrapServers, ","),
		KafkaTopic:           settings.Kafka.Topic,
		KafkaGroupID:         settings.Kafka.GroupID,
		KafkaSecurityProtocol: settings.Kafka.SecurityProtocol,

		// ClickHouse
		ClickHouseHost:     settings.ClickHouse.Host,
		ClickHousePort:     settings.ClickHouse.Port,
		ClickHouseDatabase: settings.ClickHouse.Database,
		ClickHouseUser:     settings.ClickHouse.Username,
		ClickHousePassword: settings.ClickHouse.Password,

		// Batch
		BatchSize:      settings.Batch.Size,
		BatchTimeoutMs: settings.Batch.TimeoutMs,

		// Service
		LogLevel: settings.LogLevel,
	}

	// Apply defaults if not set
	if cfg.KafkaTopic == "" {
		cfg.KafkaTopic = "creatorverse.ai.token_usage"
	}
	if cfg.KafkaGroupID == "" {
		cfg.KafkaGroupID = "token-metrics-consumer"
	}
	// ClickHousePort and ClickHouseDatabase are required - no defaults
	// Validation will catch missing values
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeoutMs == 0 {
		cfg.BatchTimeoutMs = 5000
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// ValidationError represents a configuration validation error
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	var msgs []string
	for _, err := range e {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// Validate checks that all required configuration fields are set and valid
func (c *Config) Validate() error {
	var errors ValidationErrors

	// Required fields - Kafka
	if len(c.KafkaBrokers) == 0 || (len(c.KafkaBrokers) == 1 && c.KafkaBrokers[0] == "") {
		errors = append(errors, ValidationError{
			Field:   "kafka.bootstrap_servers",
			Message: "at least one Kafka broker must be specified",
		})
	}

	if c.KafkaTopic == "" {
		errors = append(errors, ValidationError{
			Field:   "kafka.topic",
			Message: "Kafka topic is required",
		})
	}

	// Validate Kafka broker format (basic check)
	for i, broker := range c.KafkaBrokers {
		if broker == "" {
			continue
		}
		if !strings.Contains(broker, ":") {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("kafka.bootstrap_servers[%d]", i),
				Message: fmt.Sprintf("broker should be in host:port format, got: %s", broker),
			})
		}
	}

	// Required fields - ClickHouse
	if c.ClickHouseHost == "" {
		errors = append(errors, ValidationError{
			Field:   "clickhouse.host",
			Message: "ClickHouse host is required",
		})
	}

	if c.ClickHousePort == 0 {
		errors = append(errors, ValidationError{
			Field:   "clickhouse.port",
			Message: "ClickHouse port is required",
		})
	} else if c.ClickHousePort < 0 || c.ClickHousePort > 65535 {
		errors = append(errors, ValidationError{
			Field:   "clickhouse.port",
			Message: fmt.Sprintf("invalid port number: %d (must be 1-65535)", c.ClickHousePort),
		})
	}

	if c.ClickHouseDatabase == "" {
		errors = append(errors, ValidationError{
			Field:   "clickhouse.database",
			Message: "ClickHouse database is required",
		})
	}

	// Validate batch configuration
	if c.BatchSize <= 0 {
		errors = append(errors, ValidationError{
			Field:   "batch.size",
			Message: fmt.Sprintf("batch size must be positive, got: %d", c.BatchSize),
		})
	}

	if c.BatchSize > 10000 {
		errors = append(errors, ValidationError{
			Field:   "batch.size",
			Message: fmt.Sprintf("batch size too large (max 10000), got: %d", c.BatchSize),
		})
	}

	if c.BatchTimeoutMs <= 0 {
		errors = append(errors, ValidationError{
			Field:   "batch.timeout_ms",
			Message: fmt.Sprintf("batch timeout must be positive, got: %d", c.BatchTimeoutMs),
		})
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// ClickHouseAddr returns the full ClickHouse address (host:port)
func (c *Config) ClickHouseAddr() string {
	return fmt.Sprintf("%s:%d", c.ClickHouseHost, c.ClickHousePort)
}
