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
	HealthCheckPort int
	LogLevel        string
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
	HealthCheckPort int    `json:"health_check_port"`
	LogLevel        string `json:"log_level"`
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
		HealthCheckPort: settings.HealthCheckPort,
		LogLevel:        settings.LogLevel,
	}

	// Apply defaults if not set
	if cfg.KafkaTopic == "" {
		cfg.KafkaTopic = "creatorverse.ai.token_usage"
	}
	if cfg.KafkaGroupID == "" {
		cfg.KafkaGroupID = "token-metrics-consumer"
	}
	if cfg.ClickHousePort == 0 {
		cfg.ClickHousePort = 9000
	}
	if cfg.ClickHouseDatabase == "" {
		cfg.ClickHouseDatabase = "creatorverse"
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeoutMs == 0 {
		cfg.BatchTimeoutMs = 5000
	}
	if cfg.HealthCheckPort == 0 {
		cfg.HealthCheckPort = 8080
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}

	return cfg, nil
}

// ClickHouseAddr returns the full ClickHouse address (host:port)
func (c *Config) ClickHouseAddr() string {
	return fmt.Sprintf("%s:%d", c.ClickHouseHost, c.ClickHousePort)
}
