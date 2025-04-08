package config

import (
	"log"

	"github.com/kelseyhightower/envconfig"
)

// Config holds the application configuration
type Config struct {
	ListenAddress   string `envconfig:"LISTEN_ADDRESS" default:":8080"`
	PostgresURL     string `envconfig:"POSTGRES_URL" required:"true"`
	KafkaBrokers    string `envconfig:"KAFKA_BROKERS" required:"true"` // Comma-separated list
	KafkaTopic      string `envconfig:"KAFKA_TOPIC" required:"true"`
	APIKeyTableName string `envconfig:"API_KEY_TABLE_NAME" default:"api_keys"`
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}
	log.Printf("Configuration loaded: %+v", cfg) // Be careful logging sensitive info like URLs in production
	return &cfg, nil
}