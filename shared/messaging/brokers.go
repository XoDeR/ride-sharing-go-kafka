package messaging

import (
	"strings"

	"ride-sharing/shared/env"
)

// BrokersFromEnv returns the Kafka bootstrap servers from the comma separated KAFKA_BROKERS variable.
func BrokersFromEnv() []string {
	var brokers []string
	for _, b := range strings.Split(env.GetString("KAFKA_BROKERS", "kafka:9092"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}
