package events

import (
	"context"
	"encoding/json"
	"log"

	"ride-sharing/services/trip-service/internal/domain"
	"ride-sharing/shared/contracts"
	"ride-sharing/shared/messaging"
)

type paymentConsumer struct {
	kafka   *messaging.Kafka
	service domain.TripService
}

func NewPaymentConsumer(kafka *messaging.Kafka, service domain.TripService) *paymentConsumer {
	return &paymentConsumer{
		kafka:   kafka,
		service: service,
	}
}

func (c *paymentConsumer) Listen() error {
	return c.kafka.Consume(messaging.GroupTripPaymentSuccess, []string{contracts.PaymentEventSuccess}, func(ctx context.Context, msg messaging.Delivery) error {
		var message contracts.Message
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return err
		}
		var payload messaging.PaymentStatusUpdateData
		if err := json.Unmarshal(message.Data, &payload); err != nil {
			log.Printf("Failed to unmarshal payload: %v", err)
			return err
		}

		log.Printf("Trip has been completed and payed.")

		return c.service.UpdateTrip(
			ctx,
			payload.TripID,
			"payed",
			nil,
		)
	})
}
