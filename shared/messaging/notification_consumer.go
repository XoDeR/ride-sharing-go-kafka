package messaging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"os"

	"ride-sharing/shared/contracts"
)

// NotificationConsumer pushes the messages of the given topics to the
// WebSocket of the user they belong to (Message.OwnerID).
//
// It is started once per api-gateway instance, not per WebSocket connection.
// Every instance uses its own consumer group, so every instance sees every
// message and delivers it if the user is connected to this instance, other
// instances drop it. It starts at the latest offset: notifications are only
// relevant for connected users, so a restarted gateway must not replay old ones.
type NotificationConsumer struct {
	kafka   *Kafka
	connMgr *ConnectionManager
	topics  []string
}

func NewNotificationConsumer(kafka *Kafka, connMgr *ConnectionManager, topics []string) *NotificationConsumer {
	return &NotificationConsumer{
		kafka:   kafka,
		connMgr: connMgr,
		topics:  topics,
	}
}

func (nc *NotificationConsumer) Start() error {
	return nc.kafka.Consume(instanceGroupID(), nc.topics, nc.handle, StartAtLatest())
}

// instanceGroupID returns a consumer group id that is unique for this process
// (new on every restart too, so the group never has offsets to resume from).
func instanceGroupID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}

	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)

	return GroupAPIGatewayPrefix + "-" + host + "-" + hex.EncodeToString(suffix)
}

// handle never returns an error: a push that could not be delivered is not
// worth retrying and must not end up in the dead letter topic.
func (nc *NotificationConsumer) handle(_ context.Context, d Delivery) error {
	var msgBody contracts.Message
	if err := json.Unmarshal(d.Value, &msgBody); err != nil {
		log.Println("Failed to unmarshal message:", err)
		return nil
	}

	userID := msgBody.OwnerID

	var payload any
	if msgBody.Data != nil {
		if err := json.Unmarshal(msgBody.Data, &payload); err != nil {
			log.Println("Failed to unmarshal payload:", err)
			return nil
		}
	}

	clientMsg := contracts.WSMessage{
		Type: d.Topic,
		Data: payload,
	}

	if err := nc.connMgr.SendMessage(userID, clientMsg); err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			// The user is connected to another gateway instance or is gone
			return nil
		}
		log.Printf("Failed to send message to user %s: %v", userID, err)
	}

	return nil
}
