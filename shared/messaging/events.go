package messaging

import (
	"ride-sharing/shared/contracts"
	pbd "ride-sharing/shared/proto/driver"
	pb "ride-sharing/shared/proto/trip"
)

// Kafka consumer groups. Every group reads every message of its topics once,
// replicas of the same service share a group and split the partitions.
const (
	GroupDriverFindDrivers    = "driver-service.find-drivers"
	GroupTripDriverResponse   = "trip-service.driver-response"
	GroupTripPaymentSuccess   = "trip-service.payment-success"
	GroupPaymentCreateSession = "payment-service.create-session"
	// GroupAPIGatewayPrefix is completed with a unique instance id: every
	// api-gateway instance has to see every notification.
	GroupAPIGatewayPrefix = "api-gateway"
)

// DeadLetterTopic receives messages whose processing failed after all retries.
const DeadLetterTopic = "dead_letter"

// Topics lists every topic of the system, they are created on startup.
var Topics = []string{
	contracts.TripEventCreated,
	contracts.TripEventDriverNotInterested,
	contracts.TripEventNoDriversFound,
	contracts.TripEventDriverAssigned,
	contracts.DriverCmdTripRequest,
	contracts.DriverCmdTripAccept,
	contracts.DriverCmdTripDecline,
	contracts.PaymentCmdCreateSession,
	contracts.PaymentEventSessionCreated,
	contracts.PaymentEventSuccess,
	DeadLetterTopic,
}

type TripEventData struct {
	Trip *pb.Trip `json:"trip"`
}

type DriverTripResponseData struct {
	Driver  *pbd.Driver `json:"driver"`
	TripID  string      `json:"tripID"`
	RiderID string      `json:"riderID"`
}

type PaymentEventSessionCreatedData struct {
	TripID    string  `json:"tripID"`
	SessionID string  `json:"sessionID"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

type PaymentTripResponseData struct {
	TripID   string  `json:"tripID"`
	UserID   string  `json:"userID"`
	DriverID string  `json:"driverID"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type PaymentStatusUpdateData struct {
	TripID   string `json:"tripID"`
	UserID   string `json:"userID"`
	DriverID string `json:"driverID"`
}
