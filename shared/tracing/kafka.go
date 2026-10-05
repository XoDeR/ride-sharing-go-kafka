package tracing

import (
	"context"
	"encoding/json"

	"ride-sharing/shared/contracts"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// kafkaHeadersCarrier implements the TextMapCarrier interface for Kafka record headers
type kafkaHeadersCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeadersCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeadersCarrier) Set(key string, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}

func setOwnerAttribute(span trace.Span, value []byte) {
	// Try to extract and add message details to span
	var msgBody contracts.Message
	if err := json.Unmarshal(value, &msgBody); err == nil && msgBody.OwnerID != "" {
		span.SetAttributes(attribute.String("messaging.owner_id", msgBody.OwnerID))
	}
}

// TracedKafkaPublisher wraps the Kafka publish function with tracing
func TracedKafkaPublisher(ctx context.Context, msg kafka.Message, publish func(context.Context, kafka.Message) error) error {
	tracer := otel.GetTracerProvider().Tracer("kafka")

	ctx, span := tracer.Start(ctx, "kafka.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.String("messaging.kafka.message.key", string(msg.Key)),
		),
	)
	defer span.End()

	setOwnerAttribute(span, msg.Value)

	// Inject trace context into the record headers
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeadersCarrier{headers: &msg.Headers})

	if err := publish(ctx, msg); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

// TracedKafkaConsumer wraps the Kafka message handler with tracing.
// The parent context only provides cancellation, the trace itself continues
// the one injected by the publisher.
func TracedKafkaConsumer(ctx context.Context, msg kafka.Message, handler func(context.Context, kafka.Message) error) error {
	// Extract trace context from the record headers
	headers := msg.Headers
	ctx = otel.GetTextMapPropagator().Extract(ctx, kafkaHeadersCarrier{headers: &headers})

	tracer := otel.GetTracerProvider().Tracer("kafka")

	ctx, span := tracer.Start(ctx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.String("messaging.kafka.message.key", string(msg.Key)),
			attribute.Int("messaging.kafka.partition", msg.Partition),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		),
	)
	defer span.End()

	setOwnerAttribute(span, msg.Value)

	if err := handler(ctx, msg); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}
