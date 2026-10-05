package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"ride-sharing/shared/contracts"
	"ride-sharing/shared/env"
	"ride-sharing/shared/retry"
	"ride-sharing/shared/tracing"

	"github.com/segmentio/kafka-go"
)

// Delivery is a message received from Kafka. It keeps the broker library out
// of the services.
type Delivery struct {
	// Topic doubles as the message type (it used to be the routing key).
	Topic     string
	Key       string
	Partition int
	Offset    int64
	// Value is the JSON encoded contracts.Message
	Value   []byte
	Headers map[string]string
}

type Handler func(ctx context.Context, d Delivery) error

type Kafka struct {
	brokers []string
	writer  *kafka.Writer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewKafka connects to the brokers (waiting for them to come up) and makes sure all topics exist.
func NewKafka(brokers []string) (*Kafka, error) {
	if len(brokers) == 0 {
		return nil, errors.New("no kafka brokers configured")
	}

	cfg := retry.Config{MaxRetries: 10, InitialWait: 2 * time.Second, MaxWait: 10 * time.Second}
	k := &Kafka{brokers: brokers}
	k.ctx, k.cancel = context.WithCancel(context.Background())

	err := retry.WithBackoff(k.ctx, cfg, func() error {
		return k.ensureTopics(k.ctx, Topics)
	})
	if err != nil {
		k.cancel()
		return nil, fmt.Errorf("failed to connect to Kafka: %v", err)
	}

	k.writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.Hash{}, // same key (owner) -> same partition -> ordered
		// Wait for the broker to persist the record before reporting success
		RequiredAcks: kafka.RequireAll,
		// Writes are synchronous, do not wait to fill a batch
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
	}

	return k, nil
}

// ensureTopics creates the missing topics. Several services run it at the same
// time on startup, so a topic created in the meantime is not an error.
func (k *Kafka) ensureTopics(ctx context.Context, topics []string) error {
	dialer := &kafka.Dialer{Timeout: 10 * time.Second}

	conn, err := dialer.DialContext(ctx, "tcp", k.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return err
	}

	existing := make(map[string]bool)
	for _, p := range partitions {
		existing[p.Topic] = true
	}

	// Topics have to be created on the controller
	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	ctrlConn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()

	numPartitions := env.GetInt("KAFKA_PARTITIONS", 3)
	replicationFactor := env.GetInt("KAFKA_REPLICATION_FACTOR", 1)

	for _, topic := range topics {
		if existing[topic] {
			continue
		}

		// One topic per call: kafka-go only reports the first failed topic of a batch
		err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		})
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			return fmt.Errorf("failed to create topic %s: %v", topic, err)
		}
	}

	return nil
}

func (k *Kafka) PublishMessage(ctx context.Context, topic string, message contracts.Message) error {
	log.Printf("Publishing message to topic: %s", topic)

	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	msg := kafka.Message{
		Topic:   topic,
		Key:     []byte(message.OwnerID),
		Value:   jsonMsg,
		Headers: []kafka.Header{{Key: "content-type", Value: []byte("application/json")}},
	}

	return tracing.TracedKafkaPublisher(ctx, msg, k.publish)
}

func (k *Kafka) publish(ctx context.Context, msg kafka.Message) error {
	return k.writer.WriteMessages(ctx, msg)
}

type consumeConfig struct {
	startOffset int64
}

type ConsumeOption func(*consumeConfig)

// StartAtLatest makes a consumer group without committed offsets skip the
// existing messages instead of reading the topics from the beginning.
func StartAtLatest() ConsumeOption {
	return func(c *consumeConfig) { c.startOffset = kafka.LastOffset }
}

// Consume reads the topics as a member of the consumer group in the background
// until Close is called. Messages are handled one at a time (per consumer) and
// their offset is committed once the handler succeeded (at-least-once), so
// handlers have to be idempotent. A message that keeps failing is moved to
// the dead letter topic, so it never blocks the partition.
func (k *Kafka) Consume(groupID string, topics []string, handler Handler, opts ...ConsumeOption) error {
	cfg := consumeConfig{startOffset: kafka.FirstOffset}
	for _, opt := range opts {
		opt(&cfg)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     k.brokers,
		GroupID:     groupID,
		GroupTopics: topics,
		StartOffset: cfg.startOffset,
		MaxWait:     500 * time.Millisecond,
	})

	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		defer reader.Close()

		for {
			msg, err := reader.FetchMessage(k.ctx)
			if err != nil {
				if k.ctx.Err() != nil || errors.Is(err, io.EOF) {
					return
				}
				log.Printf("Failed to fetch message for group %s: %v", groupID, err)
				select {
				case <-k.ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}

			if !k.process(msg, handler) {
				return // shutting down, leave the message uncommitted
			}

			if err := reader.CommitMessages(k.ctx, msg); err != nil && k.ctx.Err() == nil {
				log.Printf("ERROR: Failed to commit offset %d of %s[%d]: %v", msg.Offset, msg.Topic, msg.Partition, err)
			}
		}
	}()

	return nil
}

// process handles the message and reports whether its offset should be committed.
func (k *Kafka) process(msg kafka.Message, handler Handler) bool {
	d := toDelivery(msg)
	committable := true

	_ = tracing.TracedKafkaConsumer(k.ctx, msg, func(ctx context.Context, _ kafka.Message) error {
		log.Printf("Received a message from %s: %s", d.Topic, d.Value)

		cfg := retry.DefaultConfig()
		committable = processWithRetry(ctx, cfg, handler, d, func(ctx context.Context, d Delivery, cause error) error {
			return k.publishDeadLetter(ctx, d, cause, cfg.MaxRetries)
		})
		return nil
	})

	return committable && k.ctx.Err() == nil
}

// processWithRetry runs the handler with retries. If it keeps failing, the
// message is handed to deadLetter. It returns false if the message must stay
// uncommitted because the context was cancelled.
func processWithRetry(ctx context.Context, cfg retry.Config, handler Handler, d Delivery, deadLetter func(context.Context, Delivery, error) error) bool {
	err := retry.WithBackoff(ctx, cfg, func() error {
		return handler(ctx, d)
	})
	if err == nil {
		return true
	}
	if ctx.Err() != nil {
		return false
	}

	log.Printf("Message processing failed after %d retries for %s[%d]@%d, err: %v", cfg.MaxRetries, d.Topic, d.Partition, d.Offset, err)

	if dlqErr := deadLetter(ctx, d, err); dlqErr != nil {
		if ctx.Err() != nil {
			return false
		}
		// Committing anyway keeps the partition moving, the content is logged so it is not lost silently
		log.Printf("ERROR: Failed to publish message to the dead letter topic: %v. Dropping message: %s", dlqErr, d.Value)
	}

	return true
}

func (k *Kafka) publishDeadLetter(ctx context.Context, d Delivery, cause error, retries int) error {
	headers := make([]kafka.Header, 0, len(d.Headers)+5)
	for key, value := range d.Headers {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	headers = append(headers,
		kafka.Header{Key: "x-death-reason", Value: []byte(cause.Error())},
		kafka.Header{Key: "x-original-topic", Value: []byte(d.Topic)},
		kafka.Header{Key: "x-original-partition", Value: []byte(strconv.Itoa(d.Partition))},
		kafka.Header{Key: "x-original-offset", Value: []byte(strconv.FormatInt(d.Offset, 10))},
		kafka.Header{Key: "x-retry-count", Value: []byte(strconv.Itoa(retries))},
	)

	return k.writer.WriteMessages(ctx, kafka.Message{
		Topic:   DeadLetterTopic,
		Key:     []byte(d.Key),
		Value:   d.Value,
		Headers: headers,
	})
}

func toDelivery(msg kafka.Message) Delivery {
	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	return Delivery{
		Topic:     msg.Topic,
		Key:       string(msg.Key),
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Value:     msg.Value,
		Headers:   headers,
	}
}

// Close stops all consumers and closes the connections.
func (k *Kafka) Close() {
	k.cancel()
	k.wg.Wait()
	if k.writer != nil {
		k.writer.Close()
	}
}
