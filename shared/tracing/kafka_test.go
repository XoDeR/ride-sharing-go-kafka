package tracing

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestKafkaHeadersCarrier(t *testing.T) {
	var headers []kafka.Header
	c := kafkaHeadersCarrier{headers: &headers}

	if got := c.Get("traceparent"); got != "" {
		t.Fatalf("expected empty value for a missing key, got %q", got)
	}

	c.Set("traceparent", "a")
	c.Set("tracestate", "b")
	c.Set("traceparent", "c") // must replace, not append

	if len(headers) != 2 {
		t.Fatalf("expected 2 headers, got %d: %+v", len(headers), headers)
	}
	if got := c.Get("traceparent"); got != "c" {
		t.Fatalf("expected traceparent=c, got %q", got)
	}
	if got := c.Get("tracestate"); got != "b" {
		t.Fatalf("expected tracestate=b, got %q", got)
	}
	if keys := c.Keys(); len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %v", keys)
	}
}
