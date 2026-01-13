package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSpec(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	spec()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	var msg AirbyteMessage
	if err := json.Unmarshal([]byte(output), &msg); err != nil {
		t.Fatalf("failed to parse spec output: %v", err)
	}

	if msg.Type != "SPEC" {
		t.Errorf("expected type SPEC, got %s", msg.Type)
	}

	if msg.Spec == nil {
		t.Fatal("spec is nil")
	}

	if msg.Spec.DocumentationURL == "" {
		t.Error("documentation URL is empty")
	}

	connSpec := msg.Spec.ConnectionSpecification
	if connSpec == nil {
		t.Fatal("connection specification is nil")
	}

	required, ok := connSpec["required"].([]interface{})
	if !ok {
		t.Fatal("required field is not an array")
	}

	expectedRequired := []string{"amqp_url", "exchange", "queue_name", "stream_name"}
	for _, exp := range expectedRequired {
		found := false
		for _, req := range required {
			if req.(string) == exp {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing required field: %s", exp)
		}
	}

	props, ok := connSpec["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("properties field is not a map")
	}

	expectedProps := []string{"amqp_url", "exchange", "queue_name", "binding_key", "stream_name"}
	for _, prop := range expectedProps {
		if _, exists := props[prop]; !exists {
			t.Errorf("missing property: %s", prop)
		}
	}

	amqpURLProp := props["amqp_url"].(map[string]interface{})
	if amqpURLProp["airbyte_secret"] != true {
		t.Error("amqp_url should be marked as airbyte_secret")
	}
}

func TestDiscover(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	config := Config{
		AMQPUrl:    "amqp://test:test@localhost:5672/",
		Exchange:   "test.exchange",
		QueueName:  "test.queue",
		StreamName: "test_stream",
	}

	discover(config)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	var msg AirbyteMessage
	if err := json.Unmarshal([]byte(output), &msg); err != nil {
		t.Fatalf("failed to parse discover output: %v", err)
	}

	if msg.Type != TypeCatalog {
		t.Errorf("expected type %s, got %s", TypeCatalog, msg.Type)
	}

	if msg.Catalog == nil {
		t.Fatal("catalog is nil")
	}

	if len(msg.Catalog.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(msg.Catalog.Streams))
	}

	stream := msg.Catalog.Streams[0]
	if stream.Name != "test_stream" {
		t.Errorf("expected stream name test_stream, got %s", stream.Name)
	}

	if len(stream.SupportedSyncModes) == 0 {
		t.Error("no supported sync modes")
	}

	if stream.SupportedSyncModes[0] != "full_refresh" {
		t.Errorf("expected full_refresh sync mode, got %s", stream.SupportedSyncModes[0])
	}
}

func TestEmitMessage(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	emitLog("INFO", "test message")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	var msg AirbyteMessage
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &msg); err != nil {
		t.Fatalf("failed to parse emitted message: %v", err)
	}

	if msg.Type != TypeLog {
		t.Errorf("expected type %s, got %s", TypeLog, msg.Type)
	}

	if msg.Log == nil {
		t.Fatal("log is nil")
	}

	if msg.Log.Level != "INFO" {
		t.Errorf("expected level INFO, got %s", msg.Log.Level)
	}

	if msg.Log.Message != "test message" {
		t.Errorf("expected message 'test message', got '%s'", msg.Log.Message)
	}
}

func TestEmitMessageReturnsError(t *testing.T) {
	msg := AirbyteMessage{
		Type: TypeLog,
		Log: &LogMessage{
			Level:   "INFO",
			Message: "test",
		},
	}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := emitMessage(msg)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestConfigParsing(t *testing.T) {
	configJSON := `{
		"amqp_url": "amqps://user:pass@host:5671/vhost",
		"exchange": "events",
		"queue_name": "airbyte.queue",
		"binding_key": "routing.key",
		"stream_name": "events_stream"
	}`

	var config Config
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	if config.AMQPUrl != "amqps://user:pass@host:5671/vhost" {
		t.Errorf("unexpected amqp_url: %s", config.AMQPUrl)
	}

	if config.Exchange != "events" {
		t.Errorf("unexpected exchange: %s", config.Exchange)
	}

	if config.QueueName != "airbyte.queue" {
		t.Errorf("unexpected queue_name: %s", config.QueueName)
	}

	if config.BindingKey != "routing.key" {
		t.Errorf("unexpected binding_key: %s", config.BindingKey)
	}

	if config.StreamName != "events_stream" {
		t.Errorf("unexpected stream_name: %s", config.StreamName)
	}
}

func TestConfigParsingWithEmptyBindingKey(t *testing.T) {
	configJSON := `{
		"amqp_url": "amqp://localhost",
		"exchange": "test",
		"queue_name": "queue",
		"binding_key": "",
		"stream_name": "stream"
	}`

	var config Config
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	if config.BindingKey != "" {
		t.Errorf("expected empty binding_key, got: %s", config.BindingKey)
	}
}

func TestRecordStructure(t *testing.T) {
	record := Record{
		Stream: "test_stream",
		Data: map[string]interface{}{
			"message":   map[string]interface{}{"key": "value"},
			"timestamp": "2025-01-15T10:00:00Z",
		},
		EmittedAt: 1705312800000,
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("failed to marshal record: %v", err)
	}

	var parsed Record
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal record: %v", err)
	}

	if parsed.Stream != "test_stream" {
		t.Errorf("unexpected stream: %s", parsed.Stream)
	}

	if parsed.EmittedAt != 1705312800000 {
		t.Errorf("unexpected emitted_at: %d", parsed.EmittedAt)
	}
}

func TestConnectionStatusStructure(t *testing.T) {
	tests := []struct {
		name    string
		status  ConnectionStatus
		wantMsg string
	}{
		{
			name:    "success",
			status:  ConnectionStatus{Status: "SUCCEEDED", Message: "Connected"},
			wantMsg: "Connected",
		},
		{
			name:    "failure",
			status:  ConnectionStatus{Status: "FAILED", Message: "Connection refused"},
			wantMsg: "Connection refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.status)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}

			var parsed ConnectionStatus
			if err := json.Unmarshal(data, &parsed); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}

			if parsed.Message != tt.wantMsg {
				t.Errorf("expected message %s, got %s", tt.wantMsg, parsed.Message)
			}
		})
	}
}

func TestAirbyteMessageTypes(t *testing.T) {
	tests := []struct {
		constant string
		expected string
	}{
		{TypeLog, "LOG"},
		{TypeConnectionStatus, "CONNECTION_STATUS"},
		{TypeCatalog, "CATALOG"},
		{TypeRecord, "RECORD"},
		{TypeState, "STATE"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.constant)
			}
		})
	}
}

func TestCatalogStreamConfiguration(t *testing.T) {
	catalog := Catalog{
		Streams: []Stream{
			{
				Name: "test_stream",
				JSONSchema: map[string]interface{}{
					"type": "object",
				},
				SupportedSyncModes:  []string{"full_refresh"},
				SourceDefinedCursor: false,
			},
		},
	}

	data, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("failed to marshal catalog: %v", err)
	}

	var parsed Catalog
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal catalog: %v", err)
	}

	if len(parsed.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(parsed.Streams))
	}

	stream := parsed.Streams[0]
	if stream.SourceDefinedCursor != false {
		t.Error("expected SourceDefinedCursor to be false")
	}
}

func TestStateMessage(t *testing.T) {
	state := State{
		Data: map[string]interface{}{
			"cursor": "2025-01-15T10:00:00Z",
			"offset": 100,
		},
	}

	msg := AirbyteMessage{
		Type:  TypeState,
		State: &state,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal state message: %v", err)
	}

	var parsed AirbyteMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal state message: %v", err)
	}

	if parsed.Type != TypeState {
		t.Errorf("expected type %s, got %s", TypeState, parsed.Type)
	}

	if parsed.State == nil {
		t.Fatal("state is nil")
	}

	if parsed.State.Data["cursor"] != "2025-01-15T10:00:00Z" {
		t.Errorf("unexpected cursor value")
	}
}

func TestEmitMessageValidJSON(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	msg := AirbyteMessage{
		Type: TypeRecord,
		Record: &Record{
			Stream: "test",
			Data: map[string]interface{}{
				"key": "value",
			},
			EmittedAt: 1705312800000,
		},
	}
	_ = emitMessage(msg)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := strings.TrimSpace(buf.String())

	if !json.Valid([]byte(output)) {
		t.Errorf("emitted message is not valid JSON: %s", output)
	}
}

func TestSpecContainsAMQPSExample(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	spec()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "amqps://") {
		t.Error("spec should contain amqps:// example for TLS connections")
	}
}

func TestConstants(t *testing.T) {
	if defaultPrefetchCount != 100 {
		t.Errorf("expected defaultPrefetchCount to be 100, got %d", defaultPrefetchCount)
	}

	if maxReconnectAttempts != 5 {
		t.Errorf("expected maxReconnectAttempts to be 5, got %d", maxReconnectAttempts)
	}

	if initialReconnectWait != 1*time.Second {
		t.Errorf("expected initialReconnectWait to be 1s, got %v", initialReconnectWait)
	}

	if maxReconnectWait != 30*time.Second {
		t.Errorf("expected maxReconnectWait to be 30s, got %v", maxReconnectWait)
	}

	if connectionTimeout != 30*time.Second {
		t.Errorf("expected connectionTimeout to be 30s, got %v", connectionTimeout)
	}
}

func TestNewConsumer(t *testing.T) {
	config := Config{
		AMQPUrl:    "amqp://localhost",
		Exchange:   "test",
		QueueName:  "queue",
		StreamName: "stream",
	}

	c := newConsumer(config)

	if c.config.AMQPUrl != config.AMQPUrl {
		t.Errorf("expected AMQPUrl %s, got %s", config.AMQPUrl, c.config.AMQPUrl)
	}

	if c.done == nil {
		t.Error("done channel should not be nil")
	}

	if c.messageCount != 0 {
		t.Errorf("expected messageCount 0, got %d", c.messageCount)
	}
}

func TestConsumerClose(t *testing.T) {
	config := Config{
		AMQPUrl:    "amqp://localhost",
		Exchange:   "test",
		QueueName:  "queue",
		StreamName: "stream",
	}

	c := newConsumer(config)
	c.close()
}

func TestConsumerConnectFailsWithInvalidURL(t *testing.T) {
	config := Config{
		AMQPUrl:    "amqp://invalid:invalid@localhost:99999/",
		Exchange:   "test",
		QueueName:  "queue",
		StreamName: "stream",
	}

	c := newConsumer(config)
	err := c.connect()

	if err == nil {
		t.Error("expected error for invalid connection")
		c.close()
	}
}

func TestConsumerReconnectRespectsContext(t *testing.T) {
	config := Config{
		AMQPUrl:    "amqp://invalid:invalid@localhost:99999/",
		Exchange:   "test",
		QueueName:  "queue",
		StreamName: "stream",
	}

	c := newConsumer(config)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.reconnect(ctx)

	if err == nil {
		t.Error("expected error when context is cancelled")
	}
}

func TestDialWithTimeout(t *testing.T) {
	_, err := dialWithTimeout("amqp://invalid:invalid@localhost:99999/", 100*time.Millisecond)

	if err == nil {
		t.Error("expected error for invalid connection")
	}
}
