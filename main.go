package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TypeLog              = "LOG"
	TypeConnectionStatus = "CONNECTION_STATUS"
	TypeCatalog          = "CATALOG"
	TypeRecord           = "RECORD"
	TypeState            = "STATE"

	defaultPrefetchCount = 100
	maxReconnectAttempts = 5
	initialReconnectWait = 1 * time.Second
	maxReconnectWait     = 30 * time.Second
	connectionTimeout    = 30 * time.Second
)

type Config struct {
	AMQPUrl    string `json:"amqp_url"`
	Exchange   string `json:"exchange"`
	QueueName  string `json:"queue_name"`
	BindingKey string `json:"binding_key"`
	StreamName string `json:"stream_name"`
}

type Spec struct {
	DocumentationURL        string                 `json:"documentationUrl"`
	ConnectionSpecification map[string]interface{} `json:"connectionSpecification"`
}

type ConnectionStatus struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type Catalog struct {
	Streams []Stream `json:"streams"`
}

type Stream struct {
	Name                    string                 `json:"name"`
	JSONSchema              map[string]interface{} `json:"json_schema"`
	SupportedSyncModes      []string               `json:"supported_sync_modes"`
	SourceDefinedCursor     bool                   `json:"source_defined_cursor"`
	DefaultCursorField      []string               `json:"default_cursor_field,omitempty"`
	SourceDefinedPrimaryKey [][]string             `json:"source_defined_primary_key,omitempty"`
}

type Record struct {
	Stream    string                 `json:"stream"`
	Data      map[string]interface{} `json:"data"`
	EmittedAt int64                  `json:"emitted_at"`
}

type AirbyteMessage struct {
	Type             string            `json:"type"`
	Log              *LogMessage       `json:"log,omitempty"`
	ConnectionStatus *ConnectionStatus `json:"connectionStatus,omitempty"`
	Spec             *Spec             `json:"spec,omitempty"`
	Catalog          *Catalog          `json:"catalog,omitempty"`
	Record           *Record           `json:"record,omitempty"`
	State            *State            `json:"state,omitempty"`
}

type LogMessage struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

type State struct {
	Data map[string]interface{} `json:"data"`
}

func emitMessage(msg AirbyteMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	fmt.Println(string(data))
	return nil
}

func emitLog(level, message string) {
	if err := emitMessage(AirbyteMessage{
		Type: TypeLog,
		Log: &LogMessage{
			Level:   level,
			Message: message,
		},
	}); err != nil {
		log.Printf("failed to emit log: %v", err)
	}
}

func spec() {
	s := Spec{
		DocumentationURL: "https://github.com/cloudamqp/airbyte-connector",
		ConnectionSpecification: map[string]interface{}{
			"$schema":  "http://json-schema.org/draft-07/schema#",
			"title":    "AMQP Source Spec",
			"type":     "object",
			"required": []string{"amqp_url", "exchange", "queue_name", "stream_name"},
			"properties": map[string]interface{}{
				"amqp_url": map[string]interface{}{
					"type":           "string",
					"title":          "AMQP URL",
					"description":    "AMQP connection URL (e.g., amqps://user:pass@host:5671/vhost)",
					"airbyte_secret": true,
					"examples":       []string{"amqps://user:password@hostname.cloudamqp.com:5671/vhost"},
				},
				"exchange": map[string]interface{}{
					"type":        "string",
					"title":       "Exchange Name",
					"description": "Name of the AMQP exchange to consume from",
					"examples":    []string{"my.exchange"},
				},
				"queue_name": map[string]interface{}{
					"type":        "string",
					"title":       "Queue Name",
					"description": "Name of the queue to create and bind to the exchange",
					"examples":    []string{"airbyte.queue"},
				},
				"binding_key": map[string]interface{}{
					"type":        "string",
					"title":       "Binding Key",
					"description": "Routing key for queue binding (empty for fanout/all messages)",
					"default":     "",
				},
				"stream_name": map[string]interface{}{
					"type":        "string",
					"title":       "Stream Name",
					"description": "Name of the Airbyte stream for the destination",
					"examples":    []string{"amqp_messages"},
				},
			},
		},
	}

	if err := emitMessage(AirbyteMessage{
		Type: "SPEC",
		Spec: &s,
	}); err != nil {
		log.Fatalf("failed to emit spec: %v", err)
	}
}

func dialWithTimeout(url string, timeout time.Duration) (*amqp.Connection, error) {
	cfg := amqp.Config{
		Dial: amqp.DefaultDial(timeout),
	}
	return amqp.DialConfig(url, cfg)
}

func check(config Config) {
	conn, err := dialWithTimeout(config.AMQPUrl, connectionTimeout)
	if err != nil {
		if err := emitMessage(AirbyteMessage{
			Type: TypeConnectionStatus,
			ConnectionStatus: &ConnectionStatus{
				Status:  "FAILED",
				Message: fmt.Sprintf("failed to connect to AMQP: %v", err),
			},
		}); err != nil {
			log.Printf("failed to emit connection status: %v", err)
		}
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		if err := emitMessage(AirbyteMessage{
			Type: TypeConnectionStatus,
			ConnectionStatus: &ConnectionStatus{
				Status:  "FAILED",
				Message: fmt.Sprintf("failed to open channel: %v", err),
			},
		}); err != nil {
			log.Printf("failed to emit connection status: %v", err)
		}
		return
	}
	defer ch.Close()

	if err := emitMessage(AirbyteMessage{
		Type: TypeConnectionStatus,
		ConnectionStatus: &ConnectionStatus{
			Status:  "SUCCEEDED",
			Message: "Successfully connected to AMQP broker",
		},
	}); err != nil {
		log.Printf("failed to emit connection status: %v", err)
	}
}

func discover(config Config) {
	catalog := Catalog{
		Streams: []Stream{
			{
				Name: config.StreamName,
				JSONSchema: map[string]interface{}{
					"$schema": "http://json-schema.org/draft-07/schema#",
					"type":    "object",
					"properties": map[string]interface{}{
						"message": map[string]interface{}{
							"type": "object",
						},
						"timestamp": map[string]interface{}{
							"type":   "string",
							"format": "date-time",
						},
					},
				},
				SupportedSyncModes:  []string{"full_refresh"},
				SourceDefinedCursor: false,
			},
		},
	}

	if err := emitMessage(AirbyteMessage{
		Type:    TypeCatalog,
		Catalog: &catalog,
	}); err != nil {
		log.Fatalf("failed to emit catalog: %v", err)
	}
}

type consumer struct {
	config       Config
	conn         *amqp.Connection
	channel      *amqp.Channel
	done         chan struct{}
	messageCount int
}

func newConsumer(config Config) *consumer {
	return &consumer{
		config: config,
		done:   make(chan struct{}),
	}
}

func (c *consumer) connect() error {
	var err error
	c.conn, err = dialWithTimeout(c.config.AMQPUrl, connectionTimeout)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		c.conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	if err := c.channel.Qos(defaultPrefetchCount, 0, false); err != nil {
		c.channel.Close()
		c.conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	_, err = c.channel.QueueDeclare(
		c.config.QueueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		c.channel.Close()
		c.conn.Close()
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	err = c.channel.QueueBind(
		c.config.QueueName,
		c.config.BindingKey,
		c.config.Exchange,
		false,
		nil,
	)
	if err != nil {
		c.channel.Close()
		c.conn.Close()
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	return nil
}

func (c *consumer) reconnect(ctx context.Context) error {
	wait := initialReconnectWait

	for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		emitLog("INFO", fmt.Sprintf("reconnection attempt %d/%d", attempt, maxReconnectAttempts))

		if err := c.connect(); err != nil {
			emitLog("WARN", fmt.Sprintf("reconnect failed: %v", err))

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}

			wait *= 2
			if wait > maxReconnectWait {
				wait = maxReconnectWait
			}
			continue
		}

		emitLog("INFO", "reconnected successfully")
		return nil
	}

	return fmt.Errorf("failed to reconnect after %d attempts", maxReconnectAttempts)
}

func (c *consumer) close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *consumer) consume(ctx context.Context) error {
	msgs, err := c.channel.Consume(
		c.config.QueueName,
		"airbyte-source-amqp",
		false, // auto-ack disabled for manual acknowledgment
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	connCloseChan := c.conn.NotifyClose(make(chan *amqp.Error, 1))
	channelCloseChan := c.channel.NotifyClose(make(chan *amqp.Error, 1))

	emitLog("INFO", fmt.Sprintf("consuming from queue=%s exchange=%s", c.config.QueueName, c.config.Exchange))

	for {
		select {
		case <-ctx.Done():
			emitLog("INFO", fmt.Sprintf("shutting down, processed %d messages", c.messageCount))
			return nil

		case err := <-connCloseChan:
			if err != nil {
				emitLog("WARN", fmt.Sprintf("connection closed: %v", err))
				return fmt.Errorf("connection closed: %w", err)
			}
			return nil

		case err := <-channelCloseChan:
			if err != nil {
				emitLog("WARN", fmt.Sprintf("channel closed: %v", err))
				return fmt.Errorf("channel closed: %w", err)
			}
			return nil

		case msg, ok := <-msgs:
			if !ok {
				emitLog("INFO", "message channel closed")
				return nil
			}

			if err := c.processMessage(msg); err != nil {
				emitLog("ERROR", fmt.Sprintf("failed to process message: %v", err))
				if nackErr := msg.Nack(false, true); nackErr != nil {
					emitLog("ERROR", fmt.Sprintf("failed to nack message: %v", nackErr))
				}
				continue
			}

			if err := msg.Ack(false); err != nil {
				emitLog("ERROR", fmt.Sprintf("failed to ack message: %v", err))
			}

			c.messageCount++
			if c.messageCount%100 == 0 {
				emitLog("INFO", fmt.Sprintf("processed %d messages", c.messageCount))
			}
		}
	}
}

func (c *consumer) processMessage(msg amqp.Delivery) error {
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Body, &data); err != nil {
		return fmt.Errorf("message is not valid JSON: %w", err)
	}

	record := Record{
		Stream: c.config.StreamName,
		Data: map[string]interface{}{
			"message":   data,
			"timestamp": time.Now().Format(time.RFC3339),
		},
		EmittedAt: time.Now().UnixMilli(),
	}

	return emitMessage(AirbyteMessage{
		Type:   TypeRecord,
		Record: &record,
	})
}

func read(config Config, _ Catalog, _ map[string]interface{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		emitLog("INFO", fmt.Sprintf("received signal %v, initiating graceful shutdown", sig))
		cancel()
	}()

	emitLog("INFO", "connecting to AMQP broker")

	c := newConsumer(config)
	defer c.close()

	if err := c.connect(); err != nil {
		emitLog("ERROR", err.Error())
		os.Exit(1)
	}

	for {
		err := c.consume(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}

		emitLog("WARN", fmt.Sprintf("consumer stopped: %v, attempting reconnect", err))
		c.close()

		if err := c.reconnect(ctx); err != nil {
			emitLog("ERROR", fmt.Sprintf("failed to reconnect: %v", err))
			os.Exit(1)
		}
	}
}

func main() {
	log.SetOutput(os.Stderr)

	if len(os.Args) < 2 {
		log.Fatal("Usage: airbyte-source-amqp <command> [--config config.json] [--catalog catalog.json] [--state state.json]")
	}

	command := os.Args[1]

	var configFile, catalogFile, stateFile string
	for i := 2; i < len(os.Args); i += 2 {
		if i+1 >= len(os.Args) {
			break
		}
		switch os.Args[i] {
		case "--config":
			configFile = os.Args[i+1]
		case "--catalog":
			catalogFile = os.Args[i+1]
		case "--state":
			stateFile = os.Args[i+1]
		}
	}

	switch command {
	case "spec":
		spec()
	case "check":
		if configFile == "" {
			log.Fatal("--config required for check command")
		}
		configData, err := os.ReadFile(configFile)
		if err != nil {
			log.Fatal(err)
		}
		var config Config
		if err := json.Unmarshal(configData, &config); err != nil {
			log.Fatal(err)
		}
		check(config)
	case "discover":
		if configFile == "" {
			log.Fatal("--config required for discover command")
		}
		configData, err := os.ReadFile(configFile)
		if err != nil {
			log.Fatal(err)
		}
		var config Config
		if err := json.Unmarshal(configData, &config); err != nil {
			log.Fatal(err)
		}
		discover(config)
	case "read":
		if configFile == "" || catalogFile == "" {
			log.Fatal("--config and --catalog required for read command")
		}
		configData, err := os.ReadFile(configFile)
		if err != nil {
			log.Fatal(err)
		}
		var config Config
		if err := json.Unmarshal(configData, &config); err != nil {
			log.Fatal(err)
		}

		catalogData, err := os.ReadFile(catalogFile)
		if err != nil {
			log.Fatal(err)
		}
		var catalog Catalog
		if err := json.Unmarshal(catalogData, &catalog); err != nil {
			log.Fatal(err)
		}

		var state map[string]interface{}
		if stateFile != "" {
			stateData, err := os.ReadFile(stateFile)
			if err == nil {
				_ = json.Unmarshal(stateData, &state)
			}
		}

		read(config, catalog, state)
	default:
		log.Fatalf("Unknown command: %s", command)
	}
}
