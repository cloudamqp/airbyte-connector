package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TypeLog              = "LOG"
	TypeConnectionStatus = "CONNECTION_STATUS"
	TypeCatalog          = "CATALOG"
	TypeRecord           = "RECORD"
	TypeState            = "STATE"
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

func emitMessage(msg AirbyteMessage) {
	data, _ := json.Marshal(msg)
	fmt.Println(string(data))
}

func emitLog(level, message string) {
	emitMessage(AirbyteMessage{
		Type: TypeLog,
		Log: &LogMessage{
			Level:   level,
			Message: message,
		},
	})
}

func spec() {
	s := Spec{
		DocumentationURL: "https://github.com/84codes/airbyte-source-amqp",
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
					"examples":       []string{"amqps://user:password@hostname.lavinmq.com:5671/vhost"},
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

	emitMessage(AirbyteMessage{
		Type: "SPEC",
		Spec: &s,
	})
}

func check(config Config) {
	conn, err := amqp.Dial(config.AMQPUrl)
	if err != nil {
		emitMessage(AirbyteMessage{
			Type: TypeConnectionStatus,
			ConnectionStatus: &ConnectionStatus{
				Status:  "FAILED",
				Message: fmt.Sprintf("failed to connect to AMQP: %v", err),
			},
		})
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		emitMessage(AirbyteMessage{
			Type: TypeConnectionStatus,
			ConnectionStatus: &ConnectionStatus{
				Status:  "FAILED",
				Message: fmt.Sprintf("failed to open channel: %v", err),
			},
		})
		return
	}
	defer ch.Close()

	emitMessage(AirbyteMessage{
		Type: TypeConnectionStatus,
		ConnectionStatus: &ConnectionStatus{
			Status:  "SUCCEEDED",
			Message: "Successfully connected to AMQP broker",
		},
	})
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

	emitMessage(AirbyteMessage{
		Type:    TypeCatalog,
		Catalog: &catalog,
	})
}

func read(config Config, _ Catalog, _ map[string]interface{}) {
	emitLog("INFO", "connecting to AMQP broker")

	conn, err := amqp.Dial(config.AMQPUrl)
	if err != nil {
		emitLog("ERROR", fmt.Sprintf("failed to connect: %v", err))
		os.Exit(1)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		emitLog("ERROR", fmt.Sprintf("failed to open channel: %v", err))
		os.Exit(1)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		config.QueueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		emitLog("ERROR", fmt.Sprintf("failed to declare queue: %v", err))
		os.Exit(1)
	}

	err = ch.QueueBind(
		config.QueueName,
		config.BindingKey,
		config.Exchange,
		false,
		nil,
	)
	if err != nil {
		emitLog("ERROR", fmt.Sprintf("failed to bind queue: %v", err))
		os.Exit(1)
	}

	emitLog("INFO", fmt.Sprintf("consuming from queue=%s exchange=%s", config.QueueName, config.Exchange))

	msgs, err := ch.Consume(
		config.QueueName,
		"airbyte-source-amqp",
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		emitLog("ERROR", fmt.Sprintf("failed to start consumer: %v", err))
		os.Exit(1)
	}

	messageCount := 0
	for msg := range msgs {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Body, &data); err != nil {
			emitLog("WARN", fmt.Sprintf("failed to parse message as JSON: %v", err))
			continue
		}

		record := Record{
			Stream: config.StreamName,
			Data: map[string]interface{}{
				"message":   data,
				"timestamp": time.Now().Format(time.RFC3339),
			},
			EmittedAt: time.Now().UnixMilli(),
		}

		emitMessage(AirbyteMessage{
			Type:   TypeRecord,
			Record: &record,
		})

		messageCount++
		if messageCount%100 == 0 {
			emitLog("INFO", fmt.Sprintf("processed %d messages", messageCount))
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
