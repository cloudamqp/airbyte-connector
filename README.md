# Airbyte Connector

An Airbyte source connector for consuming messages from AMQP brokers (RabbitMQ, LavinMQ, etc.).

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `amqp_url` | Yes | AMQP connection URL (e.g., `amqps://user:pass@host:5671/vhost`) |
| `exchange` | Yes | Name of the AMQP exchange to consume from |
| `queue_name` | Yes | Name of the queue to create and bind to the exchange |
| `binding_key` | No | Routing key for queue binding (empty for fanout/all messages) |
| `stream_name` | Yes | Name of the Airbyte stream for the destination |

## Usage

### With Airbyte

The connector is available on Docker Hub:

```
ericaweistrand/airbyte-source-amqp:latest
```

To add as a custom connector in Airbyte:
1. Go to Settings -> Sources -> + New connector
2. Enter Docker repository: `ericaweistrand/airbyte-source-amqp`
3. Enter Docker image tag: `latest` (or a specific version like `1.0.0`)

### Local Development

Build:
```bash
go build -o airbyte-connector .
```

Test:
```bash
go test -v ./...
```

Run commands:
```bash
# Get connector spec
./airbyte-connector spec

# Check connection
./airbyte-connector check --config config.json

# Discover schema
./airbyte-connector discover --config config.json

# Read data
./airbyte-connector read --config config.json --catalog catalog.json
```

### Docker

Build:
```bash
docker build -t airbyte-source-amqp .
```

Run:
```bash
docker run airbyte-source-amqp spec
```

## Release

To release a new version:

1. Create and push a tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

2. GitHub Actions will automatically build and push the Docker image to Docker Hub with tags:
   - `ericaweistrand/airbyte-source-amqp:1.0.0`
   - `ericaweistrand/airbyte-source-amqp:latest`

### Required Secrets

Set these secrets in GitHub repository settings:
- `DOCKERHUB_USERNAME`: Docker Hub username
- `DOCKERHUB_TOKEN`: Docker Hub access token
