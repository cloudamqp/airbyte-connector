FROM golang:1.23-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o airbyte-source-amqp .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /airbyte/integration_code

COPY --from=builder /build/airbyte-source-amqp ./

ENV AIRBYTE_ENTRYPOINT="/airbyte/integration_code/airbyte-source-amqp"

ENTRYPOINT ["/airbyte/integration_code/airbyte-source-amqp"]

LABEL io.airbyte.name=airbyte-source-amqp
