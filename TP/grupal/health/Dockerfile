FROM golang:1.24.2-alpine3.21 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY ./health ./health
COPY ./common ./common

RUN go build -o health.bin ./health



FROM alpine:latest

WORKDIR /app

RUN apk add --no-cache docker-cli docker-compose

COPY --from=builder /app/health.bin ./

ENTRYPOINT [ "./health.bin" ]
