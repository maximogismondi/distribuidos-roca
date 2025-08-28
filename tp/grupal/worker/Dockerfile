FROM golang:1.24.2-alpine3.21 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY ./worker ./worker
COPY ./common ./common

RUN go build -o worker.bin ./worker



FROM busybox:latest

WORKDIR /app

COPY --from=builder /app/worker.bin ./

ENTRYPOINT [ "./worker.bin" ]
