FROM golang:1.24.2-alpine3.21 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY ./proxy ./proxy
COPY ./common ./common

RUN go build -o proxy.bin ./proxy


FROM busybox:latest

WORKDIR /app

COPY --from=builder /app/proxy.bin ./

ENTRYPOINT [ "./proxy.bin" ]
