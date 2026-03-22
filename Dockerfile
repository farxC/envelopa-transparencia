FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/etl ./cmd/etl

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata wget

WORKDIR /app

COPY --from=builder /bin/api /app/api
COPY --from=builder /bin/etl /app/etl

EXPOSE 8080

ENTRYPOINT ["/app/api"]
