FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git gcc musl-dev

WORKDIR /app


RUN go install github.com/swaggo/swag/cmd/swag@latest
RUN go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Generate Swagger docs before building..
RUN swag init -g cmd/api/main.go

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/etl ./cmd/etl

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata wget

WORKDIR /app

COPY --from=builder /bin/api /app/api
COPY --from=builder /bin/etl /app/etl

EXPOSE 8080

ENTRYPOINT ["/app/api"]
