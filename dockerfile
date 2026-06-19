# ---- Build stage ----
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o feature-store .

# ---- Run stage ----
FROM alpine:3.19

WORKDIR /app

COPY --from=builder /app/feature-store .

EXPOSE 8080

ENTRYPOINT ["./feature-store"]