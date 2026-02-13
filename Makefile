.PHONY: build test test-unit test-integration test-bench lint fmt vet clean docker docker-up docker-down docker-deps migrate

# Build
build:
	CGO_ENABLED=0 go build -o bin/ttdust ./cmd/server/main.go

# Testing
test:
	go test -v -race ./...

test-unit:
	go test -v -race -short ./internal/...

test-integration:
	go test -v -race ./tests/...

test-bench:
	go test -bench=. -benchmem ./tests/benchmarks/...

test-coverage:
	go test -race -coverprofile=coverage.out -covermode=atomic ./internal/...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Code quality
lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

vet:
	go vet ./...

check: fmt vet lint test-unit

# Docker
docker:
	docker build -t ttdust:local .

docker-debug:
	docker build --target debug -t ttdust:debug .

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-deps:
	docker-compose up -d postgres redis minio minio-init

docker-observability:
	docker-compose --profile observability up -d

docker-clean:
	docker-compose down -v

# Database
migrate:
	docker-compose run --rm migrate

# Proto generation
proto:
	protoc --go_out=. --go-grpc_out=. proto/*.proto

# Clean
clean:
	rm -rf bin/ coverage.out coverage.html
