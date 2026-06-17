.PHONY: build test test-integration vet lint fmt tidy clean all

BINARY := lbsync
PKG    := ./...

all: lint test build

build:
	go build -trimpath -ldflags="-s -w" -o bin/$(BINARY) ./cmd/lbsync

test:
	go test -race -count=1 $(PKG)

test-integration:
	go test -tags integration -count=1 ./internal/cluster/...

vet:
	go vet $(PKG)

lint:
	golangci-lint run

fmt:
	golangci-lint fmt

tidy:
	go mod tidy

clean:
	rm -rf bin
