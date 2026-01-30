# Makefile for flx-nocode-api

# Variables
CARGO = cargo
BINARY_NAME = flx-nocode-api
RELEASE_DIR = target/release
DEBUG_DIR = target/debug

# Colors for help message
BLUE = \033[1;34m
NC = \033[0m

.PHONY: all build release run test clean check fmt lint doc help docker-build docker-up docker-down

all: build

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build         Build the project in debug mode"
	@echo "  release       Build the project in release mode"
	@echo "  run           Run the project in debug mode"
	@echo "  test          Run tests"
	@echo "  clean         Clean build artifacts"
	@echo "  check         Check the code for errors"
	@echo "  fmt           Format the code"
	@echo "  lint          Lint the code using clippy"
	@echo "  doc           Generate documentation"
	@echo "  docker-build  Build docker containers"
	@echo "  docker-up     Start docker containers"
	@echo "  docker-down   Stop docker containers"

build:
	$(CARGO) build

release:
	$(CARGO) build --release

run:
	$(CARGO) run

test:
	$(CARGO) test

clean:
	$(CARGO) clean

check:
	$(CARGO) check

fmt:
	$(CARGO) fmt

lint:
	$(CARGO) clippy -- -D warnings

doc:
	$(CARGO) doc --no-deps --open

docker-build:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down
