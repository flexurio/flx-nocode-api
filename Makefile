# Makefile for flx-nocode-api

# Variables
CARGO = cargo
BINARY_NAME = flx-nocode-api
RELEASE_DIR = target/release
DEBUG_DIR = target/debug

# Colors for help message
BLUE = \033[1;34m
NC = \033[0m

.PHONY: all build release run test clean check fmt lint doc help docker-build docker-up docker-down mcp-stdio mcp-inspect mcp-inspect-stdio

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
	@echo "  mcp-stdio     Run the MCP server over stdio (needs MCP_STDIO_EMAIL or MCP_STDIO_TOKEN)"
	@echo "  mcp-inspect   List MCP tools via MCP Inspector CLI (MCP_PORT=8080 TOKEN=<jwt>)"
	@echo "  mcp-inspect-stdio  Open MCP Inspector on the stdio transport"

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

# ── MCP (Model Context Protocol) ─────────────────────────────────────────────
MCP_PORT ?= $(or $(PORT),8080)

mcp-stdio:
	$(CARGO) run -- mcp --stdio

mcp-inspect:
	npx -y @modelcontextprotocol/inspector --cli http://localhost:$(MCP_PORT)/mcp \
		--transport http --method tools/list \
		$(if $(TOKEN),--header "Authorization: Bearer $(TOKEN)",)

mcp-inspect-stdio: build
	npx -y @modelcontextprotocol/inspector $(DEBUG_DIR)/$(BINARY_NAME) mcp --stdio
