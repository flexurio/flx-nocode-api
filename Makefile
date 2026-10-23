# Makefile for flx-nocode-api

# Variables
CARGO = cargo
BINARY_NAME = flx-nocode-api
RELEASE_DIR = target/release
DEBUG_DIR = target/debug

# Colors for help message
BLUE = \033[1;34m
NC = \033[0m

.PHONY: all build release release-version run test test-e2e bench perf-baseline clean check fmt lint doc help docker-build docker-up docker-down mcp-stdio mcp-inspect mcp-inspect-stdio

all: build

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build            Build the project in debug mode"
	@echo "  release          Build the project in release mode"
	@echo "  release-version  Create release tag & push (e.g. make release-version VERSION=patch)"
	@echo "  run              Run the project in debug mode"
	@echo "  test             Run unit tests"
	@echo "  test-e2e         Boot the binary (SQLite) and run the HTTP end-to-end test"
	@echo "  bench            Criterion benchmarks for the query compiler / planner"
	@echo "  perf-baseline    Run every k6 scenario against BASE_URL (default :8080), save to docs/perf/<date>/"
	@echo "  clean            Clean build artifacts"
	@echo "  check            Check the code for errors"
	@echo "  fmt              Format the code"
	@echo "  lint             Lint the code using clippy"
	@echo "  doc              Generate documentation"
	@echo "  docker-build     Build docker containers"
	@echo "  docker-up        Start docker containers"
	@echo "  docker-down      Stop docker containers"
	@echo "  mcp-stdio        Run the MCP server over stdio (needs MCP_STDIO_EMAIL or MCP_STDIO_TOKEN)"
	@echo "  mcp-inspect      List MCP tools via MCP Inspector CLI (MCP_PORT=8080 TOKEN=<jwt>)"
	@echo "  mcp-inspect-stdio Open MCP Inspector on the stdio transport"

build:
	$(CARGO) build

release:
	$(CARGO) build --release

release-version:
	@chmod +x scripts/release.sh
	@./scripts/release.sh $(VERSION)

run:
	$(CARGO) run

test:
	$(CARGO) test --lib --bins

test-e2e:
	$(CARGO) test --test e2e_http -- --nocapture

bench:
	$(CARGO) bench --bench query_compiler

# ── Performance baseline (k6) ────────────────────────────────────────────────
BASE_URL ?= http://localhost:8080
PERF_OUT ?= docs/perf/$(shell date +%Y%m%d-%H%M)
perf-baseline:
	@mkdir -p $(PERF_OUT)
	@for s in get-list-cached get-list-uncached post-master-detail login-burst mixed; do \
		echo "== $$s"; \
		BASE_URL=$(BASE_URL) k6 run --quiet --summary-export=$(PERF_OUT)/$$s.json perf/k6/$$s.js || true; \
	done
	@echo "results in $(PERF_OUT)"

clean:
	$(CARGO) clean

check:
	$(CARGO) check

fmt:
	$(CARGO) fmt

lint:
	$(CARGO) clippy --all-targets -- -D clippy::correctness -D clippy::suspicious -D clippy::complexity -D clippy::perf -D clippy::style -D unused -D dead_code

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
