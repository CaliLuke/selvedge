# Selvedge - Development Commands
# Run `make` or `make help` to see available commands

.PHONY: help build test test-coverage test-verbose lint lint-fix fmt tidy clean install-tools install-hooks check check-fast

# Default target - show help
.DEFAULT_GOAL := help

# Go parameters
GOCMD := go
GOBUILD := $(GOCMD) build
GOTEST := $(GOCMD) test
GOFMT := $(GOCMD) fmt
GOMOD := $(GOCMD) mod
GOCLEAN := $(GOCMD) clean

# golangci-lint
include scripts/quality/tools.env
GOLANGCI_LINT := $(shell which golangci-lint 2>/dev/null || echo "$(shell go env GOPATH)/bin/golangci-lint")

help: ## Show this help message
	@echo "Selvedge - Development Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the project
	$(GOBUILD) ./...

test: ## Run tests
	$(GOTEST) ./... -count=1

test-verbose: ## Run tests with verbose output
	$(GOTEST) ./... -v -count=1

test-coverage: check ## Run full tests and enforce package coverage

lint: ## Run golangci-lint
	$(GOLANGCI_LINT) run

lint-fix: ## Run golangci-lint with auto-fix
	$(GOLANGCI_LINT) run --fix

fmt: ## Apply Go formatting and import fixes
	$(GOLANGCI_LINT) fmt

tidy: ## Run go mod tidy
	$(GOMOD) tidy

clean: ## Clean build artifacts and caches
	$(GOCLEAN) ./...
	rm -f *.db *.db-shm *.db-wal
	rm -rf .golangci-lint-cache

install-tools: ## Install development tools (golangci-lint)
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

install-hooks: ## Install Prek pre-commit and pre-push hooks
	prek install --hook-type pre-commit --hook-type pre-push

check-fast: ## Check build, formatting, lint, hygiene and maintainability without edits
	GOLANGCI_LINT="$(GOLANGCI_LINT)" ./scripts/check.sh fast

check: ## Run all quality gates, including fresh race tests and package coverage
	GOLANGCI_LINT="$(GOLANGCI_LINT)" ./scripts/check.sh full
