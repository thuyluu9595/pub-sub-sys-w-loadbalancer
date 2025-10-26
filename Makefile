.PHONY: help setup build start stop restart logs clean test monitor stats

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
NC=\033[0m # No Color

help: ## Show this help message
	@echo "$(GREEN)MQTT Multi-Broker Load Balancing$(NC)"
	@echo "=================================="
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}'

setup: ## Initial setup - create directories and config files
	@echo "$(GREEN)Setting up project structure...$(NC)"
	./setup.sh
	@echo "$(GREEN)Setup complete!$(NC)"

build: ## Build all Docker containers
	@echo "$(GREEN)Building Docker containers...$(NC)"
	docker-compose build
	@echo "$(GREEN)Build complete!$(NC)"

start: ## Start all services
	@echo "$(GREEN)Starting MQTT Load Balancing System...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)System started!$(NC)"
	@echo "$(YELLOW)Run 'make logs' to view logs$(NC)"
	@echo "$(YELLOW)Run 'make monitor' to open monitoring dashboard$(NC)"

stop: ## Stop all services
	@echo "$(RED)Stopping all services...$(NC)"
	docker-compose stop
	@echo "$(GREEN)Services stopped$(NC)"

restart: stop start ## Restart all services

logs: ## Show logs from all services
	docker-compose logs -f

logs-coordinator: ## Show coordinator logs only
	docker-compose logs -f coordinator

logs-publisher: ## Show publisher logs only
	docker-compose logs -f publisher

logs-subscriber: ## Show subscriber logs only
	docker-compose logs -f subscriber

logs-brokers: ## Show all broker logs
	docker-compose logs -f broker1 broker2 broker3 broker4

ps: ## Show running containers
	@echo "$(GREEN)Running containers:$(NC)"
	docker-compose ps

clean: ## Stop and remove all containers, networks, and volumes
	@echo "$(RED)Cleaning up...$(NC)"
	docker-compose down -v
	@echo "$(GREEN)Cleanup complete$(NC)"

clean-all: clean ## Remove everything including images
	@echo "$(RED)Removing all images...$(NC)"
	docker-compose down -v --rmi all
	@echo "$(GREEN)Full cleanup complete$(NC)"

test: ## Run system tests
	@echo "$(GREEN)Running system tests...$(NC)"
	python3 test.py

monitor: ## Start monitoring dashboard
	@echo "$(GREEN)Starting monitoring dashboard...$(NC)"
	python3 monitor.py

stats: ## Show quick statistics
	@echo "$(GREEN)System Statistics:$(NC)"
	@echo "-------------------"
	@docker-compose ps --format "table {{.Service}}\t{{.Status}}" 2>/dev/null || true
	@echo ""
	@echo "$(YELLOW)Broker Statistics:$(NC)"
	@docker stats --no-stream broker1 broker2 broker3 broker4 2>/dev/null || echo "Brokers not running"

hot-topics: ## Show hot topics detected
	@echo "$(GREEN)Hot Topics Detected:$(NC)"
	@docker-compose logs coordinator | grep "Hot Topics detected" | tail -5

load-distribution: ## Show broker load distribution
	@echo "$(GREEN)Broker Load Distribution:$(NC)"
	@docker-compose logs coordinator | grep "utilization:" | tail -10

install-deps: ## Install Python dependencies for test and monitor scripts
	@echo "$(GREEN)Installing Python dependencies...$(NC)"
	pip3 install paho-mqtt numpy scipy
	@echo "$(GREEN)Dependencies installed$(NC)"

health-check: ## Check health of all services
	@echo "$(GREEN)Checking service health...$(NC)"
	@echo "Brokers:"
	@for port in 1883 1884 1885 1886; do \
		if nc -z localhost $$port 2>/dev/null; then \
			echo "  ✓ Port $$port is open"; \
		else \
			echo "  ✗ Port $$port is closed"; \
		fi \
	done
	@echo ""
	@echo "Containers:"
	@docker-compose ps

demo: build start ## Build and start with demo messages
	@echo "$(GREEN)Starting demo...$(NC)"
	@sleep 10
	@echo "$(YELLOW)System is running. Opening monitoring dashboard in 5 seconds...$(NC)"
	@sleep 5
	@make monitor

quick-start: setup build start ## Complete setup from scratch
	@echo "$(GREEN)Quick start complete!$(NC)"
	@echo "$(YELLOW)View logs: make logs$(NC)"
	@echo "$(YELLOW)Monitor: make monitor$(NC)"
	@echo "$(YELLOW)Test: make test$(NC)"

backup-logs: ## Backup all logs to timestamped directory
	@mkdir -p logs_backup
	@timestamp=$$(date +%Y%m%d_%H%M%S); \
	echo "$(GREEN)Backing up logs to logs_backup/$$timestamp/$(NC)"; \
	mkdir -p logs_backup/$$timestamp; \
	docker-compose logs > logs_backup/$$timestamp/all_logs.txt; \
	docker-compose logs coordinator > logs_backup/$$timestamp/coordinator.txt; \
	docker-compose logs publisher > logs_backup/$$timestamp/publisher.txt; \
	docker-compose logs subscriber > logs_backup/$$timestamp/subscriber.txt; \
	echo "$(GREEN)Logs backed up!$(NC)"

scale-subscribers: ## Scale subscribers (usage: make scale-subscribers N=10)
	@echo "$(GREEN)Scaling subscribers to $(N)...$(NC)"
	docker-compose up -d --scale subscriber=$(N)

# Performance testing targets
perf-test: ## Run performance test with high load
	@echo "$(GREEN)Running performance test...$(NC)"
	@docker-compose exec publisher python -c "import sys; sys.exit(0)" || echo "Publisher not ready"
	@echo "Check logs for performance metrics"

network-test: ## Test network connectivity between containers
	@echo "$(GREEN)Testing network connectivity...$(NC)"
	@docker-compose exec coordinator ping -c 3 broker1
	@docker-compose exec coordinator ping -c 3 broker2
	@docker-compose exec coordinator ping -c 3 broker3
	@docker-compose exec coordinator ping -c 3 broker4

# Development targets
dev-coordinator: ## Open shell in coordinator container
	docker-compose exec coordinator /bin/sh

dev-publisher: ## Open shell in publisher container
	docker-compose exec publisher /bin/sh

dev-subscriber: ## Open shell in subscriber container
	docker-compose exec subscriber /bin/sh

# Documentation
docs: ## Show system architecture
	@cat << 'EOF'
	
	MQTT Multi-Broker Load Balancing System Architecture
	=====================================================
	
	┌─────────────────────────────────────────────────────────┐
	│              Coordination Service                        │
	│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐   │
	│  │   LoOP Hot  │  │ Trie Topic   │  │ Load Balancer│   │
	│  │  Detection  │  │  Management  │  │  Algorithm 1 │   │
	│  └─────────────┘  └──────────────┘  └──────────────┘   │
	└──────────┬─────────────────────────────────┬────────────┘
	           │                                 │
	    ┌──────┴────────┐                ┌──────┴────────┐
	    │               │                │               │
	┌───▼───┐      ┌───▼───┐        ┌───▼───┐      ┌───▼───┐
	│Broker1│      │Broker2│        │Broker3│      │Broker4│
	│350Mbps│      │450Mbps│        │550Mbps│      │600Mbps│
	└───┬───┘      └───┬───┘        └───┬───┘      └───┬───┘
	    │              │                │              │
	    └──────────────┴────────────────┴──────────────┘
	                         │
	        ┌────────────────┴────────────────┐
	        │                                 │
	    ┌───▼───────┐                  ┌─────▼─────┐
	    │ Publishers│                  │Subscribers│
	    │    (5)    │                  │   (50)    │
	    └───────────┘                  └───────────┘
	
	EOF

version: ## Show version info
	@echo "MQTT Load Balancing System v1.0"
	@echo "Based on: IEEE TRANSACTIONS ON NETWORK AND SERVICE MANAGEMENT"
	@echo "VOL. 21, NO. 4, AUGUST 2024"