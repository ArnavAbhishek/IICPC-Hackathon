.PHONY: build up down demo fleet logs test clean

build: ## compile all services locally
	go build ./...

test:
	go vet ./...
	go test ./...

up: ## build images and start the platform
	docker compose up -d --build

down:
	docker compose down

demo: ## upload reference + chaos engines, run benchmarks, tail results
	./scripts/demo.sh

N ?= 4
fleet: ## scale the bot fleet: make fleet N=8
	docker compose up -d --scale botfleet=$(N) --no-recreate

logs:
	docker compose logs -f --tail 50

clean: ## full teardown including volumes and stray sandboxes
	-docker ps -aq --filter label=arena.run | xargs -r docker rm -f
	docker compose down -v
