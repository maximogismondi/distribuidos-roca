# Makefile for managing Docker Compose

# Variables
docker_compose_file := docker-compose.yaml

# Targets
.PHONY: up down restart logs build



docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./worker/Dockerfile -t "worker:latest" .
	docker build -f ./proxy/Dockerfile -t "proxy:latest" .
	docker build -f ./health/Dockerfile -t "health:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

# Start the Docker Compose services
up: docker-image
	docker compose -f $(docker_compose_file) up -d --build

# Stop the Docker Compose services
down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down

# Restart the Docker Compose services
restart: down up

# View logs of the Docker Compose services
logs:
	docker-compose -f $(docker_compose_file) logs -f

# Build the Docker Compose services
build:
	docker-compose -f $(docker_compose_file) build