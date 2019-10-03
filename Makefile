PRJ_DIR = $(shell pwd)
DOCKER_REGISTRY ?= 172.21.13.239:8090
DOCKER_IMAGE := abs/sync_script:latest

.PHONY: all build push clean it

all: build push clean

build:
	docker build --tag "$(DOCKER_IMAGE)" .

clear:
	rm -rf $(PRJ_DIR)/.pytest_cache $(PRJ_DIR)/dbs $(PRJ_DIR)/logs