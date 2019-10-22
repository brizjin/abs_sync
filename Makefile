PRJ_DIR = $(shell pwd)
DOCKER_REGISTRY ?= 172.21.13.239:8090
DOCKER_IMAGE := abs/sync_script:latest
HOME = ${USERPROFILE}

.PHONY: all build push clean it

all: build push clean

build: cp_ssh_key
	docker build --tag "$(DOCKER_IMAGE)" .

run: build
	docker run --rm -it abs/sync_script

it:
	docker run --rm -it abs/sync_script bash

clear:
	rm -rf $(PRJ_DIR)/.pytest_cache $(PRJ_DIR)/dbs $(PRJ_DIR)/logs $(PRJ_DIR)/.ssh

COPIED := $(PRJ_DIR)/.makefile/ssh_copied
$(COPIED):
	-cp -R $(HOME)/.ssh $(PRJ_DIR)/.ssh
	-mkdir ./.makefile
	touch $(COPIED)
cp_ssh_key: $(COPIED)