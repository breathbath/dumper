# set default shell
SHELL = bash -e -o pipefail

VERSION                    ?= $(shell cat ./VERSION)

## Docker related
DOCKER_EXTRA_ARGS        ?=
DOCKER_REGISTRY          ?=
DOCKER_REPOSITORY        ?=
YANDEX_IMAGE_NAME        := ${DOCKER_REGISTRY}${DOCKER_REPOSITORY}yandex-disk
DOCKER_BUILD_ARGS        ?= ${DOCKER_EXTRA_ARGS} --build-arg version="${VERSION}"

default: startAll

help:
	@echo "Usage: make [<target>]"
	@echo "where available targets are:"
	@echo
	@echo "setupYandexDisk      		 : Start yandex setup"
	@echo "buildYandexDiskDocker         : Build yandex disk docker image"
	@echo "help             	 		 : Print this help"
	@echo "startAll             		 : Rebuilds and runs the whole app stack"
	@echo "buildDumperDocker             : Build dumper docker image"
	@echo "buildAndPush             	 : Builds and pushes docker images for gauth and yandex disk"
	@echo "pushMysql             	 	 : Builds and pushes docker images for mysql"
	@echo

buildAndPush:
	docker build $(DOCKER_BUILD_ARGS) -t breathbath/yandex-disk:${VERSION} -t breathbath/yandex-disk:latest -f docker/yandex/Dockerfile .
	docker build $(DOCKER_BUILD_ARGS) -t breathbath/dumper:${VERSION} -t breathbath/dumper:latest -f docker/dumper/Dockerfile .
	docker push breathbath/yandex-disk:${VERSION}
	docker push breathbath/dumper:${VERSION}

setupYandexDisk:
	docker-compose run --entrypoint '' yandex_disk yandex-disk setup

buildYandexDiskDocker:
	docker-compose build yandex_disk

buildDumperDocker:
	docker-compose build dumper

startAll:
	docker-compose up -d

pushMysql:
	docker build $(DOCKER_BUILD_ARGS) -t breathbath/mysql8:${VERSION} -t breathbath/mysql8:latest -f docker/mysql/Dockerfile .
	docker push breathbath/mysql8:${VERSION}
