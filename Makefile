.PHONY: all addlicense clean start stop up down run _run shell _shell web _web local local-adults local-usa2018 usa2018 _usa2018

SHELL              := /bin/bash
REQUIRED_BINS      := docker docker-compose

WORKERS            := 4
DEMO		   := 0
SPARK_MASTER_NAME  := spark-master
SPARK_MASTER_PORT  := 7077

LICENSE_TYPE   := "apache"
LICENSE_HOLDER := "Unibg Seclab (https://seclab.unibg.it)"

all: | start run stop

check_deps:
	$(foreach bin,$(REQUIRED_BINS),\
		$(if $(shell which $(bin)),,$(error Please install `$(bin)`)))

clean: | _clean_local _clean_docker

_clean_local:
	make -C local clean

_clean_docker: check_deps
	@ echo -e "\n[*] Removing docker containers.\n"
	docker-compose rm --force --stop -v
	@- rm -f .*.build

.spark.build: $(shell find spark -type f)
	docker-compose build spark-master spark-worker
	@ touch $@

start up: check_deps .spark.build
	@ echo -e "\n[*] Starting Apache Spark cluster on docker with $(WORKERS) workers.\n"
	docker-compose up -d --scale spark-worker=$(WORKERS) spark-master spark-worker namenode datanode

stop down: check_deps
	@ echo -e "\n[*] Shutting down Apache Spark cluster.\n"
	docker-compose kill

_shell: check_deps
	@ echo -e "\n[*] Running pyspark.\n"
	- docker-compose run spark-shell

shell: | start _shell stop

.jupyter.build: $(shell find jupyter -type f)
	docker-compose build jupyter
	@ touch $@

_web: check_deps .jupyter.build
	@ echo -e "\n[*] Running jupyter.\n"
	- docker-compose run --service-ports jupyter

web: | start _web stop

run adults _adults:
run adults: | start _run stop

_run _adults: check_deps
	@ echo -e "\n[*] Running mondrian on /mondrian/adults.json.\n"
	- docker-compose run \
		-e LOCAL_DATASET=/mondrian/adults.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/adults.csv \
		-e SPARK_FILES=/mondrian/adults.json \
		-e SPARK_APP_CONFIG=/mondrian/adults.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		mondrian-app

usa2018 _usa2018:
usa2018: | start _usa2018 stop

_usa2018: check_deps
	@ echo -e "\n[*] Running mondrian on /mondrian/usa2018.json.\n"
	- docker-compose run \
		-e LOCAL_DATASET=/mondrian/usa2018.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/usa2018.csv \
		-e SPARK_FILES=/mondrian/usa2018.json \
		-e SPARK_APP_CONFIG=/mondrian/usa2018.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		mondrian-app


local local-adults:
	make -C local adults

local-usa2018:
	make -C local usa2018

addlicense:
	go get -u github.com/google/addlicense
	$(shell go env GOPATH)/bin/addlicense -c $(LICENSE_HOLDER) -l $(LICENSE_TYPE) .
