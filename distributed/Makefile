.PHONY: all addlicense clean start stop up down run _run shell _shell web _web usa2018 _usa2018

SHELL              	:= /bin/bash
REQUIRED_BINS      	:= docker docker-compose

WORKERS            	:= 4
DEMO		   		:= 0
SPARK_MASTER_NAME  	:= spark-master
SPARK_MASTER_PORT	:= 7077

all: | start run stop

check_deps:
	$(foreach bin,$(REQUIRED_BINS),\
		$(if $(shell which $(bin)),,$(error Please install `$(bin)`)))

clean: check_deps
	@ echo -e "\n[*] Removing docker containers.\n"
	docker-compose rm --force --stop -v
	@- rm -f .*.build
	@ find . -type f -name '*.pyc' -delete
	@ find . -type d -name '__pycache__' -delete

.spark.build: $(shell find spark -type f)
	docker-compose build spark-master spark-worker
	@ touch $@

start up: check_deps .spark.build
	@ echo -e "\n[*] Starting Apache Spark cluster on docker with $(WORKERS) workers.\n"
	docker-compose up -d --scale spark-worker=$(WORKERS) spark-master spark-worker namenode datanode
	@ echo -e "\n[*] Wait till Hadoop is up."
	@ sleep 3
	@ echo -e "\n[*] Initializing Hadoop Distributed File System.\n"
	docker-compose run init-hdfs
	@ echo -e "\n[*] Starting Apache Spark history server.\n"
	docker-compose up -d spark-history-server

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

run adults: | start _run stop

_run _adults: check_deps
	@ echo -e "\n[*] Running mondrian on dataset/adults.csv.\n"
	- docker-compose run \
		-p 4040:4040 \
		-e LOCAL_DATASET=/mondrian/dataset/adults.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/adults.csv \
		-e SPARK_APP_CONFIG=/mondrian/config/adults.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		mondrian-app

usa2018: | start _usa2018 stop

_usa2018: check_deps
	@ echo -e "\n[*] Running mondrian on dataset/usa2018.csv.\n"
	- docker-compose run \
		-p 4040:4040 \
		-e LOCAL_DATASET=/mondrian/dataset/usa2018.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/usa2018.csv \
		-e SPARK_APP_CONFIG=/mondrian/config/usa2018.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		mondrian-app
