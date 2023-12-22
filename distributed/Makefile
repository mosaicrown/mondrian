.PHONY: _adults _poker _run _sub_k _transactions _usa1990 _usa2018 _usa2019 all add_workers adults check_deps check_spark_image clean clean_test_files down poker run start stop transactions up usa1990 usa2018 usa2019

SHELL              	:= /bin/bash

PACKAGE				:= mondrian
PACKAGE_ZIP			:= $(PACKAGE).zip
OUTPUT_DIR	   		:= anonymized

DATANODES			:= 1
WORKERS            	:= 4
DEMO		   		:= 0
TEST				:= 0
SPARK_MASTER_NAME  	:= spark-master
SPARK_MASTER_PORT	:= 7077
WORKER_MEMORY		:= 2G
DRIVER_MEMORY		:= 2G

COMPOSE				:= DATANODES=$(DATANODES) WORKERS=$(WORKERS) SPARK_WORKER_MEMORY=$(WORKER_MEMORY) docker compose

K := -1
CONFIG := placeholder

# add ./bin to the path
export PATH = $(shell printenv PATH):$(ROOT_DIR)/$(LOCAL_BINARIES)

all: | start run stop

# PERCOM Artifact experiments
clean_test_files:
	docker exec -t spark-driver /mondrian/script/clean_files.sh

check_deps:
	$(if $(shell which docker),,$(error Please install `docker`))
	$(if $(shell which docker compose),,$(error Please install `docker compose`))

check_spark_image:
	$(if $(shell docker images -q spark),,docker build -t spark ./spark)

clean: check_deps
	@ echo -e "\n[*] Removing docker containers.\n"
	$(COMPOSE) rm --force --stop -v
	@- rm -f .*.build
	@- rm -f $(PACKAGE_ZIP)

.spark.build: $(shell find spark -type f)
	$(COMPOSE) build spark-master spark-worker
	@ touch $@

_sub_k:
	@if [ ${K} != -1 ]; then\
		sed -i -e 's/"K":.\+,/"K": ${K},/g' ./config/${CONFIG}.json;\
	fi	

start up: check_deps .spark.build
	@ echo -e "\n[*] Starting Apache Spark cluster on docker with $(WORKERS) workers.\n"
	$(COMPOSE) up -d spark-master spark-worker namenode datanode spark-driver
	@ echo -e "\n[*] Wait till Hadoop is up."
	@ sleep 5
	@ echo -e "\n[*] Initializing Hadoop Distributed File System.\n"
	$(COMPOSE) run --rm init-hdfs
	@ echo -e "\n[*] Starting Apache Spark history server.\n"
	$(COMPOSE) up -d spark-history-server

add_workers:
	docker service scale spark_spark-worker=$(WORKERS)

stop down: check_deps
	@ echo -e "\n[*] Shutting down Apache Spark cluster.\n"
	$(COMPOSE) stop

$(PACKAGE_ZIP): $(PACKAGE)
	cd $(PACKAGE); zip -r ../$(PACKAGE_ZIP) .

$(OUTPUT_DIR):
	mkdir -p $(OUTPUT_DIR)

run adults: | start _run stop

_run _adults: check_deps $(PACKAGE_ZIP) $(OUTPUT_DIR)
	@ echo -e "\n[*] Running mondrian on dataset/adults.csv.\n"
	- docker exec \
		-t \
		-e LOCAL_DATASET=/mondrian/dataset/adults.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/adults.csv \
		-e HDFS_ANONYMIZED_DATASET=hdfs://namenode:8020/anonymized/adults.csv \
		-e LOCAL_ANONYMIZED_DATASET=/mondrian/anonymized/adults.csv \
		-e SPARK_APP_CONFIG=/mondrian/config/adults.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		-e SPARK_APP_TEST=$(TEST) \
		spark-driver \
		/mondrian/script/submit.sh

usa2018: | start _sub_k _usa2018 stop

_usa2018: check_deps $(PACKAGE_ZIP) $(OUTPUT_DIR)
	@ echo -e "\n[*] Running mondrian on dataset/usa2018.csv.\n"
	- docker exec \
		-t \
		-e LOCAL_DATASET=/mondrian/dataset/usa2018.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/usa2018.csv \
		-e HDFS_ANONYMIZED_DATASET=hdfs://namenode:8020/anonymized/usa2018.csv \
		-e LOCAL_ANONYMIZED_DATASET=/mondrian/anonymized/usa2018.csv \
		-e SPARK_APP_CONFIG=/mondrian/config/usa2018.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		-e SPARK_APP_TEST=$(TEST) \
		spark-driver \
		/mondrian/script/submit.sh

poker: | start _poker stop
_poker: check_deps $(PACKAGE_ZIP) $(OUTPUT_DIR)
	@ echo -e "\n[*] Running mondrian on dataset/poker.csv.\n"
	- docker exec \
		-t \
		-e LOCAL_DATASET=/mondrian/dataset/poker.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/poker.csv \
		-e HDFS_ANONYMIZED_DATASET=hdfs://namenode:8020/anonymized/poker.csv \
		-e LOCAL_ANONYMIZED_DATASET=/mondrian/anonymized/poker.csv \
		-e SPARK_APP_CONFIG=/mondrian/config/poker.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		-e SPARK_APP_TEST=$(TEST) \
		spark-driver \
		/mondrian/script/submit.sh

usa1990: | start _usa1990 stop
_usa1990: check_deps $(PACKAGE_ZIP) $(OUTPUT_DIR)
	@ echo -e "\n[*] Running mondrian on dataset/usa1990.csv.\n"
	- docker exec \
		-t \
		-e LOCAL_DATASET=/mondrian/dataset/usa1990.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/usa1990.csv \
		-e HDFS_ANONYMIZED_DATASET=hdfs://namenode:8020/anonymized/usa1990.csv \
		-e LOCAL_ANONYMIZED_DATASET=/mondrian/anonymized/usa1990.csv \
		-e SPARK_APP_CONFIG=/mondrian/config/usa1990.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		-e SPARK_APP_TEST=$(TEST) \
		spark-driver \
		/mondrian/script/submit.sh

usa2019: | start _sub_k _usa2019 stop
_usa2019: check_deps $(PACKAGE_ZIP) $(OUTPUT_DIR)
	@ echo -e "\n[*] Running mondrian on dataset/usa2019.csv\n"
	- docker exec \
		-t \
		-e LOCAL_DATASET=/mondrian/dataset/usa2019.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/usa2019.csv \
		-e HDFS_ANONYMIZED_DATASET=hdfs://namenode:8020/anonymized/usa2019.csv \
		-e LOCAL_ANONYMIZED_DATASET=/mondrian/anonymized/usa2019.csv \
		-e SPARK_APP_CONFIG=/mondrian/config/usa2019.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		-e SPARK_APP_TEST=$(TEST) \
		spark-driver \
		/mondrian/script/submit.sh

transactions: | start _sub_k _transactions stop
_transactions: check_deps $(PACKAGE_ZIP) $(OUTPUT_DIR)
	@ echo -e "\n[*] Running mondrian on dataset/transactions.csv\n"
	- docker exec \
		-t \
		-e LOCAL_DATASET=/mondrian/dataset/transactions.csv \
		-e HDFS_DATASET=hdfs://namenode:8020/dataset/transactions.csv \
		-e HDFS_ANONYMIZED_DATASET=hdfs://namenode:8020/anonymized/transactions.csv \
		-e LOCAL_ANONYMIZED_DATASET=/mondrian/anonymized/transactions.csv \
		-e SPARK_APP_CONFIG=/mondrian/config/transactions.json \
		-e SPARK_APP_WORKERS=$(WORKERS) \
		-e SPARK_APP_DEMO=$(DEMO) \
		-e SPARK_APP_TEST=$(TEST) \
		-e WORKER_MEMORY=${WORKER_MEMORY} \
		-e DRIVER_MEMORY=${DRIVER_MEMORY} \
		spark-driver \
		/mondrian/script/submit.sh
