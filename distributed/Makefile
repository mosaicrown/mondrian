.PHONY: all addlicense clean start stop up down run _run shell _shell notebook _notebook usa2018 _usa2018 poker _poker

SHELL              	:= /bin/bash
REQUIRED_BINS      	:= docker docker-compose

PACKAGE				:= mondrian
PACKAGE_ZIP			:= $(PACKAGE).zip
OUTPUT_DIR	   		:= anonymized

WORKERS            	:= 4
DEMO		   		:= 0
TEST				:= 0
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
	@- rm -f $(PACKAGE_ZIP)

.spark.build: $(shell find spark -type f)
	docker-compose build spark-master spark-worker
	@ touch $@

start up: check_deps .spark.build
	@ echo -e "\n[*] Starting Apache Spark cluster on docker with $(WORKERS) workers.\n"
	docker-compose up -d --scale spark-worker=$(WORKERS) spark-master spark-worker namenode datanode spark-driver
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
	- docker exec \
		-it \
		-e PYSPARK_DRIVER_PYTHON=ipython \
		spark-driver \
		pyspark

shell: | start _shell stop

_notebook: check_deps
	@ echo -e "\n[*] Running jupyter notebook.\n"
	- docker exec \
		-it \
		-e PYSPARK_DRIVER_PYTHON="jupyter" \
    	-e PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root" \
		spark-driver \
		pyspark

notebook: | start _notebook stop

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

usa2018: | start _usa2018 stop

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
