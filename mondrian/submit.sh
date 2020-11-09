#!/bin/bash
# Copyright 2020 Unibg Seclab (https://seclab.unibg.it)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PATH="/opt/bitnami/hadoop/bin:${PATH}"

echo "[*] Exit Hadoop safe mode"
hdfs dfsadmin -D 'fs.defaultFS=hdfs://namenode:8020' -safemode forceExit

echo "[*] Load dataset to the Hadoop Distributed File System"
hadoop fs -mkdir -p hdfs://namenode:8020/dataset
hadoop fs -put -f ${LOCAL_DATASET} ${HDFS_DATASET}

echo "[*] Submit Spark Job"
spark-submit \
    --master ${SPARK_MASTER_URL} \
    --conf spark.default.parallelism=${SPARK_APP_WORKERS} \
    --conf spark.sql.shuffle.partitions=${SPARK_APP_WORKERS} \
    --files ${SPARK_FILES} \
    ${SPARK_APP} \
    ${SPARK_APP_CONFIG} ${SPARK_APP_WORKERS} ${SPARK_APP_DEMO}
