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

FROM bitnami/spark:3.0.1

USER root

# [app] install hadoop client
RUN curl -o hadoop.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
    tar -xzvf hadoop.tar.gz && mv hadoop-3.2.1 ../hadoop && rm hadoop.tar.gz

# [master, worker] upgrade python from 3.6.13 to 3.8.12
RUN . /opt/bitnami/scripts/libcomponent.sh && component_unpack "python" "3.8.12-12" --checksum c081f7f9663d4d64e7f26d6e6a77b3860ee1f39c309bd564f801c39192042d2f

# [master, worker] install application requirements
COPY requirements.txt /tmp/
RUN ["pip", "install", "--upgrade", "pip"]
RUN ["pip", "install", "--requirement", "/tmp/requirements.txt"]

# [history server] overwrite default directory containing application event logs
RUN echo "spark.history.fs.logDirectory hdfs://namenode:8020/spark-events" >> $SPARK_HOME/conf/spark-defaults.conf
