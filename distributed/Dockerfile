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

FROM maven as builder

# Download requirements to interact with object storage
RUN cat > /tmp/pom.xml <<EOF
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>eu.glaciation-project.mondrian</groupId>
  <artifactId>object-storage-and-monitoring-deps</artifactId>
  <version>1</version>
  <dependencies>
    <!-- https://mvnrepository.com/artifact/io.prometheus.jmx/jmx_prometheus_javaagent -->
    <dependency>
        <groupId>io.prometheus.jmx</groupId>
        <artifactId>jmx_prometheus_javaagent</artifactId>
        <version>1.0.1</version>
    </dependency>
    <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws -->
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-aws</artifactId>
        <version>3.3.4</version>
    </dependency>
    <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-hadoop-cloud -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-hadoop-cloud_2.13</artifactId>
        <version>3.4.0</version>
    </dependency>
  </dependencies>
</project>
EOF
RUN mvn dependency:copy-dependencies -f /tmp/pom.xml -DoutputDirectory=/tmp/jars/

FROM apache/spark-py:v3.4.0

USER root

# Install application object storage and monitoring requirements
COPY --from=builder /tmp/jars/ /opt/spark/jars/

# Install application requirements
COPY requirements.txt /tmp/
RUN ["pip", "install", "--upgrade", "pip"]
RUN ["pip", "install", "--requirement", "/tmp/requirements.txt"]

# Install application
RUN mkdir "/mondrian"
COPY anonymize.py /mondrian/
COPY mondrian.zip /mondrian/
