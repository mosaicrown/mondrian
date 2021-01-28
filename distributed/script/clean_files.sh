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
echo "[*] Deleting test result file from previous runs"
hadoop fs -rm -R -f hdfs://namenode:8020/anonymized/test_results.csv
hadoop fs -rm -R -f hdfs://namenode:8020/anonymized/artifact_result.csv
