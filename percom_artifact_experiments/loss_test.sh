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


# Run this test from the Makefile in the root folder of the project

# Prepare workers array
tmp=("$2")
workers=()
for element in "${tmp}"
do
    workers+=($element)
done

# Spark-Mondrian test parameters
runs=$1
fractions=("$3")
fragmentation="quantile"
measures='"discernability_penalty", "global_certainty_penalty"'

# Clean test result files
make -C .. clean_test_files

echo "===================="
echo " CENTRALIZED VERSION"
echo "===================="


# Run of Centralized version
cp ./local_usa2018.json ../local/config/usa2018.json
sed -i -e "s/measuresX/${measures}/g" ../local/config/usa2018.json
sed -i -e "s/kParam/5/g" ../local/config/usa2018.json
sed -i -e "s/lParam/2/g" ../local/config/usa2018.json
make -C .. local-usa2018

echo "========================"
echo " END CENTRALIZED VERSION"
echo "========================"


# Run of Spark-Mondrian varying the number of workers
for r in $(seq 1 $runs);
do
	for w in ${workers[@]};
	do
	    for f in ${fractions[@]};
	    do

                echo "===================="
                echo " SPARK-BASED VERSION"
                echo " ${w} WORKERS"
                echo "===================="

	        cp ./template.json ./usa2018.json
		sed -i -e "s/fractionX/\"fraction\": ${f}/g" ./usa2018.json
		sed -i -e "s/fragmentationX/\"fragmentation\": \"${fragmentation}\"/g" ./usa2018.json
                sed -i -e "s/measuresX/${measures}/g" ./usa2018.json
		sed -i -e "s/kParam/5/g" ./usa2018.json
		sed -i -e "s/lParam/2/g" ./usa2018.json
		cp ./usa2018.json ../distributed/config/usa2018.json
		cd ..
		#start containers
		make start WORKERS=$w
		#wait to prevent datanode-in-safe-mode sporadic error
		sleep 20
		#launch test
		make _usa2018 WORKERS=$w TEST=1
		if [ $w -eq ${workers[-1]} ] && [ $r -eq $runs ]
		then
			docker exec -it spark-driver /mondrian/script/download.sh
		else
			#clean environment
			make stop
		fi
                echo "====================="
                echo " SPARK-BASED VERSION"
                echo " ${w} WORKERS RUN END"
                echo "====================="
		cd ./percom_artifact_experiments
	    done
	done
done

# Create test results table
python3 ./calculate_loss.py 

# Store test results
timestamp=$(date +%s)
folder_name=./results/loss_results_${timestamp}
mkdir ${folder_name}
cp ../distributed/test/test_results.csv ${folder_name}/spark_based_results.csv
cp ../local/test/test_centralized_results.csv ${folder_name}/centralized_results.csv
cp ./loss_measures.csv ${folder_name}/loss_table.csv 
cp ./loss.log ${folder_name}/loss.log

echo "================================="
echo " RUNTIME TEST FOLDER:"
echo " loss_results_${timestamp}"
echo "================================="

