#!/bin/bash
# Copyright 2022 Unibg Seclab (https://seclab.unibg.it)
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

if [ -f ../distributed/dataset/transactions.csv ]
then
    echo "No need to download the transactions dataset, you already have it."
    exit 0
fi

[[ ! -z ${KAGGLE_CONFIG_DIR+z} ]] && FILE="${KAGGLE_CONFIG_DIR}/kaggle.json" || FILE="${HOME}/.kaggle/kaggle.json"

if [ -f "$FILE" ]; then
    kaggle competitions download --force -c acquire-valued-shoppers-challenge
    unzip acquire-valued-shoppers-challenge.zip
    rm acquire-valued-shoppers-challenge.zip
    rm offers.csv.gz  
    rm sampleSubmission.csv.gz  
    rm testHistory.csv.gz  
    rm trainHistory.csv.gz  
    gunzip transactions.csv.gz
    venv/bin/python build_transactions.py
    rm transactions.csv
else
    echo "Please create the $FILE credential file"
fi
