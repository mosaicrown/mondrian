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

if [ -f ../distributed/dataset/usa2019.csv ] && [ -f ../local/dataset/usa2019.csv ]
then
    echo "No need to download the usa2019 dataset, you already have it."
    exit 0
fi

echo -e "Downloading usa2019 dataset.\n"
wget "https://www2.census.gov/programs-surveys/acs/data/pums/2019/1-Year/csv_pus.zip"
unzip csv_pus.zip -d usa2019-raw
venv/bin/python build_usa2019.py
rm -rf csv_pus.zip usa2019-raw
