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

archives=($( ls ../ | grep \.*.csv.gz))
if [ ${#archives[@]} -eq 1 ]
	then
		gunzip -d -k -f ../${archives[0]}
		fileName=$(echo "${archives[0]}" | cut -d '.' -f 1)
		fileExt=$(echo "${archives[0]}" | cut -d '.' -f 2)
		file="${fileName}.${fileExt}"
		cp ../$file ../distributed/dataset/usa2018.csv
		python3 format_USA.py
		cp ../distributed/dataset/usa2018.csv ../local/dataset/usa2018.csv
	else
		echo "[ ERROR ] Multiple or no archive detected: keep one archive in the root directory"
		exit 1 
	fi
