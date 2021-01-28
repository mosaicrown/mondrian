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

import csv
import os
from collections import namedtuple

dir_path = os.path.dirname(os.path.relpath(__file__))
data = [] # Multiple entries
local_time = 0.0 # Single entry
# Recover Spark-Mondrian data
with open(os.path.join(dir_path, "../distributed/test/test_results.csv")) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    first = True
    for row in csv_reader:
        if first:
            Row = namedtuple("Row", row)
            first = False
        else:
            row_tuple = Row(*row)
            data.append(f"{row_tuple.fragments}\t{row_tuple.time}")

# Sort data by fragments
data.sort(key=lambda x : int((x.split("\t"))[0]))
# Recover Centralized version data
with open(os.path.join(dir_path, "../local/test/test_centralized_results.csv")) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    first = True
    for row in csv_reader:
        if first:
            Row = namedtuple("Row", row)
            first = False
        else:
            row_tuple = Row(*row)
            local_time = row_tuple.time


with open(os.path.join(dir_path, "./plot.dat"), "w") as m_file:
    with open(os.path.join(dir_path, "./plot2.dat"), "w") as l_file:
        for item in data:
            m_file.write(f"{item}\n")
            tokens = item.split("\t")
            l_file.write(f"{tokens[0]}\t{local_time}\n")
