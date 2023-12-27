# Copyright 2023 Unibg Seclab (https://seclab.unibg.it)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import statistics
import csv
import os
from collections import namedtuple

dir_path = os.path.dirname(os.path.relpath(__file__))
data = {} # Multiple entries
Entry = namedtuple("Entry", "DP GCP")
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
            if row_tuple.fragments not in data:
                data[row_tuple.fragments] = []
            data[row_tuple.fragments].append(Entry(row_tuple.DP, row_tuple.GCP))

with open(os.path.join(dir_path, "../local/test/test_centralized_results.csv")) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    first = True
    for row in csv_reader:
        if first:
            Row = namedtuple("Row", row)
            first = False
        else:
           local_row_tuple = Row(*row)

final_columns = {}
for workers, measures in data.items():
    mean_DP = statistics.mean([float(x.DP) for x in measures])
    mean_GCP = statistics.mean([float(x.GCP) for x in measures])
    std_DP = statistics.stdev([float(x.DP) for x in measures], mean_DP)
    std_GCP = statistics.stdev([float(x.GCP) for x in measures], mean_GCP)
    final_columns[workers] = [f"{mean_DP} +- {std_DP}", f"{mean_GCP} +- {std_GCP}"]

with open(os.path.join(dir_path, "./loss_measures.csv"), "w") as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow(["", "100% - Centralized", "0.01% sampling - 5 Workers", "0.01% sampling - 10 Workers", "0.01% sampling - 20 Workers"])
    csv_writer.writerow(["DP", local_row_tuple.DP, final_columns["5"][0],final_columns["10"][0],final_columns["20"][0]])
    csv_writer.writerow(["GCP", local_row_tuple.GCP, final_columns["5"][1],final_columns["10"][1],final_columns["20"][1]])



