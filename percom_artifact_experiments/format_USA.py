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
data = []

# Recover Spark-Mondrian data
with open(os.path.join(dir_path, "../distributed/dataset/usa2018.csv")) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    first = True
    counter=0
    for row in csv_reader:
        if first:
            Row = namedtuple("Row", row)
            first = False
        else:
            data.append(Row(*row))
            counter+=1
            if counter==500000:
                break

with open(os.path.join(dir_path, "../distributed/dataset/usa2018.csv"), "w") as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow(["STATEFIP", "AGE", "EDUCD", "OCC", "INCTOT"])
    for item in data:
        csv_writer.writerow([item.STATEFIP, item.AGE, item.EDUCD, item.OCC, "< 40K" if int(item.INCTOT) < 40000 else ">= 40K"])
