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

import csv
import os
from collections import namedtuple

def result_handler(measures):
    dir_path = os.path.dirname(os.path.relpath(__file__))
    with open(os.path.join(dir_path, "../test/test_centralized_results.csv"), "w") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(['timestamp', 'K', 'L', 'DP', 'NCP', 'GCP', 'time'])
        csv_writer.writerow(_recover_measures(measures))

def _recover_measures(measures):
    keys = ['timestamp', 'K', 'L', 'DP', 'NCP', 'GCP', 'time']
    result = []
    for key in keys:
        result.append(measures[key] if key in measures else "")
    return result
