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


def get_extension(filename):
    _, sep, extension = filename.rpartition(".")
    if not sep:
        extension = None
    return extension


def repartition_dataframe(df, num_partitions):
    # NOTE: Before the number of partitions was fixed to df.rdd.getNumPartitions()
    return df.rdd.map(lambda r : (r['fragment'], r)) \
        .partitionBy(num_partitions, lambda fragment: fragment) \
        .map(lambda r : r[1]) \
        .toDF()
