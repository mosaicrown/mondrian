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


def customPartitioner(el):
	return el


def repartition_dataframe(df, spark):
    df = spark.createDataFrame(df.rdd.map(lambda r : (r['fragment'], r))\
    .partitionBy(df.rdd.getNumPartitions(), customPartitioner)\
    .map(lambda r : r[1]))\
    .toDF(*df.columns)
    return df
