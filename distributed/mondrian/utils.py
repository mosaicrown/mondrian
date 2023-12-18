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

from pyspark.sql import functions as F


def get_extension(filename):
    _, sep, extension = filename.rpartition(".")
    if not sep:
        extension = None
    return extension


def get_quasiid_spans(df, quasiid_columns):
    categoricals = [
        column for column, dtype in df.dtypes
        if column in quasiid_columns and dtype == 'string'
    ]
    funcs = (F.countDistinct(F.col(cname)) if cname in categoricals else
                    F.max(F.col(cname)) - F.min(F.col(cname))
                    for cname in quasiid_columns)
    quasiid_spans = df.agg(*funcs).collect()[0]
    quasiid_spans_by_name = dict(zip(quasiid_columns,
                                        quasiid_spans))
    return quasiid_spans, quasiid_spans_by_name


def repartition_dataframe(df, num_partitions):
    # NOTE: Before the number of partitions was fixed to df.rdd.getNumPartitions()
    return df.rdd.map(lambda r : (r['fragment'], r)) \
        .partitionBy(num_partitions, lambda fragment: fragment) \
        .map(lambda r : r[1]) \
        .toDF()
