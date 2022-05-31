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

from pyspark.sql import types as T
from pyspark.sql import functions as F
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

def prepare_parallelization_udf_schema(redact, df, id_columns):

    if not redact:
        schema = T.StructType(
            df.select(
                [column for column in df.columns if column not in id_columns]
            ).schema
        )
    else:
        schema = T.StructType(df.schema)

    return schema

def remap_fragments_to_float(df, spark, schema):
    """ Remap from binary string to float the fragment column """

    fragments_created = df.select('fragment').distinct().toPandas()  # set of fragments
    fragments_mapping = {}  # conversion dictionary
    new_index = 0.0

    for frag in fragments_created['fragment']:
        fragments_mapping[frag] = new_index
        new_index += 1.0

    spark.sparkContext.broadcast(fragments_mapping)
    schema['fragment'].dataType = T.FloatType()

    @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
    def remap_fragments(df):
        df['fragment'] = df['fragment'].apply(lambda x: fragments_mapping[x])
        return df

    df = df \
        .groupby('fragment') \
        .applyInPandas(remap_fragments.func, schema=remap_fragments.returnType).cache()

    return df