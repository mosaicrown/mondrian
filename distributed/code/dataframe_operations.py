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

# Functions to process dataframes. #

def customPartitioner(el):
	return el

def repartition_dataframe(df, spark):
    df = spark.createDataFrame(df.rdd.map(lambda r : (r['fragment'], r))\
    .partitionBy(df.rdd.getNumPartitions(), customPartitioner)\
    .map(lambda r : r[1]))\
    .toDF(*df.columns)
    return df

def make_categorical(df):
    """Convert the string/object columns in a dataframe to categories."""
    categorical = set(df.select_dtypes(include=['object']).columns)
    for column in categorical:
        df[column] = df[column].astype('category')
    return df


def make_object(df, quasiid_columns, quasiid_gnrlz=None):
    """Convert categorical, object and generalized qi columns in a dataframe
    to strings.

    :df: The df to be type-converted
    :quasiid_columns: QI column names
    :quasiid_gnrlz: Dictionary of generalization info
    :returns: The dataframe with applied type conversions
    """
    categoricals = set(df.select_dtypes(include=['category']).columns)
    objects = set(df.select_dtypes(include=['object']).columns)
    qi_gnrlz = set()
    str_types = (categoricals.union(objects)).union(qi_gnrlz)
    for column in str_types:
        df[column] = df[column].astype(str)
    return df
