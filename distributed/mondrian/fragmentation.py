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

import math

import pandas as pd
from pyspark.ml.feature import Bucketizer
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mondrian import partition_dataframe
from utils import repartition_dataframe


# Mondrian-based fragmentations

def mondrian_without_parallelization(df, quasiid_columns, sensitive_columns,
                                     column_score, is_valid, fragments, colname,
                                     k=None, is_sampled=False, scale=False,
                                     flat=False):
    """Generate a number of fragments by cutting columns over median."""
    partitions, medians = partition_dataframe(
        df=df,
        quasiid_columns=quasiid_columns,
        sensitive_columns=sensitive_columns,
        column_score=column_score,
        is_valid=is_valid,
        partitions=fragments,
        is_sampled=is_sampled,
        k=k,
        flat=flat
    )

    if is_sampled and 'bucket' not in df.columns:
        # return partition and partitioning information if sampled run
        return partitions, medians
    else:
        # encode fragmentation info in the dataframe as a column
        if not scale:
            df[colname] = -1
        else:
            prefix = df[colname][0]

        for i, partition in enumerate(partitions):
            if scale:
                df.loc[partition, colname] = [(prefix << 1) + i] * len(partition)
                if is_sampled:
                    df.loc[partition, 'bucket'] = [str([medians[i]])] * len(partition)
            else:
                df.loc[partition, colname] = [i] * len(partition)

        # Force fragmentation info to int32
        df = df.astype({colname: 'int32'}, copy=False)

        return df


def mondrian_with_parallelization(df, quasiid_columns, sensitive_columns,
                                  column_score, is_valid, fragments, colname,
                                  repartition_strategy, is_sampled=False):
    """Distribute Mondrian starting from the initial cuts."""
    # Initailize fragmentation column
    df = df.withColumn(colname, F.lit(0))
    if is_sampled:
        df = df.withColumn('bucket', F.lit("[[[],[],[]]]"))

    @F.pandas_udf(df.schema, F.PandasUDFType.GROUPED_MAP)
    def single_cut(df):
        if current_filter is None or df[colname][0] in current_filter:
            df = mondrian_without_parallelization(
                df, quasiid_columns, sensitive_columns, column_score,
                is_valid, 2, colname, is_sampled=is_sampled, scale=True
            )
        return df

    total_steps = math.ceil(math.log(fragments, 2))
    current_filter = None
    df.rdd.context.broadcast(current_filter)

    print('\n[*] Cut 0 of', total_steps)
    print('Number of pre-processing partitions:', df.rdd.getNumPartitions())
    print('Partition ids:', df.select(colname).distinct().collect())

    for step in range(1, total_steps + 1):
        # Update filter to match the number of required fragements
        if step == total_steps:
            last_step_cuts = int(fragments - math.pow(2, total_steps - 1))
            current_filter = {i for i in range(last_step_cuts)}
            df.rdd.context.broadcast(current_filter)

        # Distribute the execution of a single step of Mondrian
        df = df \
            .groupby(colname) \
            .applyInPandas(single_cut.func, schema=single_cut.returnType).cache()

        # Repartition dataframe for following work
        if repartition_strategy == 'repartitionByRange':
            df = df.repartitionByRange(colname)
        elif repartition_strategy == 'customRepartition':
            df = repartition_dataframe(df, min(2**step, fragments)) # ???

        print(f'\n[*] Cut {step} of {total_steps}')
        print('Number of pre-processing partitions:', df.rdd.getNumPartitions())
        print('Partition ids:', df.select(colname).distinct().collect())

    bins = []
    if is_sampled:
        for log in df.select("bucket").distinct().toPandas()['bucket']:
            log = eval(log)
            bins.append(log[0])

    return df, bins


def mondrian_buckets(df, bins):
    """ Emulates Spark Bucketizer when using Mondrian in sampled runs. """
    df = df.withColumn('fragment', F.lit(0))
    bucket_index = 0
    comparison = ("<", ">=")
    for columns, values, signs in bins:
        lines = []
        for column, value, sign in zip(columns, values, signs):
            if sign in comparison:
                lines.append(f"{column} {sign} {value}")
            else:
                set_line = "', '".join(value)
                lines.append(f"`{column}` in ('{set_line}')")
        expression = " and ".join(lines)
        df = df.withColumn("fragment", F.expr(f"case when {expression} then {bucket_index} else fragment end"))
        bucket_index += 1
    return df


# Quantile-based fragmentations

def quantile_fragmentation(df, quasiid_columns, column_score, fragments,
                           colname):
    """Generate a number of fragments by cutting a column over quantiles."""
    scores = [(column_score(df[column]), column) for column in quasiid_columns]
    print(f'scores: {scores}')
    for _, column in sorted(scores, reverse=True):
        try:
            quantiles, bins = pd.qcut(df[column], fragments,
                                      labels=range(fragments), retbins=True)
            # Avoid having duplicate bins
            if len(set(bins)) != len(bins):
                continue

            print(f"{fragments} quantiles generated from '{column}'.")
            # Update colname with integer quantile assignments
            df[colname] = quantiles.astype('int32')
            return df, column, bins
        except Exception:
            # cannot generate enough quantiles from this column.
            print(f'cannot generate enough quantiles from {column}.')
    raise Exception(f"Can't generate {fragments} quantiles.")


def quantile_buckets(df, column, bins):
    """ Use Spark Bucketizer to encode fragmentation information. """
    bins[0] = float("-inf")  # avoid out of Bucketizer bounds exception
    bins[-1] = float("inf")  # avoid out of Bucketizer bounds exception

    # Assign every row to bucket 0 when there is only one bucket
    if len(bins) == 2:
        df = df.withColumn('fragment', F.lit(0))
        return df

    # Use Bucketizer to encode fragmentation info
    bucketizer = Bucketizer(splits=bins,
                            inputCol=column,
                            outputCol='fragment')
    df = bucketizer.transform(df)

    # Force fragmentation info to integer
    df = df.withColumn('fragment',
                        F.col('fragment').cast(T.IntegerType()))

    return df
