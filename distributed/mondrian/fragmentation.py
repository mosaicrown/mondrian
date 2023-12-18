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

import pandas as pd
import numpy as np
from pyspark.sql import functions as F

from mondrian import partition_dataframe


def quantile_fragmentation(df, quasiid_columns, column_score, fragments,
                           colname, is_sampled=False, k=None):
    """Generate a number of fragments by cutting a column over quantiles."""
    # is_sampled parameter is used to avoid runtime errors
    scores = [(column_score(df[column]), column) for column in quasiid_columns]
    print('scores: {}'.format(scores))
    for _, column in sorted(scores, reverse=True):
        try:
            quantiles = pd.qcut(df[column], fragments, labels=range(fragments))
            print("{} quantiles generated from '{}'.".format(
                fragments, column))
            df[colname] = quantiles
            return df
        except Exception:
            # cannot generate enough quantiles from this column.
            print('cannot generate enough quantiles from {}.'.format(column))
    raise Exception("Can't generate {} quantiles.".format(fragments))


def mondrian_fragmentation(df, quasiid_columns, sensitive_columns, column_score,
                           is_valid, fragments, colname, k, is_sampled=False, scale=False, flat=False):
    """Generate a number of fragments by cutting columns over median."""
    # generate fragments using mondrian
    if is_sampled:
        partitions, medians = partition_dataframe(df=df,
                                     quasiid_columns=quasiid_columns,
                                     sensitive_columns=sensitive_columns,
                                     column_score=column_score,
                                     is_valid=is_valid,
                                     partitions=fragments,
                                     k=k,
                                     is_sampled=is_sampled)
    else:
        partitions = partition_dataframe(df=df,
                                     quasiid_columns=quasiid_columns,
                                     sensitive_columns=sensitive_columns,
                                     column_score=column_score,
                                     is_valid=is_valid,
                                     partitions=fragments,
                                     k=k,
                                     is_sampled=is_sampled,
                                     flat=flat)

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


def create_fragments(df, quasiid_columns, column_score, fragments, colname,
                     criteria, k,is_sampled=False):
    """Encode sharding information in the dataset as a column."""
    return criteria(df=df,
                    quasiid_columns=quasiid_columns,
                    column_score=column_score,
                    fragments=fragments,
                    colname=colname,
                    is_sampled=is_sampled,
                    k=k)

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

def get_fragments_quantiles(df, quasiid_columns, column_score, fragments):
    """Compute quantiles on the best scoring quasi-identifier."""
    scores = [(column_score(df[column]), column) for column in quasiid_columns]
    print('scores: {}'.format(scores))
    for _, column in sorted(scores, reverse=True):
        try:
            quantiles = df[column].quantile(np.linspace(0, 1, fragments + 1))
            # avoid having duplicate quantiles
            if len(set(quantiles)) != len(quantiles):
                continue

            print("{} quantiles generated from '{}'.".format(
                fragments, column))
            return column, quantiles.values
        except Exception:
            # cannot generate enough quantiles from this column.
            print('cannot generate enough quantiles from {}.'.format(column))
    raise Exception("Can't generate {} quantiles.".format(fragments))
