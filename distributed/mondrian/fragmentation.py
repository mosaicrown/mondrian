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

from mondrian import partition_dataframe


def quantile_fragmentation(df, quasiid_columns, column_score, fragments,
                           colname):
    """Generate a number of fragments by cutting a column over quantiles."""
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
                           is_valid, fragments, colname):
    """Generate a number of fragments by cutting columns over median."""
    # generate fragments using mondrian
    partitions = partition_dataframe(df=df,
                                     quasiid_columns=quasiid_columns,
                                     sensitive_columns=sensitive_columns,
                                     column_score=column_score,
                                     is_valid=is_valid,
                                     partitions=fragments)
    # encode framentation info in the dataframe as a column
    df[colname] = -1
    for i, partition in enumerate(partitions):
        df.loc[partition, colname] = [i] * len(partition)
    return df


def create_fragments(df, quasiid_columns, column_score, fragments, colname,
                     criteria):
    """Encode sharding information in the dataset as a column."""
    return criteria(df=df,
                    quasiid_columns=quasiid_columns,
                    column_score=column_score,
                    fragments=fragments,
                    colname=colname)


def get_fragments_quantiles(df, quasiid_columns, column_score, fragments):
    """Compute quantiles on the best scoring quasi-identifier."""
    scores = [(column_score(df[column]), column) for column in quasiid_columns]
    print('scores: {}'.format(scores))
    for _, column in sorted(scores, reverse=True):
        try:
            quantiles = df[column].quantile(np.linspace(0, 1, fragments + 1))
            print("{} quantiles generated from '{}'.".format(
                fragments, column))
            return column, quantiles.values
        except Exception:
            # cannot generate enough quantiles from this column.
            print('cannot generate enough quantiles from {}.'.format(column))
    raise Exception("Can't generate {} quantiles.".format(fragments))
