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

import generalization as gnrlz
from mondrian import partition_dataframe
from validation import get_validation_function


# Functions to generate the anonymous dataset.

def join_column(ser, dtype, generalization=None):
    """Make a clustered representation of the series in input.

    :ser: The Pandas series
    :column_name: The name of the column to be generalized
    :generalization: Dictionary of generalizations (info and params)
    """
    values = ser.unique()
    if len(values) == 1:
        return str(values[0])
    try:
        if not generalization:
            raise KeyError
        if generalization['generalization_type'] == 'categorical':
            return gnrlz.__generalize_to_lcc(
                values, generalization['taxonomy_tree'])
        elif generalization['generalization_type'] == 'numerical':
            return gnrlz.__generalize_to_lcp(
                values, generalization['taxonomy_tree'],
                generalization['min'],
                generalization['params']['fanout'])
        elif generalization[
                'generalization_type'] == 'common_prefix':
            return gnrlz.__generalize_to_cp(
                values,
                hidemark=generalization['params']['hide-mark'])
    except KeyError:
        if dtype.name in ('object', 'category'):
            # ...set generalization
            return '{' + ','.join(map(str, values)) + '}'
        else:
            # ...range generalization
            return '[{}-{}]'.format(ser.min(), ser.max())


def generalize_quasiid(df,
                       partitions,
                       quasiid_columns,
                       quasiid_gnrlz=None):
    """Return a new dataframe by generalizing the partitions."""
    dtypes = df.dtypes

    for i, partition in enumerate(partitions):
        if i % 100 == 0:
            print("Finished {}/{} partitions...".format(i, len(partitions)))

        for column in quasiid_columns:
            generalization = quasiid_gnrlz[column] \
                             if quasiid_gnrlz and column in quasiid_gnrlz \
                             else None
            df.loc[partition, column] = join_column(df[column][partition],
                                                    dtypes[column],
                                                    generalization)
    return df


def remove_id(df, id_columns, redact=False):
    """Remove identifiers columns.

    :df: The Pandas DataFrame
    :id_columns: The list of columns to remove
    :redact: If False drops the given columns. Otherwise it redacts them.
        Defaults to False.
    """
    if not redact:
        df.drop(columns=id_columns, inplace=True)
    else:
        df.loc[:, id_columns] = "REDACTED"

    return df


def anonymize(df,
              id_columns,
              quasiid_columns,
              sensitive_columns,
              column_score,
              K,
              L,
              quasiid_gnrlz=None,
              redact=False):
    """Perform the clustering using K-anonymity and L-diversity and using
    the Mondrian algorithm. Then generalizes the quasi-identifier columns.
    """
    df = remove_id(df, id_columns, redact)
    partitions = partition_dataframe(df=df,
                                     quasiid_columns=quasiid_columns,
                                     sensitive_columns=sensitive_columns,
                                     column_score=column_score,
                                     is_valid=get_validation_function(K,L))
    return generalize_quasiid(df=df,
                              partitions=partitions,
                              quasiid_columns=quasiid_columns,
                              quasiid_gnrlz=quasiid_gnrlz)
