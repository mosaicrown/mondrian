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

import functools

if __package__:
    from . import generalization as gnrlz
    from .mondrian import partition_dataframe
    from .utils import make_object
    from .validation import is_k_l_valid
else:
    import generalization as gnrlz
    from mondrian import partition_dataframe
    from utils import make_object
    from validation import is_k_l_valid


# Functions to generate the anonymous dataset

def join_column(ser, column_name, quasiid_gnrlz=None):
    """Make a clustered representation of the series in input.

    :ser: The Pandas series
    :column_name: The name of the column to be generalized
    :quasiid_gnrlz: Dictionary of generalizations (info and params)
    """
    values = ser.unique()
    if len(values) == 1:
        return str(values[0])
    try:
        if not quasiid_gnrlz:
            raise KeyError
        if quasiid_gnrlz[column_name]['generalization_type'] == 'categorical':
            return gnrlz.__generalize_to_lcc(
                values, quasiid_gnrlz[column_name]['taxonomy_tree'])
        elif quasiid_gnrlz[column_name]['generalization_type'] == 'numerical':
            return gnrlz.__generalize_to_lcp(
                values, quasiid_gnrlz[column_name]['taxonomy_tree'],
                quasiid_gnrlz[column_name]['min'],
                quasiid_gnrlz[column_name]['params']['fanout'])
        elif quasiid_gnrlz[column_name][
                'generalization_type'] == 'common_prefix':
            return gnrlz.__generalize_to_cp(
                values,
                hidemark=quasiid_gnrlz[column_name]['params']['hide-mark'])
    except KeyError:
        if ser.dtype.name in ('object', 'category'):
            # ...set generalization
            return '{' + ','.join(map(str, values)) + '}'
        else:
            # ...range generalization
            return '[{}-{}]'.format(ser.min(), ser.max())


def build_anonymized_dataset(df,
                             partitions,
                             quasiid_columns,
                             quasiid_gnrlz=None):
    """Return a new dataframe by generalizing the partitions."""
    adf = make_object(df.copy(), quasiid_columns, quasiid_gnrlz)

    for i, partition in enumerate(partitions):
        if i % 100 == 0:
            print("Finished {}/{} partitions...".format(i, len(partitions)))

        for column in quasiid_columns:
            generalization = join_column(df[column][partition],
                                          column,
                                          quasiid_gnrlz=quasiid_gnrlz)
            adf.loc[partition, column] = [generalization] * len(partition)

    return adf


def anonymize(df,
              quasiid_columns,
              sensitive_column,
              column_score,
              K,
              L,
              quasiid_gnrlz=None):
    """Perform the clustering using K-anonymity and L-diversity and using
    the Mondrian algorithm. Then generalizes the quasi-identifier columns.
    """
    partitions = partition_dataframe(df=df,
                                     quasiid_columns=quasiid_columns,
                                     sensitive_column=sensitive_column,
                                     column_score=column_score,
                                     is_valid=functools.partial(is_k_l_valid,
                                                                K=K,
                                                                L=L))

    return build_anonymized_dataset(df=df,
                                    partitions=partitions,
                                    quasiid_columns=quasiid_columns,
                                    quasiid_gnrlz=quasiid_gnrlz)
