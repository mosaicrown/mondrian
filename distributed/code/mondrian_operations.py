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

import generalizations as gnrlz

# Function that define Mondrian. #
def cut_column(ser):
    """Determine two sets of indices identifing the values to be stored in left
    and right partitions after the cut.

    :ser: Pandas series
    """
    if ser.dtype.name in ('object', 'category'):
        frequencies = ser.value_counts()
        pos = len(ser) // 2
        median_idx = lc = 0
        for count in frequencies:
            median_idx += 1
            lc += count
            if lc >= pos:
                # move the median to the less represented side
                rc = len(ser) - lc
                if lc - count > rc:
                    median_idx -= 1
                break
        values = frequencies.index
        lv = set(values[:median_idx])
        rv = set(values[median_idx:])
        dfl = ser.index[ser.isin(lv)]
        dfr = ser.index[ser.isin(rv)]
    else:
        median = ser.median()
        dfl = ser.index[ser < median]
        dfr = ser.index[ser >= median]
    return (dfl, dfr)


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


def mondrian_fragmentation(df, quasiid_columns, sensitive_column, column_score,
                           is_valid, fragments, colname):
    """Generate a number of fragments by cutting columns over median."""
    # generate fragments using mondrian
    partitions = partition_dataframe(df=df,
                                     quasiid_columns=quasiid_columns,
                                     sensitive_column=sensitive_column,
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


def partition_dataframe(df,
                        quasiid_columns,
                        sensitive_column,
                        column_score,
                        is_valid,
                        partitions=float("inf")):
    """Iterate over the partitions and perform the best cut
    (according to the column score) up until cuts are possible."""
    num_partitions = partitions
    finished_partitions = []
    # puts a range index obj (start, end, step) into a list
    partitions = [df.index]

    while partitions and len(partitions) < num_partitions:
        print('Partitions: {}, '.format(len(partitions)))
        partition = partitions.pop(0)
        scores = [(column_score(df[column][partition]), column)
                  for column in quasiid_columns]

        for score, column in sorted(scores, reverse=True):
            lp, rp = cut_column(df[column][partition])
            if not is_valid(df, lp, sensitive_column) or \
                    not is_valid(df, rp, sensitive_column):
                continue
            print('cutting over column: {} (score: {})'.format(column, score))
            partitions.append(lp)
            partitions.append(rp)
            break

        else:
            # no valid cut found
            finished_partitions.append(partition)
            print('No valid cut for this partition. Keeping it intact.')
    return finished_partitions if num_partitions == float(
        "inf") else partitions

def __generalization_preproc(job, df, spark):
    """Anonymization preprocessing to arrange generalizations.

    :job: Dictionary job, contains information about generalization methods
    :df: Dataframe to be anonymized
    :spark: Spark instance
    :returns: Dictionary of taxonomies required to perform generalizations
    """
    quasiid_gnrlz = dict()
    if not job['quasiid_generalizations']:
        return None

    for gen_item in job['quasiid_generalizations']:

        g_dict = dict()
        g_dict['qi_name'] = gen_item['qi_name']
        g_dict['generalization_type'] = gen_item['generalization_type']
        g_dict['params'] = gen_item['params']

        if g_dict['generalization_type'] == 'categorical':
            # read taxonomy from file
            t_db = g_dict['params']['taxonomy_tree']
            if t_db is None:
                raise gnrlz.IncompleteGeneralizationInfo()
            taxonomy = gnrlz._read_categorical_taxonomy(t_db)
            g_dict['taxonomy_tree'] = taxonomy
        elif g_dict['generalization_type'] == 'numerical':
            try:
                fanout = g_dict['params']['fanout']
                accuracy = g_dict['params']['accuracy']
                digits = g_dict['params']['digits']
            except KeyError:
                raise gnrlz.IncompleteGeneralizationInfo()
            if fanout is None or accuracy is None or digits is None:
                raise gnrlz.IncompleteGeneralizationInfo()
            taxonomy, minv = gnrlz.__taxonomize_numeric(
                spark=spark,
                df=df,
                col_label=g_dict['qi_name'],
                fanout=int(fanout),
                accuracy=float(accuracy),
                digits=int(digits))
            g_dict['taxonomy_tree'] = taxonomy
            g_dict['min'] = minv

        quasiid_gnrlz[gen_item['qi_name']] = g_dict

    # return the generalization dictionary
    return quasiid_gnrlz

