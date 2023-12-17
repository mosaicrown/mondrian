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


# Functions to evaluate the information loss

def evaluate_information_loss(adf, udf):
    """Run the PandasUDF on fragments and aggregate the output."""
    penalties = adf.groupby('fragment').applyInPandas(udf.func, udf.returnType)
    penalty = penalties.toPandas().sum()
    return penalty['information_loss']


def extract_span(aggregation, column_name=None, quasiid_gnrlz=None):
    if column_name and quasiid_gnrlz[column_name][
            'generalization_type'] == 'categorical':
        # count the leaves of the subtree originated by the categorical values
        subtree = quasiid_gnrlz[column_name]['taxonomy_tree'].subtree(
            aggregation)
        leaves = len(subtree.leaves())
        return leaves if leaves > 1 else 0
    if column_name and quasiid_gnrlz[column_name][
            'generalization_type'] == 'common_prefix':
        mark = quasiid_gnrlz[column_name]['params']['hide_mark']
        domain = int(quasiid_gnrlz[column_name]['params']['char_domain_size'])
        count = aggregation.count(mark)
        return domain**count if count else 0
    if aggregation.startswith('[') and (aggregation.endswith(']')
                                        or aggregation.endswith(')')):
        # TODO: Add support for lexicographic generalization
        low, high = map(float, aggregation[1:-1].split('-'))
        return high - low
    if aggregation.startswith('{') and aggregation.endswith('}'):
        return aggregation[1:-1].count(',') + 1
    return 0


def normalized_certainty_penalty(adf,
                                 quasiid_columns,
                                 quasiid_range,
                                 quasiid_gnrlz=None):
    # compute dataset-level range on the quasi-identifiers columns
    partitions = adf.groupby(quasiid_columns)

    ncp = 0
    for _, partition in partitions:
        # work on a single row, each row has the same value of the
        # quasi-identifiers
        row = partition.iloc[0]
        rncp = 0
        for column_idx, column in enumerate(quasiid_columns):
            if quasiid_gnrlz and column in quasiid_gnrlz:
                rncp += extract_span(row[column], column,
                                     quasiid_gnrlz) / quasiid_range[column_idx]
            else:
                rncp += extract_span(row[column]) / quasiid_range[column_idx]
        rncp *= len(partition)
        ncp += rncp

    return ncp


def discernability_penalty(adf, quasiid_columns):
    """Compute Discernability Penalty (DP)."""
    sizes = adf.groupby(quasiid_columns).size()

    dp = 0
    for size in sizes:
        dp += size**2
    return dp
