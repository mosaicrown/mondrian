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

if __package__:
    from .score import span
else:
    from score import span


# Functions to evaluate the information loss

def extract_span(aggregation, column_name=None, quasiid_gnrlz=None):
    if column_name and quasiid_gnrlz[column_name][
            'generalization_type'] == 'categorical':
        # count the leaves of the subtree originated by the categorical values
        subtree = quasiid_gnrlz[column_name]['taxonomy_tree'].subtree(
            aggregation)
        return len(subtree.leaves())
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
        categories = aggregation[1:-1].split(',')
        return len(categories)
    return 0


def normalized_certainty_penalty(df, adf, quasiid_columns, qi_range, quasiid_gnrlz=None):
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
                                     quasiid_gnrlz) / qi_range[column_idx]
            else:
                rncp += extract_span(row[column]) / qi_range[column_idx]
        rncp *= len(partition)
        ncp += rncp
    return ncp


def global_certainty_penalty(df, adf, quasiid_columns, qi_range, quasiid_gnrlz):
    ncp = normalized_certainty_penalty(df, adf, quasiid_columns, qi_range, quasiid_gnrlz)
    return ncp / (len(quasiid_columns) * len(adf))


def discernability_penalty(adf, quasiid_columns):
    sizes = adf.groupby(quasiid_columns).size()

    dp = 0
    for size in sizes:
        dp += size**2
    return dp
