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


# Functions that define Mondrian

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


def partition_dataframe(df,
                        quasiid_columns,
                        sensitive_columns,
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
            if not is_valid(df, lp, sensitive_columns) or \
                    not is_valid(df, rp, sensitive_columns):
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
