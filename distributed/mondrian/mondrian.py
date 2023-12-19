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

import binpacking
import math


# K-Flat partitioning

def bin_packing(ser):
        freq = ser.value_counts()
        to_pack = {value:value_count for value, value_count in zip(freq.index, freq)}
        packed = binpacking.to_constant_bin_number(to_pack, 2)
        dfl, dfr =  (ser.index[ser.isin(packed[0].keys())], ser.index[ser.isin(packed[1].keys())])
        return (dfl, dfr)


def k_flat(ser,k):
    
    """Determine two sets of indices identifing the values to be stored in left
    and right partitions after the cut.
    
    :ser: Pandas series containing the column to cut
    :k: K used in the anonymization process
    :is_sampled: configuration flag to identify a sampled run
    :not_divisible: flag to identify the workers that contains a partition not divisible by K
    :r: number of partitions with size K-1 that will be produced by the ser in input
    """


    median = None
    is_category = False
    if ser.dtype.name in ('object', 'category'):
        is_category = True
        dfl, dfr = bin_packing(ser)
    else:
        median = ser.median()
        if median == ser.min():
                dfl = ser.index[ser <= median]
                dfr = ser.index[ser > median]
        else:
                dfl = ser.index[ser < median]
                dfr = ser.index[ser >= median]
     
    rlen, llen = (len(dfr), len(dfl))
    k_tot = math.floor(len(ser.index) / k) 
    h_tot = len(ser.index) % k
    k_left = math.floor(llen / k)
    k_right = math.floor(rlen / k)
    h_left = llen % k
    h_right = rlen % k
    to_move = 0
    if rlen == 0 or llen == 0:
       return (dfl, dfr)
    # All ok => do not move
    if (h_left <= k_left) and (h_right <= k_right) and (h_left + h_right == h_tot):
        return (dfl, dfr)
    # Validate left
    if k_left < 1:
        to_move = -(k - h_left + max(0,h_tot - (k_tot - 1)))

    # Validate right
    if k_right < 1 and to_move == 0:
        to_move = k - h_right + max(0,h_tot - (k_tot - 1))

    if to_move == 0:
            # Adjust partitions of size K + 1
            if h_left <= h_tot:
                # Adjust from left
                if h_left > k_left:
                        move_from_left = h_left - k_left
                        if k_right == 1:
                                to_move = move_from_left
                        else:
                                move_from_right = k - h_left + max([0, h_tot - (k_right - 1)])
                                to_move = move_from_left if move_from_left < move_from_right else - move_from_right

                # Adjust from right
                else:
                        move_from_right = h_right - k_right
                        if k_left <= 1:
                                to_move = - move_from_right
                        else:
                                move_from_left = k - h_right + max([0, h_tot - (k_left - 1)])
                                to_move = move_from_left if move_from_left < move_from_right else - move_from_right
            else:
                move_from_left = h_left - min([k_left, h_tot])
                move_from_right = h_right - min([k_right, h_tot])

                to_move = move_from_left if move_from_left < move_from_right else -move_from_right

    left_to_right = True

    if to_move >0:
        selected = ser[dfl]
        val_to_move = selected.unique()
        if is_category:
                freq = selected.value_counts()
                val_to_move = sorted(val_to_move, key=lambda x: freq[x],reverse=False)
        else:
                val_to_move = sorted(val_to_move, reverse=True)
    # Right to left    
    else:
        left_to_right = False
        to_move = -to_move
        selected = ser[dfr]
        val_to_move = selected.unique()
        if is_category:
                freq = selected.value_counts()
                val_to_move = sorted(val_to_move, key=lambda x: freq[x],reverse=False)
        else:
                val_to_move = sorted(val_to_move, reverse=False)

    while to_move > 0:
           median =  val_to_move.pop(0)
           rows_to_move = ser.index[ser == median]
           rows_to_move =  rows_to_move if len(rows_to_move) <= to_move else rows_to_move[0:to_move]
           moved = len(dfr)

           # Remove from right, add to left
           if not left_to_right:
              dfr = dfr.difference(rows_to_move)
              dfl = dfl.union(rows_to_move)
           # Remove from left, add to right
           else:
              dfl = dfl.difference(rows_to_move)
              dfr = dfr.union(rows_to_move)
           # For categorical sometimes the median is the other partition
           moved = abs(moved - len(dfr))
           to_move -= min(moved, to_move)

    return (dfl, dfr)


# Mondrian partitioning strategy

def cut_column(ser, is_sampled=False, categoricals_with_order={}):
    """Determine two sets of indices identifing the values to be stored in left
    and right partitions after the cut.

    :ser: Pandas series
    """
    if ser.dtype.name in ('object', 'category') and ser.name not in categoricals_with_order:
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
        if is_sampled:
            return (dfl, dfr, (lv, rv))
    else:
        if ser.name in categoricals_with_order:
            ser = ser.map(categoricals_with_order[ser.name])
        median = ser.median()
        dfl = ser.index[ser < median]
        dfr = ser.index[ser >= median]
    return (dfl, dfr) if not is_sampled else (dfl, dfr, median)


def partition_dataframe(df,
                        quasiid_columns,
                        sensitive_columns,
                        column_score,
                        is_valid,
                        k=None,
                        partitions=float("inf"),
                        is_sampled = False,
                        flat = False,
                        categoricals_with_order={}):
    """Iterate over the partitions and perform the best cut
    (according to the column score) up until cuts are possible."""
    num_partitions = partitions
    finished_partitions = []
    medians = eval(df['bucket'].loc[0]) if 'bucket' in df.columns else [[[],[],[]]]
    # puts a range index obj (start, end, step) into a list
    partitions = [df]
    to_add_again = []
    medians_to_add_again = []
    first_pop = True
    while partitions and len(partitions) < num_partitions:
        print('Partitions: {}, '.format(len(partitions)))
        partition = partitions.pop(0)
        if is_sampled:
            med = medians.pop(0)
        scores = [(column_score(df[column][partition.index]), column)
                  for column in quasiid_columns]

        for score, column in sorted(scores, reverse=True):
            if is_sampled:
                lp, rp, median = cut_column(df[column][partition.index], is_sampled, categoricals_with_order)
            else:
                if not flat:
                    lp, rp = cut_column(df[column][partition.index], is_sampled, categoricals_with_order)
                else:
                    lp, rp = k_flat(df[column][partition.index], k)
            if not is_valid(df, lp, sensitive_columns) or \
                    not is_valid(df, rp, sensitive_columns):
                if flat:
                        # Never change cut column 
                        if not (((len(lp) == k or len(lp) == k+1) and len(rp) == 0) or ((len(rp) == k or len(rp) == k+1) and len(lp)==0)):
                                if any(map(lambda x: x[0] != 1, scores)):
                                        raise NameError(f"{column}, {len(lp)}, {len(rp)}, {scores}")
                continue
            print('cutting over column: {} (score: {})'.format(column, score))
            if is_sampled:
                # Cache cut information
                med_lp = med[:]
                med_rp = [x[:] for x in med_lp]

                med_lp[0].append(column)
                med_lp[1].append(median if type(median) is not tuple else median[0])
                med_lp[2].append("<" if type(median) is not tuple else "in")

                med_rp[0].append(column)
                med_rp[1].append(median if type(median) is not tuple else median[1])
                med_rp[2].append(">=" if type(median) is not tuple else "in")
            
                medians.append(med_lp)
                medians.append(med_rp)

            partitions.append(df.iloc[lp])
            partitions.append(df.iloc[rp])
            if not first_pop:
                del partition
            else:
                first_pop = False
            break

        else:
            # no valid cut found
            finished_partitions.append(partition.index)
            if num_partitions != float("inf"):
                num_partitions -= 1.0
                to_add_again.append(partition.index)
            if is_sampled:
                medians_to_add_again.append(med)
            print('No valid cut for this partition. Keeping it intact.')

    partitions = finished_partitions if num_partitions == float("inf") else [p.index for p in partitions] + to_add_again
    return partitions, medians + medians_to_add_again
