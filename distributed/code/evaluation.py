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

import generalizations as gnrlz

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
        # if the string was generalized return 1 else 0
        hm = quasiid_gnrlz[column_name]['params']['hide-mark']
        if hm in aggregation:
            return int(aggregation[aggregation.index("[") + 1:-1])
        else:
            return 0
        return 1 if hm in aggregation else 0
    if aggregation.startswith('[') and (aggregation.endswith(']')
                                        or aggregation.endswith(')')):
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


def visualizer(df, columns):
    """Util to draw kd-tree like 2d regions (max 2 QI columns).

    :df: The dataframe
    :columns: The two QI column names
    N.B. Use of this function in Spark is not supported
    """
    print("\n[*] Printing 2d rectangles info\n")
    # xy list of coordinates ( (x, y) tuples )
    x_segments = []
    px = df[columns[0]][0]

    c = 1
    al = len(df[columns[0]][1:])
    acc = 0
    for x in df[columns[0]][1:]:
        if x != px:
            x_segments.append((px, c))
            px = x
            c = 1
        else:
            c += 1
        acc += 1
        if acc == al:
            x_segments.append((x, c))

    print("x_segments: {}".format(x_segments))

    y_segments = []
    py = df[columns[1]][0]
    c = 1
    acc = 0
    for y in df[columns[1]][1:]:
        if y != py:
            y_segments.append((py, c))
            py = y
            c = 1
        else:
            c += 1
        acc += 1
        if acc == al:
            y_segments.append((y, c))

    print("y_segments: {}".format(y_segments))

    xptr = 0
    yptr = 0
    x_f = x_segments[xptr][1]
    y_f = y_segments[yptr][1]
    rectangles = []
    while True:
        if x_f == y_f:
            rectangles.append((x_segments[xptr][0], y_segments[yptr][0]))
        elif x_f > y_f:
            counter = y_f
            rectangles.append((x_segments[xptr][0], y_segments[yptr][0]))
            while x_f > counter:
                counter += y_segments[yptr][1]
                yptr += 1
                rectangles.append((x_segments[xptr][0], y_segments[yptr][0]))
        else:
            counter = x_f
            rectangles.append((x_segments[xptr][0], y_segments[yptr][0]))
            while y_f > counter:
                counter += x_segments[xptr][1]
                xptr += 1
                rectangles.append((x_segments[xptr][0], y_segments[yptr][0]))
        if (xptr == len(x_segments) - 1) and (xptr == len(x_segments) - 1):
            break
        else:
            xptr += 1
            yptr += 1

    print("rectangle intervals: {}".format(rectangles))

    xvals = []
    for xs, _ in x_segments:
        if xs.startswith('[') and xs.endswith(']'):
            low, high = map(float, xs[1:-1].split('-'))
            xvals.append(low)
            xvals.append(high)
        else:
            xvals.append(float(xs))
    yvals = []
    for ys, _ in y_segments:
        if ys.startswith('[') and ys.endswith(']'):
            low, high = map(float, ys[1:-1].split('-'))
            yvals.append(low)
            yvals.append(high)
        else:
            yvals.append(float(ys))

    rectangle_coordinates = []
    for r in rectangles:
        xs = r[0]
        xlow = 0
        xhigh = 0
        if xs.startswith('[') and xs.endswith(']'):
            xlow, xhigh = map(float, xs[1:-1].split('-'))
        else:
            xlow = xhigh = float(xs)
        ys = r[1]
        ylow = 0
        yhigh = 0
        if ys.startswith('[') and ys.endswith(']'):
            ylow, yhigh = map(float, ys[1:-1].split('-'))
        else:
            ylow = yhigh = float(ys)
        rectangle_coordinates.append(((xlow, ylow), (xhigh, yhigh)))

    print("rectangle_coordinates: {}".format(rectangle_coordinates))