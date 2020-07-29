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

from __future__ import print_function

import pandas as pd
import numpy as np
import scipy.stats

from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import argparse
import functools
import json
import time
import sys

import generalizations as gnlts

# Functions to process dataframes. #


def make_categorical(df):
    """Convert the string/object columns in a dataframe to categories."""
    categorical = set(df.select_dtypes(include=['object']).columns)
    for column in categorical:
        df[column] = df[column].astype('category')
    return df


def make_object(df, quasiid_columns, quasiid_gnlts=None):
    """Convert categorical, object and generalized qi columns in a dataframe
    to strings.

    :df: The df to be type-converted
    :quasiid_columns: QI column names
    :quasiid_gnlts: Dictionary of generalization info
    :returns: The dataframe with applied type conversions
    """
    categoricals = set(df.select_dtypes(include=['category']).columns)
    objects = set(df.select_dtypes(include=['object']).columns)
    qi_gnlts = set()
    str_types = (categoricals.union(objects)).union(qi_gnlts)
    for column in str_types:
        df[column] = df[column].astype(str)
    return df


# Functions to evaluate the cut-score of a column #


def entropy(ser):
    """Calculate the entropy of the passed `pd.Series`."""
    counts = ser.value_counts()
    return scipy.stats.entropy(counts)


def neg_entropy(ser):
    """Revert the entropy sign to invert the column ordering."""
    return -entropy(ser)


def span(ser):
    """Calculate the span of the passed `pd.Series`."""
    if ser.dtype.name in ('object', 'category'):
        return ser.nunique()
    else:
        return ser.max() - ser.min()


# Functions to evaluate if a partition is valid #


def is_k_anonymous(df, partition, sensitive_column, K):
    """Check if a partition is k-anonymous."""
    return len(partition) >= K


def is_l_diverse(df, partition, sensitive_column, L):
    """Check if a partition is l-diverse."""
    return df[sensitive_column][partition].nunique() >= L


def is_k_l_valid(df, partition, sensitive_column, K, L):
    """Check if a partition is both k-anonymous and l-diverse."""
    return is_k_anonymous(df, partition, sensitive_column, K) \
        and is_l_diverse(df, partition, sensitive_column, L)


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


# Functions to generate the anonymous dataset. #


def join_column(ser, column_name, quasiid_gnlts=None):
    """Make a clustered representation of the series in input.

    :ser: The Pandas series
    :column_name: The name of the column to be generalized
    :quasiid_gnlts: Dictionary of generalizations (info and params)
    """
    values = ser.unique()
    if len(values) == 1:
        return list(map(str, values))
    try:
        if not quasiid_gnlts:
            raise KeyError
        if quasiid_gnlts[column_name]['generalization_type'] == 'categorical':
            return gnlts.__generalize_to_lcc(
                values, quasiid_gnlts[column_name]['taxonomy_tree'])
        elif quasiid_gnlts[column_name]['generalization_type'] == 'numerical':
            return gnlts.__generalize_to_lcp(
                values, quasiid_gnlts[column_name]['taxonomy_tree'],
                quasiid_gnlts[column_name]['min'],
                quasiid_gnlts[column_name]['params']['fanout'])
        elif quasiid_gnlts[column_name][
                'generalization_type'] == 'common_prefix':
            return gnlts.__generalize_to_cp(
                values,
                hidemark=quasiid_gnlts[column_name]['params']['hide-mark'])
    except KeyError:
        if ser.dtype.name in ('object', 'category'):
            # ...set generalization
            return ['{' + ','.join(map(str, values)) + '}']
        else:
            # ...range generalization
            return ['[{}-{}]'.format(ser.min(), ser.max())]


def build_anonymized_dataset(df,
                             partitions,
                             quasiid_columns,
                             sensitive_column,
                             quasiid_gnlts=None):
    """Return a new dataframe by generalizing the partitions."""
    adf = make_object(df.copy(), quasiid_columns, quasiid_gnlts)

    for i, partition in enumerate(partitions):
        if i % 100 == 0:
            print("Finished {}/{} partitions...".format(i, len(partitions)))
        for column in quasiid_columns:
            adf.loc[partition,
                    column] = join_column(df[column][partition],
                                          column,
                                          quasiid_gnlts=quasiid_gnlts)

    return adf


def anonymize(df,
              quasiid_columns,
              sensitive_column,
              column_score,
              K,
              L,
              quasiid_gnlts=None):
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
                                    sensitive_column=sensitive_column,
                                    quasiid_gnlts=quasiid_gnlts)


# Functions to evaluate the information loss


def evaluate_information_loss(adf, udf):
    """Run the PandasUDF on fragments and aggregate the output."""
    penalties = adf.groupby('fragment').apply(udf)
    penalty = penalties.toPandas().sum()
    return penalty['information_loss']


def extract_span(aggregation, column_name=None, quasiid_gnlts=None):
    if column_name and quasiid_gnlts[column_name][
            'generalization_type'] == 'categorical':
        # count the leaves of the subtree originated by the categorical values
        subtree = quasiid_gnlts[column_name]['taxonomy_tree'].subtree(
            aggregation)
        leaves = len(subtree.leaves())
        return leaves if leaves > 1 else 0
    if column_name and quasiid_gnlts[column_name][
            'generalization_type'] == 'common_prefix':
        # if the string was generalized return 1 else 0
        hm = quasiid_gnlts[column_name]['params']['hide-mark']
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
        categories = aggregation[1:-1].split(',')
        return len(categories)
    return 0


def normalized_certainty_penalty(adf,
                                 quasiid_columns,
                                 quasiid_range,
                                 quasiid_gnlts=None):
    # compute dataset-level range on the quasi-identifiers columns
    partitions = adf.groupby(quasiid_columns)

    ncp = 0
    for _, partition in partitions:
        # work on a single row, each row has the same value of the
        # quasi-identifiers
        row = partition.iloc[0]
        rncp = 0
        for column_idx, column in enumerate(quasiid_columns):
            if quasiid_gnlts and column in quasiid_gnlts:
                rncp += extract_span(row[column], column,
                                     quasiid_gnlts) / quasiid_range[column_idx]
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
    for xs, f in x_segments:
        if xs.startswith('[') and xs.endswith(']'):
            low, high = map(float, xs[1:-1].split('-'))
            xvals.append(low)
            xvals.append(high)
        else:
            xvals.append(float(xs))
    yvals = []
    for ys, f in y_segments:
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


def __generalization_preproc(job, df, spark):
    """Anonymization preprocessing to arrange generalizations.

    :job: Dictionary job, contains information about generalization methods
    :df: Dataframe to be anonymized
    :spark: Spark instance
    :returns: Dictionary of taxonomies required to perform generalizations
    """
    quasiid_gnlts = dict()
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
                raise gnlts.IncompleteGeneralizationInfo()
            taxonomy = gnlts._read_categorical_taxonomy(t_db)
            g_dict['taxonomy_tree'] = taxonomy
        elif g_dict['generalization_type'] == 'numerical':
            try:
                fanout = g_dict['params']['fanout']
                accuracy = g_dict['params']['accuracy']
                digits = g_dict['params']['digits']
            except KeyError:
                raise gnlts.IncompleteGeneralizationInfo()
            if fanout is None or accuracy is None or digits is None:
                raise gnlts.IncompleteGeneralizationInfo()
            taxonomy, minv = gnlts.__taxonomize_numeric(
                spark=spark,
                df=df,
                col_label=g_dict['qi_name'],
                fanout=int(fanout),
                accuracy=float(accuracy),
                digits=int(digits))
            g_dict['taxonomy_tree'] = taxonomy
            g_dict['min'] = minv

        quasiid_gnlts[gen_item['qi_name']] = g_dict

    # return the generalization dictionary
    return quasiid_gnlts


def main():
    # Parse arguments from command line
    parser = argparse.ArgumentParser(
        description='Anonymize a dataset using Mondrian in Spark.')
    parser.add_argument('METADATA', help='json file that describes the job.')
    parser.add_argument('WORKERS',
                        default=4,
                        type=int,
                        help='Number of initial cuts (workers)')
    parser.add_argument('DEMO',
                        default=0,
                        type=int,
                        help='Start tool in demo mode')

    args = parser.parse_args()
    demo = args.DEMO

    start_time = time.time()

    with open(args.METADATA) as fp:
        job = json.load(fp)

    # Create Spark Session
    spark = SparkSession \
        .builder \
        .appName('mondrian') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Enable Arrow-based columnar data transfers
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')

    # Share generalization library
    spark.sparkContext.addPyFile("/mondrian/generalizations.py")

    if demo == 1:
        print("\n[*] Spark context initialized")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    # Parameters
    filename_in = job['input']
    filename_out = job['output']
    quasiid_columns = job['quasiid_columns']
    sensitive_column = job['sensitive_column']
    if job['column_score'] == 'entropy':
        column_score = entropy
    elif job['column_score'] == 'neg_entropy':
        column_score = neg_entropy
    else:
        column_score = span
    fragments = min(args.WORKERS, job.get('max_fragments', 10**6))
    K = job['K']
    L = job['L']
    measures = job['measures']

    # Setup mondrian_fragmentation function
    is_valid = functools.partial(is_k_l_valid, K=K, L=L)
    mondrian = functools.partial(mondrian_fragmentation,
                                 sensitive_column=sensitive_column,
                                 is_valid=is_valid)

    fragmentation = quantile_fragmentation \
        if job['fragmentation'] == 'quantile' else mondrian

    fraction = job['fraction'] if 0 < job['fraction'] < 1 else None

    if fraction and fragmentation == mondrian:
        sys.exit('''Sorry, currently mondrian fregmentation criteria is only
         available without sampling.''')

    if demo == 1:
        print("\n[*] Job details initialized")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    print('\n[*] Using {} initial partitions\n'.format(fragments))

    # Read file
    print('[*] Reading from {}\n'.format(filename_in))
    df = spark.read \
        .options(header='true', inferSchema='true').csv(filename_in)
    if fraction:
        df = df.sample(fraction=fraction, seed=0)
    pdf = df.toPandas()
    pdf.info()

    print('\n[*] Fragmentation details\n')

    if not fraction:
        # Create first cut
        pdf = create_fragments(df=pdf,
                               quasiid_columns=quasiid_columns,
                               column_score=column_score,
                               fragments=fragments,
                               colname='fragment',
                               criteria=fragmentation)

        # Check first cut
        sizes = pdf.groupby('fragment').size()
        print("\n[*] Dataset distribution among fragments\n")
        print(sizes)

        print("\n[*] Dataset with fragmentation info\n")
        print(pdf.head)

        # Compute the range on the quasi-identifiers columns
        # will be useful for information loss evaluation
        quasiid_range = [-1] * len(quasiid_columns)
        for i, column in enumerate(quasiid_columns):
            quasiid_range[i] = span(pdf[column])

        # Recreate the dataframe in a way that is appreciated by pyarrow.
        pdf = pd.DataFrame.from_dict(pdf.to_dict())

        # Create spark dataframe
        df = spark.createDataFrame(pdf)
    else:
        # Compute quantiles on the sample
        column, bins = get_fragments_quantiles(df=pdf,
                                               quasiid_columns=quasiid_columns,
                                               column_score=column_score,
                                               fragments=fragments)

        # Read entire file in distributed manner
        df = spark.read \
            .options(header='true', inferSchema='true').csv(filename_in)
        bins[0] = float(
            "-inf")  # to prevent out of Bucketizer bounds exception
        bins[-1] = float(
            "inf")  # to prevent out of Bucketizer bounds exception
        bucketizer = Bucketizer(splits=bins,
                                inputCol=column,
                                outputCol='fragment')
        df = bucketizer.transform(df)

        # Check first cut
        sizes = df.groupBy('fragment').count()
        print("\n[*] Dataset distribution among fragments\n")
        sizes.show()

        print("\n[*] Dataset with fragmentation info\n")
        df.show()

        # Compute the range on the quasi-identifiers columns
        # will be useful for information loss evaluation
        categoricals = [
            item[0] for item in df.dtypes
            if item[0] in quasiid_columns and item[1].startswith('string')
        ]
        funcs = (F.countDistinct(F.col(cname)) if cname in categoricals else
                 F.max(F.col(cname)) - F.min(F.col(cname))
                 for cname in quasiid_columns)
        quasiid_range = df.agg(*funcs).collect()[0]

    # Create a schema in which the quasi identifiers are strings.
    # This is needed because the result of the UDF has to generalize them.
    schema = T.StructType(df.schema)
    for column in quasiid_columns:
        schema[column].dataType = T.StringType()

    if demo == 1 and fragments > 1:
        print("\n[*] Dataset fragmented")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    # initialize taxonomies
    quasiid_gnlts = None
    quasiid_gnlts = __generalization_preproc(job, df, spark=spark)

    if demo == 1 and quasiid_gnlts:
        print("\n[*] Taxonomies data preprocessed")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    # Create the pandas udf
    @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
    def anonymize_udf(pdf):
        adf = anonymize(df=pdf,
                        quasiid_columns=quasiid_columns,
                        sensitive_column=sensitive_column,
                        column_score=column_score,
                        K=K,
                        L=L,
                        quasiid_gnlts=quasiid_gnlts)

        # Ensure that the quasi identifier columns have been converted
        # to strings (they are required by the return type).
        for column in quasiid_columns:
            adf[column] = adf[column].astype('object')

        return adf

    print('\n[*] Starting anonymizing the dataframe\n')

    adf = df \
        .groupby('fragment') \
        .apply(anonymize_udf) \
        .cache()

    # Create Discernability Penalty udf
    schema = T.StructType(
        [T.StructField('information_loss', T.LongType(), nullable=False)])

    @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
    def discernability_penalty_udf(adf):
        dp = discernability_penalty(adf=adf, quasiid_columns=quasiid_columns)
        # pandas_udf requires a pandas dataframe as output
        return pd.DataFrame({'information_loss': [dp]})

    # Create Normalized Certainty Penalty udf
    schema = T.StructType(
        [T.StructField('information_loss', T.DoubleType(), nullable=False)])

    @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
    def normalized_certainty_penalty_udf(adf):
        gcp = normalized_certainty_penalty(adf=adf,
                                           quasiid_columns=quasiid_columns,
                                           quasiid_range=quasiid_range,
                                           quasiid_gnlts=quasiid_gnlts)
        # pandas_udf requires a pandas dataframe as output
        return pd.DataFrame({'information_loss': [gcp]})

    adf.drop('fragment').show(10)

    print('\n[*] Anonymized dataframe')

    if demo == 1:
        print("\tWait for 10 seconds to continue demo...\n")
        time.sleep(10)

    if measures:
        print('[*] Information loss evaluation\n')
    for measure in measures:
        if measure == 'discernability_penalty':
            dp = evaluate_information_loss(adf, discernability_penalty_udf)
            print(f"Discernability Penalty = {dp:.2E}")
        elif measure == 'normalized_certainty_penalty':
            ncp = evaluate_information_loss(adf,
                                            normalized_certainty_penalty_udf)
            print(f"Normalized Certainty Penalty = {ncp:.2E}")
        elif measure == 'global_certainty_penalty':
            gcp = evaluate_information_loss(adf,
                                            normalized_certainty_penalty_udf)
            gcp /= (len(quasiid_columns) * adf.count())
            print(f"Global Certainty Penalty = {gcp:.4f}")

    print(f"\n[*] Writing to {filename_out}\n")
    df.write \
        .mode("overwrite") \
        .options(header=True) \
        .csv(filename_out)

    print('\n[*] Done\n')
    spark.stop()

    if demo == 0:
        print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main()
