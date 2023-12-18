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

import argparse
import functools
import json
import sys
import time

import math
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from anonymization import anonymize
from evaluation import discernability_penalty
from evaluation import evaluate_information_loss
from evaluation import normalized_certainty_penalty
from fragmentation import mondrian_buckets
from fragmentation import mondrian_without_parallelization
from fragmentation import mondrian_with_parallelization
from fragmentation import quantile_buckets
from fragmentation import quantile_fragmentation
from generalization import generalization_preproc
from score import entropy, neg_entropy, span, norm_span
from utils import get_extension, repartition_dataframe
from test import write_test_params
from validation import get_validation_function


SCORE_FUNCTIONS = {
    'span': span,
    'entropy': entropy,
    'neg_entropy': neg_entropy,
    'norm_span' : 'norm_span'
}
REPARTITIONS = ['customRepartition', 'noRepartition', 'repartitionByRange']
FRAGMENTATIONS = ['mondrian', 'quantile']


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
    parser.add_argument('TEST',
                        default=0,
                        type=int,
                        help='Start tool in test mode')
  
    args = parser.parse_args()
    demo = args.DEMO
    test = args.TEST

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
    spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')

    if demo == 1:
        print("\n[*] Spark context initialized")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    # Parameters of the anonymization
    filename_in = job['input']
    filename_out = job['output']
    id_columns = job.get('id_columns', [])
    redact = job.get('redact', False)
    quasiid_columns = job['quasiid_columns']
    sensitive_columns = job.get('sensitive_columns', [])
    K = job.get('K')
    L = job.get('L')
    flat = job.get("k_flat", False)
    use_categorical = job.get('use_categorical', [])
    measures = job.get('measures', [])
    try:
        column_score = SCORE_FUNCTIONS[job.get('column_score', 'span')]
    except KeyError:
        raise ValueError(f"Column score must be one of "
                         f"{', '.join(SCORE_FUNCTIONS)}")

    # Parameters guiding the distribution of the job on the cluster
    fraction = job.get('fraction')
    if fraction is not None and (fraction <= 0 or fraction > 1):
        raise ValueError("Fraction value must be in (0:1]")
    to_sample = (fraction is not None and fraction != 1)
    fragments = min(args.WORKERS, job.get('max_fragments', 10**6))
    is_parallel = job.get('parallel', False)
    repartition = job.get('repartition', 'repartitionByRange')
    if repartition not in REPARTITIONS:
        raise ValueError(f"Repartition must be one of "
                         f"{', '.join(REPARTITIONS)}")
    fragmentation = job.get('fragmentation', 'quantile')
    if fragmentation not in FRAGMENTATIONS:
        raise ValueError(f"Fragmentation must be one of "
                         f"{', '.join(FRAGMENTATIONS)}")

    if not K and not L:
        raise ValueError("Both K and L parameters not given or equal to zero.")
    if L and not sensitive_columns:
        raise ValueError(
            "l-diversity needs to know which columns are sensitive."
        )

    if demo == 1:
        print("\n[*] Job details initialized")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    print('\n[*] Using {} initial partitions\n'.format(fragments))

    # Read file according to extension
    print('[*] Reading from {}\n'.format(filename_in))
    extension = get_extension(filename_in)
    df = spark.read \
        .options(header='true', inferSchema='true') \
        .format(extension).load(filename_in)

    if to_sample:
        df = df.sample(fraction=fraction)

    for attribute in use_categorical:
        df = df.withColumn(attribute, F.col(attribute).cast(T.StringType()))
    
    # Initialize taxonomies
    df, quasiid_gnrlz = generalization_preproc(df, job)
    categoricals_with_order = {}
    if quasiid_gnrlz is not None:
        for qi in quasiid_gnrlz.values():
            if 'taxonomy_ordering' in qi and qi['taxonomy_ordering'] is not None:
                categoricals_with_order[qi['qi_name']] = qi['taxonomy_ordering']
 
    pdf = df.toPandas()

    total_spans = None
    if column_score == "norm_span" :
        total_spans = {}
        for qi in quasiid_columns:
            ser = pdf[qi]
            if ser.dtype.name in ('object', 'category') and ser.name not in categoricals_with_order:
                total_spans[ser.name] = (ser.nunique(), 'unordered')
            elif ser.name in categoricals_with_order:
                total_spans[ser.name] = (len(categoricals_with_order[ser.name]) - 1, 'numerical')
            else:
                total_spans[ser.name] = (ser.max() - ser.min(), 'numerical')

        column_score = functools.partial(norm_span, total_spans=total_spans, categoricals_with_order=categoricals_with_order)

    pdf.info()

    print('\n[*] Fragmentation details')
    preposition = "with" if to_sample else "without"
    if fragmentation == 'mondrian':
        # Mondrian
        if is_parallel:
            print(f"\n[*] Run {preposition} sampling and parallelization"
                    " - Mondrian cuts")
            df, bins = mondrian_with_parallelization(
                df=df,
                quasiid_columns=quasiid_columns,
                sensitive_columns=sensitive_columns,
                column_score=column_score,
                is_valid=get_validation_function(K, L),
                fragments=fragments,
                colname="fragment",
                repartition_strategy=repartition,
                is_sampled=to_sample
            )
        else:
            print(f"\n[*] Run {preposition} sampling - Mondrian cuts\n")
            ret = mondrian_without_parallelization(
                df=pdf,
                quasiid_columns=quasiid_columns,
                sensitive_columns=sensitive_columns,
                column_score=column_score,
                is_valid=get_validation_function(K,L),
                fragments=fragments,
                colname='fragment',
                is_sampled=to_sample,
                k=K,
                flat=flat
            )
            # Unwrap return value
            if to_sample:
                pdf, bins = ret
            else:
                pdf = ret
                # Distribute dataframe
                df = spark.createDataFrame(pdf)

        if to_sample:
            # Read entire dataframe
            df = spark.read \
                .options(header='true', inferSchema='true') \
                .format(extension).load(filename_in)
            # Partition dataframe according to the bins
            df = mondrian_buckets(df, bins)
    elif fragmentation == 'quantile':
        # Quantile
        print(f"\n[*] Run {preposition} sampling - Quantile cuts\n")
        pdf, column, bins = quantile_fragmentation(
            df=pdf,
            quasiid_columns=quasiid_columns,
            column_score=column_score,
            fragments=fragments,
            colname='fragment'
        )
        if to_sample:
            # Read entire file in distributed manner
            df = spark.read \
                .options(header='true', inferSchema='true') \
                .format(extension).load(filename_in)
            df = quantile_buckets(df, column, bins)
        else:
            # Recreate the dataframe in a way that is appreciated by pyarrow.
            pdf = pd.DataFrame.from_dict(pdf.to_dict())
            # Distribute dataframe
            df = spark.createDataFrame(pdf)

    # Check result of the initial fragmentation
    sizes = df.groupBy('fragment').count()
    print("\n[*] Dataset distribution among fragments\n")
    sizes.show()

    GID_dict = {}
    if flat:
            all_GID = range(0, math.floor(df.count() / K))
            current_index = 0
            for size in sizes.collect():
                part_num = math.floor(size["count"] / K)
                GID_dict[size["fragment"]] =  all_GID[current_index:current_index + part_num]
                current_index += part_num

    print("[*] Dataset with fragmentation info\n")
    df.show()

    # Create a schema in which identifiers are either not there or strings
    # and quasi identifiers are strings.
    # This is needed because the result of the UDF has to generalize them.
    if not redact:
        schema = T.StructType(
            df.select(
                [column for column in df.columns if column not in id_columns]
            ).schema
        )
    else:
        schema = T.StructType(df.schema)
        for column in id_columns:
            schema[column].dataType = T.StringType()
    for column in quasiid_columns:
        schema[column].dataType = T.StringType()

    if demo == 1 and fragments > 1:
        print("\n[*] Dataset fragmented")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    if demo == 1 and quasiid_gnrlz:
        print("\n[*] Taxonomies data preprocessed")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)
    
    if total_spans is not None:
        for qi, span_info in total_spans.items():
            if span_info[1] == 'unordered' and qi not in categoricals_with_order:
                total_spans[qi] = (df.select(F.countDistinct(qi)).collect()[0][0], span_info[1])
            elif qi in categoricals_with_order:
                total_spans[qi] = (len(categoricals_with_order[qi]) - 1, 'numerical')
            else:
                total_spans[qi] = (df.agg({qi: 'max'}).collect()[0][0] - df.agg({qi: 'min'}).collect()[0][0], span_info[1])

        column_score = functools.partial(norm_span, total_spans=total_spans, categoricals_with_order=categoricals_with_order)

    # Create the pandas udf
    @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
    def anonymize_udf(pdf):
        adf = anonymize(df=pdf,
                        id_columns=id_columns,
                        redact=redact,
                        quasiid_columns=quasiid_columns,
                        sensitive_columns=sensitive_columns,
                        column_score=column_score,
                        K=K,
                        L=L,
                        quasiid_gnrlz=quasiid_gnrlz,
                        GID=GID_dict,
                        categoricals_with_order=categoricals_with_order)

        # Ensure that the quasi identifier columns have been converted
        # to strings (they are required by the return type).
        for column in quasiid_columns:
            adf[column] = adf[column].astype('object')

        return adf

    # Repartition dataframe according to the fragmentation info
    if repartition == 'repartitionByRange':
        df = df.repartitionByRange('fragment')
    elif repartition == 'customRepartition':
        df = repartition_dataframe(df, fragments)

    print('[*] Starting anonymizing the dataframe\n')
    print(f'[*] Number of DF partitions: {df.rdd.getNumPartitions()}')

    ''' Debug spark partitioning -> Low performance
    count = 0
    for elem in df.rdd.glom().collect():
       print("Size of Spark Partition {}: {}".format(count, len(elem)))
       count +=1
    '''
    adf = df \
        .groupby('fragment') \
        .applyInPandas(anonymize_udf.func, schema=anonymize_udf.returnType) \
        .cache()

    print(f'[*] Number of ADF partitions: {adf.rdd.getNumPartitions()}')

    print('\n[*] Anonymized dataframe\n')
    adf.drop('fragment').show(10)
    if demo == 1:
        print("\tWait for 10 seconds to continue demo...\n")
        time.sleep(10)

    # dictionary to store test params
    measures_log = {}
    measures_log["fragments"] = fragments
    measures_log["repartition"] = repartition
    measures_log["K"] = K
    measures_log["L"] = L
    measures_log["fraction"] = fraction

    # TODO: add a column to the output schema to keep information on the
    #       equivalent classes to avoid reconstructing them from scratch
    #       in the evaluation of the metrics
    if measures:
        print('[*] Information loss evaluation\n')

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

        # Compute the range on the quasi-identifiers columns for the evaluation of
        # the normalized certainty penalty
        categoricals = [
            column for column, dtype in df.dtypes
            if column in quasiid_columns and dtype == 'string'
        ]
        funcs = (F.countDistinct(F.col(cname)) if cname in categoricals else
                    F.max(F.col(cname)) - F.min(F.col(cname))
                    for cname in quasiid_columns)
        quasiid_range = df.agg(*funcs).collect()[0]

        @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
        def normalized_certainty_penalty_udf(adf):
            gcp = normalized_certainty_penalty(adf=adf,
                                            quasiid_columns=quasiid_columns,
                                            quasiid_range=quasiid_range,
                                            quasiid_gnrlz=quasiid_gnrlz)
            # pandas_udf requires a pandas dataframe as output
            return pd.DataFrame({'information_loss': [gcp]})

    for measure in measures:
        if measure == 'discernability_penalty':
            dp = evaluate_information_loss(adf, discernability_penalty_udf)
            print(f"Discernability Penalty = {dp:.2E}")
            measures_log["DP"] = dp
        elif measure == 'normalized_certainty_penalty':
            ncp = evaluate_information_loss(adf,
                                            normalized_certainty_penalty_udf)
            print(f"Normalized Certainty Penalty = {ncp:.2E}")
            measures_log["NCP"] = ncp
        elif measure == 'global_certainty_penalty':
            gcp = evaluate_information_loss(adf,
                                            normalized_certainty_penalty_udf)
            gcp /= (len(quasiid_columns) * adf.count())
            print(f"Global Certainty Penalty = {gcp:.4f}")
            measures_log["GCP"] = gcp

    # Handle fragmentation information
    if not flat:
        adf = adf.drop('fragment')
    else:
        adf = adf.withColumnRenamed('fragment', 'GID')

    # Write file according to extension
    print(f"\n[*] Writing to {filename_out}\n")
    extension = get_extension(filename_out)
    adf.write \
        .mode("overwrite") \
        .options(header=True) \
        .format(extension) \
        .save(filename_out)

    end_time = time.time()
    execution_time = end_time - start_time
    measures_log["timestamp"] = end_time
    measures_log["time"] = execution_time

    if test == 1:
        # Write test params to Hadoop
        test_result_files = [
            "hdfs://namenode:8020/anonymized/test_results.csv",
            "hdfs://namenode:8020/anonymized/artifact_result.csv"
        ]
        print("[*] Creating test configuration file on Hadoop")
        write_test_params(spark, measures_log, test_result_files)

    if demo == 0:
        print("--- %s seconds ---" % (execution_time))

    spark.stop()
    print('\n[*] Done\n')


if __name__ == "__main__":
    main()
