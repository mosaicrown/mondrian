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

from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import argparse
import functools
import json
import os
import sys
import time

from anonymization import anonymize
from dataframe_operations import repartition_dataframe
from evaluation import discernability_penalty, normalized_certainty_penalty, evaluate_information_loss
from mondrian_operations import mondrian_fragmentation, quantile_fragmentation, create_fragments,\
    get_fragments_quantiles, __generalization_preproc
from scores import entropy, neg_entropy, span

from validation import is_k_l_valid



def get_extension(filename):
    _, sep, extension = filename.rpartition(".")
    if not sep:
        extension = None
    return extension


def write_test_params(spark_session, measures, filename):
    """Function to write test configuration and macro-results to Hadoop

    :spark_session: The current Spark session
    :measures: The dictionary of parameters logged to Hadoop
    :filename: The Hadoop target file
    """
    from collections import namedtuple
    parameters = "timestamp fragments repartition K L fraction DP NCP GCP time"
    test_res_row = namedtuple("test_res_row", parameters.split())

    list_of_parameters = [*parameters.split(" ")]
    ordered_values = []
    for k in list_of_parameters:
        if k in measures:
            ordered_values.append(str(measures[k]))
        else:
            ordered_values.append("")
    test_results = [test_res_row(*ordered_values)]

    writing_mode = "overwrite"
    from pyspark.sql.utils import AnalysisException
    try:
        testfile = spark_session.read \
        .options(header='true', inferSchema='true') \
        .format("csv").load(filename)
        writing_mode = "append"
    except AnalysisException as HadoopFileNotPresetError:
        print(f"\t -> new target file created: {filename}")
        pass

    # Write test_results to HDFS
    print("\n")
    test_results_df = spark_session.createDataFrame(test_results)
    test_results_df.select(list_of_parameters).show(2)
    test_results_df.write \
        .mode(writing_mode) \
        .options(header=True) \
        .format("csv") \
        .save(filename)

    # debug written configuration values
    _visualize_csv_util(spark_session, filename, list_of_parameters)

def _visualize_csv_util(spark_session, filename, list_of_parameters):
    """Internal utility to visualize the test configuration and macro-results written to Hadoop

    :spark_session: The current Spark session
    :filename: The Hadoop target file
    :list_of_parameters: Ordered list of parameters to be printed
    """
    from pyspark.sql.utils import AnalysisException
    print("[*] Recap last 20 runs (or less)")
    try:
        df = spark_session.read \
        .options(header='true', inferSchema='true') \
        .format("csv").load(filename)
        df.select(list_of_parameters).show(20)
    except AnalysisException as HadoopFileNotPresetError:
        print(f"\t -> new target file created: {filename}")
        pass

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
    spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')

    # Share generalization library
    spark.sparkContext.addPyFile("/mondrian/code/generalizations.py")
    spark.sparkContext.addPyFile("/mondrian/code/anonymization.py")
    spark.sparkContext.addPyFile("/mondrian/code/dataframe_operations.py")
    spark.sparkContext.addPyFile("/mondrian/code/evaluation.py")
    spark.sparkContext.addPyFile("/mondrian/code/mondrian_operations.py")
    spark.sparkContext.addPyFile("/mondrian/code/scores.py")
    spark.sparkContext.addPyFile("/mondrian/code/validation.py")

    if demo == 1:
        print("\n[*] Spark context initialized")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    # Parameters
    filename_in = job['input']
    filename_out = job['output']
    repartition = job['repartition'] if 'repartition' in job and job['repartition'] in {'customRepartition', 'repartitionByRange', 'noRepartition'} else 'repartitionByRange'
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

    # Read file according to extension
    print('[*] Reading from {}\n'.format(filename_in))
    extension = get_extension(filename_in)
    df = spark.read \
        .options(header='true', inferSchema='true') \
        .format(extension).load(filename_in)

    if fraction:
        df = df.sample(fraction=fraction, seed=0)
    pdf = df.toPandas()
    pdf.info()

    print('\n[*] Fragmentation details\n')

    
    """
    TODO: Avoid having a single node performing this step for the whole dataset
    """
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

        if len(bins) != 2:
            # split into buckets only if there are more than 1
            bucketizer = Bucketizer(splits=bins,
                            inputCol=column,
                            outputCol='fragment')
            df = bucketizer.transform(df)
        else:
            # otherwise assign every row to bucket 0
            from pyspark.sql.functions import lit
            df = df.withColumn('fragment', lit(0.0))

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

    # TODO: add a column to the output schema to keep information on the
    #       equivalent classes to avoid reconstructing them from scratch
    #       in the evaluation of the metrics

    if demo == 1 and fragments > 1:
        print("\n[*] Dataset fragmented")
        print("\tWait for 10 seconds to continue demo...")
        time.sleep(10)

    # initialize taxonomies
    quasiid_gnrlz = None
    quasiid_gnrlz = __generalization_preproc(job, df, spark=spark)

    if demo == 1 and quasiid_gnrlz:
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
                        quasiid_gnrlz=quasiid_gnrlz)

        # Ensure that the quasi identifier columns have been converted
        # to strings (they are required by the return type).
        for column in quasiid_columns:
            adf[column] = adf[column].astype('object')

        return adf
  
    if repartition == 'repartitionByRange':
        df = df.repartitionByRange('fragment')
    elif repartition == 'customRepartition':
        df = repartition_dataframe(df, spark)
   	
    print('\n[*] Starting anonymizing the dataframe\n')
    print('Number of DF partitions: {}'.format(df.rdd.getNumPartitions()))
    
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
                                           quasiid_gnrlz=quasiid_gnrlz)
        # pandas_udf requires a pandas dataframe as output
        return pd.DataFrame({'information_loss': [gcp]})

    if repartition == 'repartitionByRange':
    	adf = adf.repartitionByRange('fragment')
    elif repartition == 'customRepartition':
    	adf = repartition_dataframe(adf, spark)
    	
    print('Number of ADF partitions: {}'.format(adf.rdd.getNumPartitions()))
    adf.drop('fragment').show(10)

    print('\n[*] Anonymized dataframe')

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

    if measures:
        print('[*] Information loss evaluation\n')

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

    # Write test params to Hadoop
    print("[*] Creating test configuration file on Hadoop")
    write_test_params(spark, measures_log, "hdfs://namenode:8020/anonymized/test_results.csv")

    if demo == 0:
        print("--- %s seconds ---" % (execution_time))

    spark.stop()
    print('\n[*] Done\n')

    
if __name__ == "__main__":
    main()
