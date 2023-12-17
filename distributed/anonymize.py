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
from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import generalization as gnrlz
from anonymization import anonymize
from evaluation import discernability_penalty
from evaluation import evaluate_information_loss
from evaluation import normalized_certainty_penalty
from fragmentation import create_fragments
from fragmentation import get_fragments_quantiles
from fragmentation import mondrian_fragmentation
from fragmentation import quantile_fragmentation
from fragmentation import mondrian_buckets
from score import entropy, neg_entropy, span, norm_span
from utils import get_extension, repartition_dataframe, prepare_parallelization_udf_schema, remap_fragments_to_float
from test import write_test_params
from validation import get_validation_function


def __generalization_preproc(job, df, spark):
    """Anonymization preprocessing to arrange generalizations.

    :job: Dictionary job, contains information about generalization methods
    :df: Dataframe to be anonymized
    :spark: Spark instance
    :returns: Dictionary of taxonomies required to perform generalizations
    """
    if 'quasiid_generalizations' not in job:
        return df, None

    quasiid_gnrlz = dict()

    for gen_item in job['quasiid_generalizations']:
        g_dict = dict()
        g_dict['qi_name'] = gen_item['qi_name']
        g_dict['generalization_type'] = gen_item['generalization_type']
        g_dict['params'] = gen_item['params']
        if g_dict['generalization_type'] == 'categorical':
            # read taxonomy from file
            t_db = g_dict['params']['taxonomy_tree']
            create_ordering = g_dict['params'].get('create_ordering', False)
            if t_db is None:
                raise gnrlz.IncompleteGeneralizationInfo()
            taxonomy, leaves_ordering = gnrlz._read_categorical_taxonomy(t_db, create_ordering)
            g_dict['taxonomy_tree'] = taxonomy
            g_dict['taxonomy_ordering'] = leaves_ordering
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
        elif g_dict['generalization_type'] == 'lexicographic':
            # Enforce column as string
            column = g_dict['qi_name']
            # Translate strings to numbers
            values = sorted([str(r[column])
                             for r in df.select(column).distinct().collect()])
            str2num = {value:i for i, value in enumerate(values)}
            to_num = F.udf(lambda v: str2num[str(v)], T.IntegerType())
            df = df.withColumn(column, to_num(column))
            # Prepare num to string mapping for generalization phase
            num2str = {i:value for i, value in enumerate(values)}
            g_dict['mapping'] = num2str

        quasiid_gnrlz[gen_item['qi_name']] = g_dict

    # return the generalization dictionary
    return df, quasiid_gnrlz


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

    # Parameters
    filename_in = job['input']
    filename_out = job['output']
    # when repartition is not given it defaults to repartitionByRange
    if 'repartition' in job and \
        job['repartition'] in {'customRepartition',
                               'repartitionByRange',
                               'noRepartition'}:
        repartition = job['repartition']
    else:
        repartition = 'repartitionByRange'
    id_columns = job.get('id_columns', [])
    redact = job.get('redact', False)
    quasiid_columns = job['quasiid_columns']
    sensitive_columns = job.get('sensitive_columns')
    flat = job.get("k_flat", False)
    
    # when column score is not given it defaults to span
    score_functions = {'span': span,
                       'entropy': entropy,
                       'neg_entropy': neg_entropy,
                       'norm_span' : 'norm_span'}
    if 'column_score' in job and job['column_score'] in score_functions:
        column_score = score_functions[job['column_score']]
    else:
        column_score = span
    fragments = min(args.WORKERS, job.get('max_fragments', 10**6))
    K = job.get('K')
    L = job.get('L')
    use_categorical = job.get('use_categorical', [])
    measures = job.get('measures', [])

    # Setup mondrian_fragmentation function
    mondrian = functools.partial(mondrian_fragmentation,
                                 sensitive_columns=sensitive_columns,
                                 is_valid=get_validation_function(K,L),
                                 flat=flat)

    # when fraction is not given it defaults to None
    if 'fraction' in job and 0 < job['fraction'] < 1:
        fraction = job['fraction']
    else:
        fraction = None

    # when fragmentation is not given it defaults to quantile_fragmentation
    fragmentation_functions = {'mondrian': mondrian,
                               'quantile': quantile_fragmentation}
    if 'fragmentation' in job and \
            job['fragmentation'] in fragmentation_functions:
        fragmentation = fragmentation_functions[job['fragmentation']]
    else:
        fragmentation = quantile_fragmentation

    parallel = job['parallel'] if 'parallel' in job else False

    if not K and not L:
        raise Exception("Both K and L parameters not given or equal to zero.")
    if L and not sensitive_columns:
        raise Exception(
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

    if fraction:
        df = df.sample(fraction=fraction)

    for attribute in use_categorical:
        df = df.withColumn(attribute, F.col(attribute).cast(T.StringType()))
    
    # initialize taxonomies
    df, quasiid_gnrlz = __generalization_preproc(job, df, spark=spark)
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
    print('\n[*] Fragmentation details\n')
    if not fraction:

        if parallel and job['fragmentation'] == "mondrian":
            print("\n[*] Run without sampling with partial parallelization\n")
            # Prepare the dataframe
            df = df.withColumn('fragment', F.lit("0"))
            schema = prepare_parallelization_udf_schema(redact=redact, df=df, id_columns=id_columns)

            @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
            def single_cut(df):
                if not_last_step or df['fragment'].loc[0] in last_step_filter:
                    df = mondrian_fragmentation(df, quasiid_columns, sensitive_columns, column_score,
                                           get_validation_function(K, L), 2, "fragment", K,False, True)
                return df

            steps = math.ceil(math.log(fragments, 2))
            total = steps
            last_step_cuts = int(fragments - math.pow(2, steps - 1))
            last_step_filter = ["{0:b}".format(i).zfill(steps) for i in range(0, last_step_cuts)]
            not_last_step = True
            spark.sparkContext.broadcast(last_step_filter)
            spark.sparkContext.broadcast(not_last_step)

            while steps > 0:
                if steps == 1:
                    not_last_step = False
                    spark.sparkContext.broadcast(not_last_step)

                df = df \
                    .groupby('fragment') \
                    .applyInPandas(single_cut.func, schema=single_cut.returnType).cache()
                steps -= 1
                if repartition == 'repartitionByRange':
                    df = df.repartitionByRange('fragment')
                elif repartition == 'customRepartition':
                    df = repartition_dataframe(df, spark)
                print(f'\n[*] (Cut {total - steps} of {total}) Number of pre-processing partitions: {df.rdd.getNumPartitions()}')

            # Clean fragment indexes presentation
            df = remap_fragments_to_float(df=df, spark=spark, schema=schema)
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


        else:
            run_type = "Mondrian" if job['fragmentation'] == "mondrian" else "Quantile"
            print(f"\n[*] Run without sampling - {run_type} cuts\n")
            # Create first cut
            pdf = create_fragments(df=pdf,
                                   quasiid_columns=quasiid_columns,
                                   column_score=column_score,
                                   fragments=fragments,
                                   colname='fragment',
                                   criteria=fragmentation,
                                   k=K)
            if job['fragmentation'] == "quantile":
                # Recreate the dataframe in a way that is appreciated by pyarrow.
                pdf = pd.DataFrame.from_dict(pdf.to_dict())
            # Create spark dataframe
            df = spark.createDataFrame(pdf)

            # Compute the range on the quasi-identifiers columns
            # will be useful for information loss evaluation
            quasiid_range = [-1] * len(quasiid_columns)
            for i, column in enumerate(quasiid_columns):
                quasiid_range[i] = span(pdf[column])
    else:

        if job['fragmentation'] == "mondrian":
            if parallel:
                print("\n[*] Sampled run with partial parallelization\n")

                # Prepare the dataframe
                df = df.withColumn('fragment', F.lit("0"))
                df = df.withColumn('bucket', F.lit("[[[],[],[]]]"))
                schema = prepare_parallelization_udf_schema(redact=redact, df=df, id_columns=id_columns)

                @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
                def single_cut(df):
                    if not_last_step or df['fragment'].loc[0] in last_step_filter:
                        df = mondrian_fragmentation(df, quasiid_columns, sensitive_columns, column_score,
                                                    get_validation_function(K, L), 2, "fragment", K, True, True)
                    return df

                steps = math.ceil(math.log(fragments, 2))
                total = steps
                last_step_cuts = int(fragments - math.pow(2, steps - 1))
                last_step_filter = ["{0:b}".format(i).zfill(steps) for i in range(0, last_step_cuts)]
                not_last_step = True
                spark.sparkContext.broadcast(last_step_filter)
                spark.sparkContext.broadcast(not_last_step)

                while steps > 0:
                    if steps == 1:
                        not_last_step = False
                        spark.sparkContext.broadcast(not_last_step)

                    df = df \
                        .groupby('fragment') \
                        .applyInPandas(single_cut.func, schema=single_cut.returnType).cache()
                    steps -= 1
                    if repartition == 'repartitionByRange':
                        df = df.repartitionByRange('fragment')
                    elif repartition == 'customRepartition':
                        df = repartition_dataframe(df, spark)
                    print(f'\n[*] (Cut {total - steps} of {total}) Number of pre-processing partitions: {df.rdd.getNumPartitions()}')
                # Prepare buckets from DF column
                bins = []
                for log in df.select("bucket").distinct().toPandas()['bucket']:
                    log = eval(log)
                    bins.append(log[0])

            else:
                print("\n[*] Run with sampling - Mondrian cuts\n")
                pdf, bins = create_fragments(df=pdf,
                                   quasiid_columns=quasiid_columns,
                                   column_score=column_score,
                                   fragments=fragments,
                                   colname='fragment',
                                   criteria=fragmentation,
                                   is_sampled=True,
                                   k=K)

            df = spark.read \
                .options(header='true', inferSchema='true').csv(filename_in)
            df = mondrian_buckets(df, bins)
        else:
            print("\n[*] Run with sampling- Quantile cuts\n")
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
                df = df.withColumn('fragment', F.lit(0.0))

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

    # Check first cut
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

    print("\n[*] Dataset with fragmentation info\n")
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

    # TODO: add a column to the output schema to keep information on the
    #       equivalent classes to avoid reconstructing them from scratch
    #       in the evaluation of the metrics
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

    if repartition == 'repartitionByRange':
        df = df.repartitionByRange('fragment')
    elif repartition == 'customRepartition':
        df = repartition_dataframe(df, spark)

    print('\n[*] Starting anonymizing the dataframe\n')
    print(f'\n[*] Number of DF partitions: {df.rdd.getNumPartitions()}\n')

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

    print(f'\n[*] Number of ADF partitions: {adf.rdd.getNumPartitions()}\n')
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

    # Remove fragmentation information
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
        test_result_files = ["hdfs://namenode:8020/anonymized/test_results.csv",
         "hdfs://namenode:8020/anonymized/artifact_result.csv"]
        print("[*] Creating test configuration file on Hadoop")
        write_test_params(spark, measures_log, test_result_files)

    if demo == 0:
        print("--- %s seconds ---" % (execution_time))

    spark.stop()
    print('\n[*] Done\n')


if __name__ == "__main__":
    main()
