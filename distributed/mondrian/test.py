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

from collections import namedtuple

from pyspark.sql.utils import AnalysisException


def write_test_params(spark_session, measures, filename):
    """Function to write test configuration and macro-results to Hadoop

    :spark_session: The current Spark session
    :measures: The dictionary of parameters logged to Hadoop
    :filename: The Hadoop target file
    """
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
    print("[*] Recap last 20 runs (or less)")
    try:
        df = spark_session.read \
        .options(header='true', inferSchema='true') \
        .format("csv").load(filename)
        df.select(list_of_parameters).show(30)
    except AnalysisException as HadoopFileNotPresetError:
        print(f"\t -> new target file created: {filename}")
        pass
