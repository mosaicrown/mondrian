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
import time

import pandas as pd

from mondrian import generalization as gnrlz
from mondrian.anonymization import anonymize
from mondrian.evaluation import discernability_penalty
from mondrian.evaluation import global_certainty_penalty
from mondrian.evaluation import normalized_certainty_penalty
from mondrian.score import entropy, neg_entropy, span, norm_span
from mondrian.visualization import visualizer
from mondrian.test import result_handler


SCORE_FUNCTIONS = {
    'span': span,
    'entropy': entropy,
    'neg_entropy': neg_entropy,
    'norm_span' : 'norm_span'
}


def __generalization_preproc(job, df):
    """Anonymization preprocessing to arrange generalizations.

    :job: Dictionary job, contains information about generalization methods
    :df: Dataframe to be anonymized
    :returns: Dictionary of taxonomies required to perform generalizations
    """
    if 'quasiid_generalizations' not in job:
        return None

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
            # taxonomy.show()
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
                df=df,
                col_label=g_dict['qi_name'],
                fanout=int(fanout),
                accuracy=float(accuracy),
                digits=int(digits))
            g_dict['taxonomy_tree'] = taxonomy
            g_dict['min'] = minv
            # taxonomy.show()
            # print("Minv: {}".format(minv))
        # elif g_dict['generalization_type'] == 'common_prefix':
        # common_prefix generalization doesn't require taxonomy tree
        elif g_dict['generalization_type'] == 'lexicographic':
            column = g_dict['qi_name']
            # Enforce column as string
            df[column] = df[column].map(str)
            # Convert string to num to avoid treating column as categorical
            values = sorted(df[column].unique())
            str2num = {value:i for i, value in enumerate(values)}
            df[column] = df[column].apply(lambda string: str2num[string])
            # Prepare num to string mapping for generalization phase
            num2str = {i:value for i, value in enumerate(values)}
            g_dict['mapping'] = num2str

        quasiid_gnrlz[gen_item['qi_name']] = g_dict

    # return the generalization dictionary
    return quasiid_gnrlz


def main():
    parser = argparse.ArgumentParser(
        description='Anonymize a dataset using Mondrian.')
    parser.add_argument('METADATA', help='json file that describes the job.')
    parser.add_argument('DEMO',
                        default=0,
                        type=int,
                        help='Launch in demo mode.')

    args = parser.parse_args()
    demo = args.DEMO

    if demo == 1:
        print("\n[*] Read configuration file")
        input("\t Press any key to continue...")

    with open(args.METADATA) as fp:
        job = json.load(fp)

    start_time = time.time()
    # Measures
    test_measures = {}

    # Parameters
    input_filename = job['input']
    output_filename = job['output']
    id_columns = job.get('id_columns', [])
    redact = job.get('redact', False)
    quasiid_columns = job['quasiid_columns']
    sensitive_columns = job.get('sensitive_columns', [])
    try:
        column_score = SCORE_FUNCTIONS[job.get('column_score', 'span')]
    except KeyError:
        raise ValueError(f"Column score must be one of "
                         f"{', '.join(SCORE_FUNCTIONS)}")
    K = job.get('K')
    L = job.get('L')
    measures = job.get('measures', [])

    if K:
        test_measures["K"] = K

    if not K and not L:
        raise ValueError("Both K and L parameters not given or equal to zero.")
    if L:
        test_measures["L"] = L
        if not sensitive_columns:
            raise ValueError(
                "l-diversity needs to know which columns are sensitive."
            )

    if demo == 1:
        print("\n[*] Job info configured")
        input("\t Press any key to continue...")

    if demo == 1:
        print("\n[*] Reading the dataset")
    df = pd.read_csv(input_filename)
    print(df.head)

    quasiid_gnrlz = __generalization_preproc(job, df)
    categoricals_with_order = {}
    if quasiid_gnrlz is not None:
        for qi in quasiid_gnrlz.values():
            if 'taxonomy_ordering' in qi and qi['taxonomy_ordering'] is not None:
                categoricals_with_order[qi['qi_name']] = qi['taxonomy_ordering']
    
    total_spans = None
    if column_score == "norm_span" :
        total_spans = {}
        for qi in quasiid_columns:
            ser = df[qi]
            if ser.dtype.name in ('object', 'category') and ser.name not in categoricals_with_order:
                total_spans[ser.name] = ser.nunique()
            elif ser.name in categoricals_with_order:
                total_spans[ser.name] = len(categoricals_with_order[ser.name]) - 1
            else:
                total_spans[ser.name] = ser.max() - ser.min()

        column_score = functools.partial(norm_span, total_spans=total_spans, categoricals_with_order=categoricals_with_order)

    if demo == 1:
        print("\n[*] Taxonomies info read")
        input("\t Press any key to continue...\n")

    qi_range = [-1] * len(quasiid_columns)
    for i, column in enumerate(quasiid_columns):
        qi_range[i] = span(df[column])

    adf = anonymize(
        df=df,
        id_columns=id_columns,
        redact=redact,
        quasiid_columns=quasiid_columns,
        sensitive_columns=sensitive_columns,
        column_score=column_score,
        K=K,
        L=L,
        quasiid_gnrlz=quasiid_gnrlz,
        categoricals_with_order=categoricals_with_order)

    if demo == 1:
        print("\n[*] Dataset anonymized")
        input("\t Press any key to continue...")

    print('\n[*] Anonymized dataframe:\n')

    if adf.size < 50:
        print(adf)
        visualizer(adf, quasiid_columns)
    else:
        print(adf.head)

    if demo == 1 and measures:
        print("\n[*] Starting evaluate information loss")
        input("\t Press any key to continue...")

    if measures:
        print('\n[*] Information loss evaluation\n')
    for measure in measures:
        if measure == 'discernability_penalty':
            dp = discernability_penalty(adf, quasiid_columns)
            print(f"Discernability Penalty = {dp:.2E}")
            test_measures["DP"] = dp
        elif measure == 'normalized_certainty_penalty':
            ncp = normalized_certainty_penalty(df, adf, quasiid_columns,
                                               qi_range, quasiid_gnrlz)
            print(f"Normalized Certainty Penalty = {ncp:.2E}")
            test_measures["NCP"] = ncp
        elif measure == 'global_certainty_penalty':
            gcp = global_certainty_penalty(df, adf, quasiid_columns,
                                           qi_range, quasiid_gnrlz)
            print(f"Global Certainty Penalty = {gcp:.4f}")
            test_measures["GCP"] = gcp
    # Write file according to extension
    print(f"\n[*] Writing to {output_filename}")
    adf.to_csv(output_filename, index=False)

    print('\n[*] Done\n')
    end_time = time.time()
    if demo == 0:
        print("--- %s seconds ---" % (end_time - start_time))
    test_measures["time"] = (end_time - start_time)
    test_measures["timestamp"] = end_time
    result_handler(test_measures)

if __name__ == "__main__":
    main()
