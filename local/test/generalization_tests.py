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

import mondrian.generalization as gnrlz
import pandas as pd
import pathlib
import sys
import time


def main():
    """
    Run the trivial manual tests
    """
    _prefix_test01()
    _taxonomy_test_01()
    _taxonomy_test_02()
    _numeric_generalization_test_01()


def _prefix_test01():

    print("=" * 80)
    print("PREFIX GENERALIZATION TEST 01")
    print("=" * 80)

    prefix_tests = [
        "dataset/prefix_test1.csv",
        "dataset/prefix_test2.csv",
        "dataset/prefix_test3.csv",
        "dataset/prefix_test4.csv",
        "dataset/prefix_test5.csv",
    ]

    for t in prefix_tests:
        gnrlz.generalize_to_cp(debug=True, t=t)


def _taxonomy_test_01():

    print("=" * 80)
    print("TAXONOMY TEST 01")
    print("=" * 80)

    taxonomy, _ = gnrlz._read_categorical_taxonomy(
        "taxonomy/category_example.json",
        debug=True
    )

    vals = ["daughter2 gc_d1", "daughter gc_d1"]
    category = gnrlz.generalize_to_lcc(vals, taxonomy)
    print("\nvals: {}".format(vals))
    print("Categories: {}".format(category))

    vals = ["daughter2 gc_d1", "son"]
    category = gnrlz.generalize_to_lcc(vals, taxonomy)
    print("\nvals: {}".format(vals))
    print("Categories: {}".format(category))

    vals = ["grandchild s1"]
    category = gnrlz.generalize_to_lcc(vals, taxonomy)
    print("\nvals: {}".format(vals))
    print("Categories: {}".format(category))


def _taxonomy_test_02():

    print("=" * 80)
    print("TAXONOMY TEST 02")
    print("=" * 80)

    taxonomy, _ = gnrlz._read_categorical_taxonomy("taxonomy/countries.json",
                                                   debug=True)

    vals = ["Poland", "Italy"]
    category = gnrlz.generalize_to_lcc(vals, taxonomy)
    print("\nvals: {}".format(vals))
    print("Categories: {}".format(category))

    vals = ["?", "Oceania"]
    category = gnrlz.generalize_to_lcc(vals, taxonomy)
    print("\nvals: {}".format(vals))
    print("Categories: {}".format(category))


def _numeric_generalization_test_01():

    print("=" * 80)
    print("NUMERIC GENERALIZATION TESTS")
    print("=" * 80)

    df = pd.read_csv("dataset/adults.csv")
    print(df.head)
    values_list = [1, 2, 16, 16]

    fanout = 4
    taxonomy, minv = gnrlz._taxonomize_numeric(df=df,
                                                col_label="education-num",
                                                fanout=fanout,
                                                accuracy=2.5,
                                                digits=3,
                                                debug=True)
    print("\n[*] Generalize to least common partition for values: " + "{" +
          "; ".join(map(str, set(values_list))) + "}",
          end="\n")
    partition = gnrlz.generalize_to_lcp(values_list, taxonomy, minv, fanout)
    print("[*] Partition found: " + partition, end="\n")

    fanout = 2
    taxonomy, minv = gnrlz._taxonomize_numeric(df=df,
                                                col_label="education-num",
                                                fanout=fanout,
                                                accuracy=0.227555,
                                                digits=2,
                                                debug=True)
    print("\n[*] Generalize to least common partition for values: " + "{" +
          "; ".join(map(str, set(values_list))) + "}",
          end="\n")
    partition = gnrlz.generalize_to_lcp(values_list, taxonomy, minv, fanout)
    print("[*] Partition found: " + partition, end="\n")

    fanout = 25
    taxonomy, minv = gnrlz._taxonomize_numeric(df=df,
                                                col_label="education-num",
                                                fanout=fanout,
                                                accuracy=7.8,
                                                digits=3,
                                                debug=True)
    print("\n[*] Generalize to least common partition for values: " + "{" +
          "; ".join(map(str, set(values_list))) + "}",
          end="\n")
    partition = gnrlz.generalize_to_lcp(values_list, taxonomy, minv, fanout)
    print("[*] Partition found: " + partition, end="\n")


if __name__ == "__main__":
    print("current path: " + str(pathlib.Path().absolute()))
    sys.path.append(str(pathlib.Path().absolute()))
    print("sys.path: " + str(sys.path))
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
