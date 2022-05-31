# Copyright 2022 Unibg Seclab (https://seclab.unibg.it)
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

import pandas as pd
import argparse

def main():
        parser = argparse.ArgumentParser(
        description='Query database hosting the wrapped dataset using' + 'the given mapping.')
        parser.add_argument('input', metavar='INPUT', help='anonymized dataset')
        parser.add_argument('k', help='group size to verify')
        parser.add_argument('qi', nargs='+', help='list of quasi identifier used for anonymization')
        args = parser.parse_args()

        dataset = args.input
        k = int(args.k)
        qi = args.qi
        df = pd.read_csv(dataset, dtype=str)
        print(f"DATASET SIZE: {len(df.index)}")
        correct = 0
        bigger = 0
        wrong = 0
        wrong_set = set()
        for (i,j) in zip(df.groupby(qi).size(), df.groupby(qi)):
                if i % k != 0:
                        if i % (k+1) != 0:
                                wrong += 1
                                wrong_set.add(i)
                        else:
                                bigger += int(i / (k-1))
                else:
                        correct += int(i / k)
                        if i != k:
                                print(f"Not exactly {k}: {i}")
        print(f"Correct, size=={k}: {correct}")
        print(f"Correct, size=={k+1}: {bigger}")
        print(f"Number of uncorrect sizes: {wrong}")
        if wrong:
                print("Wrong sizes:", ", ".join(map(str, wrong_set)))
if __name__ == '__main__':
        main()
