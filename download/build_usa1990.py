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

import csv
import numpy


marital_map = ('Now Married', 'Widowed', 'Divorced', 'Separated', 'Never Married')


def convert_marital(value):
    global marital_map
    return marital_map[int(value)]


def convert_income(value):
    value = int(value)
    if value == 0 :
        return '0'
    elif value < 15000:
        return '<15k'
    elif value < 30000:
        return '<30k'
    elif value < 60000:
        return '<60k'
    return '>=60k'


if __name__ == '__main__':
    with open('usa1990/USCensus1990raw.data.txt', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')

        rows = [['AGE', 'SEX', 'INCOME', 'OCC', 'EDU', 'MARITAL']]
        for row in reader:

            if len(row) == 0:
                continue

            final_row = []
            #print('+++++++++++++')
            #print('Age ', row[12])
            final_row.append(int(row[12]))
            #print('Sex ', row[112])
            final_row.append('Male' if row[112] == '0' else 'Female')
            #print('Income ', row[65])
            final_row.append(convert_income(row[65]))
            #print('Occ ', row[86])
            final_row.append(int(row[86]))
            #print('School', row[122])
            final_row.append(int(row[122]))
            #print('Marital status', row[78])
            final_row.append(convert_marital(row[78]))
            #print('+++++++++++++')

            rows.append(final_row)

        rows = numpy.asarray(rows)
        numpy.savetxt("../distributed/dataset/usa1990.csv", rows, delimiter=",", fmt='%s')
        numpy.savetxt("../local/dataset/usa1990.csv", rows, delimiter=",", fmt='%s')
