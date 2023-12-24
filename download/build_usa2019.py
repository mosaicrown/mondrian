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

dataframes = []

st_mapping = {
    0 : "?",
    1 : "Alabama",
    2 : "Alaska",
    4 : "Arizona",
    5 : "Arkansas",
    6 : "California",
    8 : "Colorado",
    9 : "Connecticut",
    10 : "Delaware",
    11 : "District of Columbia",
    12 : "Florida",
    13 : "Georgia",
    15 : "Hawaii",
    16 : "Idaho",
    17 : "Illinois",
    18 : "Indiana",
    19 : "Iowa",
    20 : "Kansas",
    21 : "Kentucky",
    22 : "Louisiana",
    23 : "Maine",
    24 : "Maryland",
    25 : "Massachusetts",
    26 : "Michigan",
    27 : "Minnesota",
    28 : "Mississippi",
    29 : "Missouri",
    30 : "Montana",
    31 : "Nebraska",
    32 : "Nevada",
    33 : "New Hampshire",
    34 : "New Jersey",
    35 : "New Mexico",
    36 : "New York",
    37 : "North Carolina",
    38 : "North Dakota",
    39 : "Ohio",
    40 : "Oklahoma",
    41 : "Oregon",
    42 : "Pennsylvania",
    44 : "Rhode Island",
    45 : "South Carolina",
    46 : "South Dakota",
    47 : "Tennessee",
    48 : "Texas",
    49 : "Utah",
    50 : "Vermont",
    51 : "Virginia",
    53 : "Washington",
    54 : "West Virginia",
    55 : "Wisconsin",
    56 : "Wyoming",
    72 : "Puerto Rico"
    }

col_list = ["ST", "AGEP", "OCCP", "WAGP"]
for name in ["psam_pusa.csv", "psam_pusb.csv"]:
    df = pd.read_csv(f"usa2019-raw/{name}", usecols=col_list)
    df.OCCP = df.OCCP.fillna(0)
    df.WAGP = df.WAGP.fillna(0)
    df.OCCP = df.OCCP.astype(int)
    df.WAGP = df.WAGP.astype(int)
    dataframes.append(df)

df = pd.concat(dataframes)
df.ST = df.ST.map(st_mapping)
df.to_csv('../distributed/dataset/usa2019.csv', index=False)
df.to_csv('../local/dataset/usa2019.csv', index=False)
