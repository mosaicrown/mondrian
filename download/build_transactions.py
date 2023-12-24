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
import time
import datetime

def convert_date(s):
        return int(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d").timetuple()))        

if __name__ == "__main__":
        col_list = ["purchaseamount", "date", "company", "category", "id"]
        df = pd.read_csv("transactions.csv", usecols=col_list)         
        df = df.sample(n=30000000)
        df["date"] = df["date"].map(convert_date)                       
        df.to_csv("../distributed/dataset/transactions.csv", index_label="INDEX")
