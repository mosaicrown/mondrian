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


# Functions to evaluate if a partition is valid

def is_k_numerous(df, partition, sensitive_column, K):
    """Check if the number of values of a columns is k-numerous."""
    return len(partition) >= K


def is_l_diverse(df, partition, sensitive_column, L):
    """Check if a partition is l-diverse."""
    return df[sensitive_column][partition].nunique() >= L


def is_k_l_valid(df, partition, sensitive_column, K, L):
    """Check if a partition is both k-anonymous and l-diverse."""
    return is_k_numerous(df, partition, sensitive_column, K) \
        and is_l_diverse(df, partition, sensitive_column, L)
