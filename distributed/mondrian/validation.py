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

import functools


def get_validation_function(K, L):
    if K and L:
        return functools.partial(is_k_l_valid, K=K, L=L)
    elif K:
        return functools.partial(is_k_anonymous, K=K)
    elif L:
        return functools.partial(is_l_diverse, L=L)
    else:
        raise AttributeError("Both K and L parameters not given or equal to zero.")


# Functions to evaluate if a partition is valid

def is_k_anonymous(df, partition, sensitive_columns, K):
    """Check if a partition is k-anonymous."""
    return len(partition) >= K


def is_l_diverse(df, partition, sensitive_columns, L):
    """Check if a partition is l-diverse."""
    # Low performance solution
    # nunique = df.loc[partition, sensitive_columns].nunique()
    # return (nunique >= L).all()

    for column in sensitive_columns:
        if df[column][partition].nunique() < L:
            return False
    return True


def is_k_l_valid(df, partition, sensitive_columns, K, L):
    """Check if a partition is both k-anonymous and l-diverse."""
    return is_k_anonymous(df, partition, sensitive_columns, K) \
        and is_l_diverse(df, partition, sensitive_columns, L)
