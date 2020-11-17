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

import scipy.stats


# Functions to evaluate the cut-score of a column

def entropy(ser):
    """Calculate the entropy of the passed `pd.Series`."""
    counts = ser.value_counts()
    return scipy.stats.entropy(counts)


def neg_entropy(ser):
    """Revert entropy sign to order column in increasing order of entropy."""
    return -entropy(ser)


def span(ser):
    """Calculate the span of the passed `pd.Series`."""
    if ser.dtype.name in ('object', 'category'):
        return ser.nunique()
    else:
        return ser.max() - ser.min()