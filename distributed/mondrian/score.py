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
    """Revert the entropy sign to invert the column ordering."""
    return -entropy(ser)

def span(ser):
    """Return the domain cardinality."""
    return ser.nunique()

def norm_span(ser, total_spans, categoricals_with_order):
    """Calculate the normalized span of the passed `pd.Series`."""
    if ser.dtype.name in ('object', 'category') and ser.name not in categoricals_with_order:
        num = ser.nunique()
    else:
        if ser.name in categoricals_with_order:
            mapped = sorted(map(lambda x: categoricals_with_order[ser.name][x], ser.unique()))
            num = mapped[-1] - mapped[0]
        else:
            num = ser.max() - ser.min()
    den = total_spans[ser.name]
    return (num / den, ser.nunique())
