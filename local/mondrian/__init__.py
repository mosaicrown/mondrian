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

# Make all the files available as submodules.
from . import anonymization
from . import evaluation
from . import generalization
from . import mondrian
from . import score
from . import utils
from . import validation
from . import visualization

# Allow 'from mondrian import *' syntax.
__all__ = [
    "anonymization",
    "evaluation",
    "generalization",
    "mondrian",
    "score",
    "utils",
    "validation",
    "visualization",
]
