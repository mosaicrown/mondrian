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

import json
import math
from collections import defaultdict

import pandas as pd
import treelib


class IncompleteGeneralizationInfo(Exception):
    """Custom generalization missing infomation"""
    def __str__(self):
        return "Missing generalization info"


class WrongTaxonomyTreeFanoutException(Exception):
    """Info about tree fanout"""
    def __str__(self):
        return "Fanout has to be at least 2"


class NegativeNumberOfDecimalDigits(Exception):
    """Info about tree fanout"""
    def __str__(self):
        return "Accuracy decimals must be at least zero"


class EmptyTaxonomyInUse(Exception):
    """Info about taxonomy tree"""
    def __str__(self):
        return "Taxonomy must specify at least one values"


def _create_numeric_taxonomy(col, fanout, accuracy, digits):
    """
    Creates a balanced taxonomy tree of a given column
    :col: The pandas column to taxonomize
    :fanout: The fanout of the balanced tree to be used
    :accuracy: The width of the bucket used to store each column value
    :digits: The number of decimals to be used to for partition boundaries
    :returns: The taxonomy of nodes, plus some useful metadata
    """
    # column data extraction
    maxv = col.max()
    minv = col.min()
    vals = col.unique()
    vals.sort()

    print("Fanout: {} - Accuracy: {} - Digits{}".format(
        fanout, accuracy, digits))

    # taxonomy dimensioning
    nof_ranges = int(math.ceil(abs(float(maxv) - minv) / accuracy))
    # print("nof_ranges[{}]".format(nof_ranges), end="\n")
    levels = int(math.ceil(math.log(nof_ranges, fanout)))
    # print("levels[{}]".format(levels), end="\n")
    max_range = math.pow(fanout, levels) * accuracy
    max_partition = "[{}-{}]".format(minv, minv + max_range)

    # taxonomy creation
    taxonomy = treelib.Tree()
    taxonomy.create_node(max_partition, 1)
    # populate taxonomy parent nodes
    formatter = "[{}-{})"
    node_progression = 1
    prev_node_progression = 0
    # for each level in the tree
    for i in range(1, levels + 1):
        points = int(math.pow(fanout, i))
        # this step is required as the tree full scale range is bigger then the column range
        if i == levels:
            slice = accuracy
        else:
            slice = max_range / points
        # for each place in the level
        for p in range(1, points + 1):
            node_idx = node_progression + p
            parent = prev_node_progression + int(math.ceil(float(p) / fanout))
            high = slice * (p - 1) + slice + minv
            low = high - slice
            if i == levels and p == points:
                formatter = "[{}-{}]"
            range_string = formatter.format(round(low, digits),
                                            round(high, digits))
            # print(range_string + "idx[{}]-parent[{}]".format(node_idx, parent),
            # end="\n")
            # nodename, nodeid, parentnodeid
            taxonomy.create_node(range_string,
                                 float(node_idx),
                                 parent=float(parent))
        prev_node_progression = node_progression
        node_progression += points
    # taxonomy.show()

    return maxv, minv, levels, vals, taxonomy


def _taxonomize_numeric(df,
                         col_label,
                         fanout,
                         accuracy,
                         digits=3,
                         debug=False):
    """
    Creates a taxonomy of a pandas numeric column of values
    WARNIN: for numeric values only
    PLEASE 1. Note the following procedure is prone to floating point approximated precision.
       For exact approximation and generalization, please use categorical taxonomies. 2. Negative spanning ranges supported but not yet tested
    """
    if debug:
        print("\n[*] Silly numeric column partitioning", end="\n")

    # checking info about the taxonomy
    if fanout <= 1:
        raise WrongTaxonomyTreeFanoutException()
    if digits < 0:
        raise NegativeNumberOfDecimalDigits()

    # create the taxonomy of parent nodes
    col = df[col_label]
    accuracy = abs(accuracy)
    _, minv, levels, vals, taxonomy = _create_numeric_taxonomy(
        col=col, fanout=fanout, accuracy=accuracy, digits=digits)

    # adding the values to the taxonomy (withouth duplicates)
    nof_leaves = int(math.pow(fanout, levels))
    scale_range = nof_leaves * accuracy
    bucket_offset = 1
    for l in range(0, levels):
        bucket_offset += int(math.pow(fanout, l))
    leaf_offset = 1 + abs(minv)  # prevent negative inversion
    for l in range(0, levels + 1):
        leaf_offset += int(math.pow(fanout, l))
    #print("fanout[{}]-nof_leaves[{}]-accuracy[{}]-scale_range[{}]-bucket_offset[{}]".
    #     format(fanout, nof_leaves, accuracy, scale_range, bucket_offset))
    for v in vals:
        bucket = bucket_offset + int(
            math.floor(nof_leaves *
                       (abs(float(v) - float(minv)) / scale_range)))
        # print("{}-{}-{}".format(v, bucket, idx), end="\n")
        taxonomy.create_node("{}".format(v),
                             leaf_offset + float(v),
                             parent=float(bucket))
        # print("added node with idx: {} for value {}".format(leaf_offset+float(v), float(v)))

    if debug:
        taxonomy.show()
        print("[*] Jsonify taxonomy", end="\n")
        print(taxonomy.to_json(with_data=False), end="\n")

    return taxonomy, minv


def generalize_to_lcp(values, taxonomy, taxonomy_min_val, fanout):
    """
    Given a list of values, find the least common partition that containts it based on a taxonomy tree
    WARNING: this method has to be used only for balanced taxonomy trees of numeric data (generated by _create_numeric_taxonomy)
    :values: The values to generalize
    :taxonomy: The taxonomy the values belongs to
    :taxonomy_min_val: The mininum value stored by the taxonomy (enables O(1) searching)
    :fanout: The fanout of the taxonomy tree
    :returns: The partition range (string) to be used to generalize the values 
    """

    if not isinstance(values, set):
        # ensures duplicates removal
        values = set(values)

    if len(values) == 0:
        return "No partition found (empty list of values)"

    # if the taxonomy tree is empty then return the set of values
    if taxonomy.root == None:
        if len(values) > 1:
            return "[" + str(min(values)) + "-" + str(max(values)) + "]"
        else:
            return "[" + str(min(values)) + "]"

    # find the values node identifiers and the ancestors
    root = taxonomy.root
    # find the taxonomy leaf offset
    taxonomy_depth = taxonomy.depth()
    leaf_offset = 1 + abs(taxonomy_min_val)
    for l in range(0, taxonomy_depth):
        leaf_offset += int(math.pow(fanout, l))

    # use treelib O(1) get_node hashmap based access method
    value_idxes = []
    for v in values:
        idx = leaf_offset + float(v)
        node = taxonomy.get_node(idx)
        if node is not None:
            value_idxes.append(node.identifier)
        else:
            # there is at least one value missing in the taxonomy, simply return the set of values
            # print('looking for idx: {}'.format(idx))
            # print("leaf offset {} - value {} ".format(leaf_offset, float(v)))
            # print("not found")
            if len(values) > 1:
                return "[" + str(min(values)) + "-" + str(max(values)) + "]"
            else:
                return "[" + str(min(values)) + "]"

    # print("Values identifiers: "+ "-".join( map(str, value_idxes)), end="\n")

    ancestors = defaultdict(list)
    # for each index node value
    for idx in value_idxes:
        ancestors[idx] = []
        # get all the ancestors except for the root of the taxonomy
        parent = taxonomy.parent(idx)
        while not parent.is_root():
            ancestors[idx].append(parent.identifier)
            parent = taxonomy.parent(parent.identifier)

    # print("\n[*] Dictionary of parents (root excluded): ", end="\n")
    # print(ancestors)
    # print("", end="\n")

    # intersect and return the ancestor with the lowest idx
    common_ancestors = set.intersection(*[set(x) for x in ancestors.values()])

    # return the least common ancestor (i.e., the partition range)
    if not len(common_ancestors) == 0:
        return taxonomy.get_node(max(common_ancestors)).tag

    # root is the common ancestor (i.e., its partition range)
    return taxonomy.get_node(root).tag


def _read_categorical_taxonomy(taxonomy_json, debug=False, create_ordering=False):
    """
    Reads a taxonomy of categories from a json collection
    :taxonomy_json: The collection to be read
    :returns: The taxonomy tree
    """

    db = ""
    with open(taxonomy_json) as f:
        db = json.load(f)

    taxonomy = treelib.Tree()

    c = db['cat']
    if c is not None:
        _read_category_recursive(None, db, taxonomy)
    else:
        raise EmptyTaxonomyInUse()

    if debug:
        taxonomy.show()

    leaves_ordering = {} if create_ordering else None
    if create_ordering:
        all_nodes = taxonomy.expand_tree(mode=1,sorting=False)
        index = 0
        for node in all_nodes:
            if taxonomy.get_node(node).is_leaf():
                leaves_ordering[node] = index
                index += 1


    return taxonomy, leaves_ordering


def _read_category_recursive(cat, subcat, taxonomy):
    # print("---")
    # print("cat+{}\nsubcat+{}".format(cat, subcat))
    c = subcat['cat']
    taxonomy.create_node(c, c, parent=cat)
    sons = subcat["subcats"]
    if sons is None:
        return
    if type(sons) == dict:
        sons = [sons]
    for e in sons:
        _read_category_recursive(c, e, taxonomy)


def generalize_to_lcc(values, taxonomy):
    """
    Given a list of string categories, find the least common category that generalizes it based on a taxonomy tree
    :values: The categories to generalize
    :taxonomy: The taxonomy the values belongs to
    :returns: The partition range (string) to be used to generalize the values 
    """

    if not isinstance(values, set):
        # ensures duplicates removal
        values = set(values)

    if len(values) == 0:
        return "No categoty found (empty list of values)"

    # if the taxonomy tree is empty then return the set of categories
    root = taxonomy.root
    if root is None:
        return "{" + ", ".join(values) + "}"

    # find the categories node identifiers and the ancestors
    # use treelib O(1) get_node hashmap based access method
    cat_idxes = []
    for v in values:
        node = taxonomy.get_node(v)
        if node is not None:
            cat_idxes.append(node.identifier)
        else:
            # there is at least one category missing in the taxonomy, simply return the set of categories
            return "{" + ", ".join(values) + "}"

    ancestors = defaultdict(list)
    # for each index node value
    for idx in cat_idxes:
        ancestors[idx] = []
        # get all the ancestors except for the root of the taxonomy
        parent = taxonomy.parent(idx)
        while not (parent is None or parent.is_root()):
            ancestors[idx].append(parent.identifier)
            parent = taxonomy.parent(parent.identifier)

    # intersect and return the ancestor with the lowest idx
    common_ancestors = set.intersection(*[set(x) for x in ancestors.values()])
    # print(common_ancestors)

    # return the least common ancestor (i.e., the one at maximum depth)
    if not len(common_ancestors) == 0:
        lcc_ancestor_depth = max(map(taxonomy.depth, common_ancestors))
        idx = [
            x for x in common_ancestors
            if taxonomy.depth(x) == lcc_ancestor_depth
        ].pop()
        return taxonomy.get_node(idx).tag

    # root is the common ancestor (i.e., its partition range)
    return taxonomy.get_node(root).tag


def generalize_to_cp(ser=None, debug=False, hidemark="*", t=None):
    """
    Given a pandas array of categorical items, set all its values to the longest common prefix
    :ser: List of unique categorical values
    :hidemark: Placeholder used to hide information
    """
    # first implementation (easy solution)
    # check empty column
    if ser is not None:
        col = ser
    else:
        df = ""
        with open(t) as f:
            df = pd.read_csv(f)
            col = df['zip'].astype(str)

    # col vals
    l = col.size
    if l == 0:
        return "Empty series"
    # print(col.values)

    pref = ""
    idx = 0
    term = False
    for c in str(col[0]):
        if term:
            break
        stopped = False
        for i in range(1, l):
            v = str(col[i])
            if len(v) <= idx:
                term = True
                break
            else:
                if v[idx] != c:
                    stopped = True
                    term = True
                    break
        if not stopped:
            pref += c
        idx += 1

    # set the prefix for each items
    len_of_first_v = len(str(col[0]))
    suff = hidemark * (len_of_first_v - len(pref))
    
    if debug:
        print("prefix: {}".format(pref))
        print("---------------")

    return pref + suff

def generalization_preproc(job, df):
    """Anonymization preprocessing to arrange generalizations.

    :job: Dictionary job, contains information about generalization methods
    :df: Dataframe to be anonymized
    :returns: Dictionary of taxonomies required to perform generalizations
    """
    if 'quasiid_generalizations' not in job:
        return None

    quasiid_gnrlz = dict()

    for gen_item in job['quasiid_generalizations']:
        g_dict = dict()
        g_dict['qi_name'] = gen_item['qi_name']
        g_dict['generalization_type'] = gen_item['generalization_type']
        g_dict['params'] = gen_item['params']

        if g_dict['generalization_type'] == 'categorical':
            # read taxonomy from file
            t_db = g_dict['params']['taxonomy_tree']
            create_ordering = g_dict['params'].get('create_ordering', False)
            if t_db is None:
                raise IncompleteGeneralizationInfo()
            taxonomy, leaves_ordering = _read_categorical_taxonomy(t_db, create_ordering)
            # taxonomy.show()
            g_dict['taxonomy_tree'] = taxonomy
            g_dict['taxonomy_ordering'] = leaves_ordering
        elif g_dict['generalization_type'] == 'numerical':
            try:
                fanout = g_dict['params']['fanout']
                accuracy = g_dict['params']['accuracy']
                digits = g_dict['params']['digits']
            except KeyError:
                raise IncompleteGeneralizationInfo()
            if fanout is None or accuracy is None or digits is None:
                raise IncompleteGeneralizationInfo()
            taxonomy, minv = _taxonomize_numeric(
                df=df,
                col_label=g_dict['qi_name'],
                fanout=int(fanout),
                accuracy=float(accuracy),
                digits=int(digits))
            g_dict['taxonomy_tree'] = taxonomy
            g_dict['min'] = minv
            # taxonomy.show()
            # print("Minv: {}".format(minv))
        # elif g_dict['generalization_type'] == 'common_prefix':
        # common_prefix generalization doesn't require taxonomy tree
        elif g_dict['generalization_type'] == 'lexicographic':
            column = g_dict['qi_name']
            # Enforce column as string
            df[column] = df[column].map(str)
            # Convert string to num to avoid treating column as categorical
            values = sorted(df[column].unique())
            str2num = {value:i for i, value in enumerate(values)}
            df[column] = df[column].apply(lambda string: str2num[string])
            # Prepare num to string mapping for generalization phase
            num2str = {i:value for i, value in enumerate(values)}
            g_dict['mapping'] = num2str

        quasiid_gnrlz[gen_item['qi_name']] = g_dict

    # return the generalization dictionary
    return quasiid_gnrlz
