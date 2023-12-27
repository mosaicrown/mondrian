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

import os

def generalizationRecord(form, files,quasi_identifiers, distributed, app):
    """This function extracts the informations relative to the wanted generalization
    for each quasi_identifier inside a request form.
    :form: request.form to check
    :files: request.files to check
    :quasi_identifier: candidate quasi-identifiers to check
    :distributed: flag to check if the run is for the distributed version
    :app: Flask application"""

    result = []
    for qi in quasi_identifiers:
        record = {}
        qi_type = form['quasiid_type_{}'.format(qi)]

        if qi_type == 'default':
            continue

        record['qi_name'] = qi
        record['generalization_type'] = qi_type
        params = {}
        if qi_type == 'categorical':
            file = files['taxonomy_tree_{}'.format(qi)]
            if distributed:
                path = os.path.join(app.root_path, '../distributed/taxonomy/{}'.format(file.filename))
            else:
                path = os.path.join(app.root_path, '../local/taxonomy/{}'.format(file.filename))
            file.save(path)
            params['taxonomy_tree'] = os.path.join(app.root_path,
             '/mondrian/taxonomy/{}'.format(file.filename)) if distributed else 'taxonomy/{}'.format(file.filename)

            if "taxonomy_ordering_{}".format(qi) in form and form["taxonomy_ordering_{}".format(qi)] == 'on':
                params["create_ordering"] = True

        if qi_type == 'numerical':
            params['fanout'] = int(form['fanout_{}'.format(qi)])
            params['digits'] = int(form['digits_{}'.format(qi)])
            params['accuracy'] = int(form['accuracy_{}'.format(qi)])

        if qi_type == 'common_prefix':
            params['hide-mark'] = form['hidemark_{}'.format(qi)]
        record['params'] = params
        result.append(record)
    return result

def recoverMeasures(data):
    """This function extracts the informations relative to the wanted quality
    measures inside a request form.
    :data: request.form to check"""

    result = []
    if 'dp' in data and data['dp'] == 'on':
        result.append("discernability_penalty")
    if 'ncp' in data and data['ncp'] == 'on':
        result.append("normalized_certainty_penalty")
    if 'gcp' in data and data['gcp'] == 'on':
        result.append("global_certainty_penalty")
    return result
