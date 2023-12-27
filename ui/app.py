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

from flask import Flask, render_template, jsonify, request
from flask.json import dump

from ui_utils.form_management import generalizationRecord, recoverMeasures
from ui_utils.shell_management import finalizeDistributed, finalizeLocal


app = Flask(__name__)

@app.route('/readForm', methods=['POST'])
def readForm():
    """Function called by the form submission. This function creates a
    configuration file based on the parameters submitted by the user"""

    if request.form:
        # check what tool to call
        distributed = request.form['distrib']== 'true'

        # distributed version
        if distributed and 'local_dataset' in request.files:
            # move dataset in known directory
            file = request.files['local_dataset'] # dataset to add to HDFS
            file_name = file.filename
            file_path = os.path.join(app.root_path, '../distributed/dataset/{}'.format(file_name)) # known location to save the dataset
            file.save(file_path)
            file_path = os.path.join(app.root_path, '/mondrian/dataset/{}'.format(file_name)) # known path for the config file
        # local version
        else:
            file = request.files['input'] # dataset to process
            file_name = file.filename
            file_path = os.path.join(app.root_path, '../local/dataset/{}'.format(file_name)) # known location to save the dataset
            file.save(file_path)
            file_path = "dataset/{}".format(file_name) # known path for the config file

        outputs = list(filter(lambda x: len(x)>0, request.form.getlist('output')))
        inputs = list(filter(lambda x: len(x)>0, request.form.getlist('input')))

        dict = {'input' : inputs[0] if distributed else file_path,
                'output' : outputs[0],
                'id_columns' : request.form.getlist('identifiers'),
                'redact': True if 'redact' in request.form and request.form['redact'] == 'on' else False,
                'quasiid_columns' : request.form.getlist('quasi-identifiers'),
                'sensitive_columns' : request.form.getlist('sensitive'),
                'column_score' : request.form['column_score'],
                'measures' : recoverMeasures(request.form),
                'fragmentation' : request.form['fragmentation'],
                'parallel': True if 'parallelization_checkbox' in request.form and request.form['parallelization_checkbox'] == 'on' else False,
                'k_flat': True if 'k-flat' in request.form and request.form['k-flat'] == 'on' else False
                }

        # process the generalization used on the quasi-identifiers
        generalizations = generalizationRecord(request.form, request.files,
        request.form.getlist('quasi-identifiers'), distributed, app)

        # check for optional parameters
        if len(generalizations) > 0:
            dict['quasiid_generalizations'] = generalizations
        if 'k' in request.form and len(request.form['k']) >0 :
            dict['K'] = int(request.form['k'])
        if 'l' in request.form and len(request.form['l']) >0 :
            dict['L'] = int(request.form['l'])
        if 'fraction' in request.form and len(request.form['fraction']) >0 :
            dict['fraction'] = float(request.form['fraction'])\
             if request.form['fraction'] != '1' else 1

        # save the json configuration of the job
        file_name, file_extension = os.path.splitext(file_name)
        json_path = os.path.join(app.root_path,
                         '../distributed/config/{}_ui.json'.format(file_name))\
                          if distributed\
                          else  os.path.join(app.root_path,
                          '../local/config/{}_ui.json'.format(file_name))

        with open(json_path, 'w') as f:
            dump(dict, f, indent=2)

        # call the job based on configurations and tool version
        if distributed:
            output_path = '/mondrian/anonymized/{}_ui{}'.format(file_name, file_extension)
            json_path = '/mondrian/config/{}_ui.json'.format(file_name)
            finalizeDistributed(file_path, dict['input'], dict['output'], output_path, json_path, int(request.form['workers']), app)
        else:
            json_path = 'config/{}_ui.json'.format(file_name)
            finalizeLocal(json_path, app)

    return render_template('summary.html', distr = distributed)

@app.route('/end')
def end():
    """Function to render the index page"""
    return render_template('summary.html')


@app.route('/')
def index():
    """Function to render the index page"""
    return render_template('index.html')


@app.route('/shutdown', methods=['GET'])
def shutdown():
    """Function to handle the server shutdown"""

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

    return 'Server shut down'

if __name__ == '__main__':
    app.run(host="0.0.0.0", port="5000")
