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

import subprocess
import os

def finalizeDistributed(path, input, output, output_path, config, nodes, app):
    """This function starts a spark job using docker using the configurations
    provided.
    :path: Path in which is saved the local dataset, relative to spark container
    :input: HDFS url of the dataset used as input
    :output: HDFS url of the anonymized dataset used as output
    :output_path: Path to where the anonymized dataset is downloaded, relative to spark container
    :config: Path in which is saved the json configuration, relative to spark container
    :nodes: Number of workers
    :app: Flask app to recover the relative paths"""

    command = f'''docker exec \\
        -t \\
		-e LOCAL_DATASET={path} \\
		-e HDFS_DATASET={input} \\
        -e HDFS_ANONYMIZED_DATASET={output} \\
		-e LOCAL_ANONYMIZED_DATASET={output_path} \\
		-e SPARK_APP_CONFIG={config} \\
		-e SPARK_APP_WORKERS={nodes} \\
		-e SPARK_APP_DEMO=0 \\
		-e SPARK_APP_TEST=0 \\
        spark-driver \\
		/mondrian/script/submit.sh'''

    os.chdir(os.path.join(app.root_path, '../distributed/'))
    executeCommand("cd mondrian;zip -r ../mondrian.zip .;cd ..")
    executeCommand("mkdir -p anonymized")
    executeCommand(command, False)

    return 0

def finalizeLocal(config, app):
    """This function starts a job using the local version of the tool.
    :config: Path in which is saved the json configuration, relative to the local tool
    :app: Flask app to recover the relative paths"""

    os.chdir(os.path.join(app.root_path, '../local/'))
    command = '''./venv/bin/python anonymize.py {json} 0'''.format(json=config)
    executeCommand(command, False)

def executeCommand(command, waiting = True):
    """This function executes a shell command.
    :command:: shell command executed
    :waiting: flag to indicate if the function waits for the command to end"""

    process = subprocess.Popen(command, shell=True, stderr=subprocess.STDOUT)

    if waiting and process.wait() != 0:
         return 1
    return 0
