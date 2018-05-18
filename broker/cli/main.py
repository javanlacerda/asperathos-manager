# Copyright (c) 2017 LSD - UFCG.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from flask import Flask
from broker.api.v10 import rest
from broker.plugins import base as plugin_base
from broker.service import api


def main():
    plugin_base.setup_plugins()
    app = Flask(__name__)
    app.register_blueprint(rest)
    app.run(host='0.0.0.0', port=api.port, debug=True)