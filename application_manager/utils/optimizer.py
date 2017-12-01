# Copyright (c) 2017 UFCG-LSD.
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

import json
import requests


def _get_optimizer_data(hosts, percentage):
    optimizer_dict = {
        'hosts': hosts,
        'percentage': percentage
    }
    optimizer_body = json.dumps(optimizer_dict)

    return optimizer_body


def get_cluster_size(optimizer_url, hosts, percentage):
    request_url = optimizer_url + '/get_cluster_size'
    headers = {'Content-type': 'application/json'}
    data = _get_optimizer_data(hosts, percentage)
    request = requests.get(request_url, data=data, headers=headers)
    data = request.json()

    return data['cluster_size']


def get_info(optimizer_url, expected_time):
    expected_ms_time = expected_time * 1000
    request_url = optimizer_url + ('/bigsea/rest/ws/resopt/bulma/5/%s' % 
                                   expected_ms_time)

    headers = {'Content-type': 'application/json'}
    request = requests.get(request_url, headers=headers)

    data = request.text.split()
    cores = int(data[0])
    vms = int(data[1])

    return cores, vms
