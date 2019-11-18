# Copyright (c) 2017 UPV-GryCAP & UFCG-LSD.
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

import datetime
import json
import redis
import requests
import six
import threading
import time
import uuid

from broker.service import api
from broker.plugins import base
from broker.persistence.etcd_db import plugin as etcd
from broker.persistence.sqlite import plugin as sqlite
from broker.utils import ids
from broker.utils import logger
from broker.utils.plugins import k8s
from broker.utils.framework import monitor, controller, visualizer
from broker import exceptions as ex


KUBEAPPS_LOG = logger.Log("KubeAppsPlugin", "logs/kubeapps.log")
application_time_log = \
    logger.Log("Application_time", "logs/application_time.log")


class KubeAppsExecutor(base.GenericApplicationExecutor):

    def __init__(self, app_id,
                 data=None, url_address='Url is not avaliable yet!'):

        self.id = ids.ID_Generator().get_ID()
        self.app_id = app_id
        self.k8s = k8s
        self.data = data
        self.url_address = url_address

    def __repr__(self):

        representation = {
            "app_id": self.app_id,
            "url_address": self.url_address
        }
        return json.dumps(representation)

    def start_application(self, data):
        # TODO: Validate data entry
        data.update({'app_id': self.app_id})
        self.activate_related_cluster(data)
        self.update_env_vars(data)

        app_port, code_from, init_size, env_vars = self.get_args(data)
        if code_from.lower() == 'git':
            git_address = data.get('git_address')
            self.url_address = \
                k8s.deploy_app_from_git(self.app_id, app_port,
                                          git_address, init_size, env_vars)
        else:
            img = data.get('img')
            self.url_address = \
                k8s.deploy_app_from_image(self.app_id, app_port,
                                          img, init_size, env_vars)

    def stop_application(self):
        # TODO: Validate data entry
        self.url_address = k8s.stop_app(self.app_id)
        
    def terminate_job(self):
        # TODO: Validate data entry
        k8s.terminate_app(self.app_id)
        self.url_address = "terminated!"

    def get_args(self, kwargs):
        
        app_port = kwargs.get('port')
        code_from = kwargs.get('code_from')
        init_size = kwargs.get('init_size')
        env_vars = kwargs.get('env_vars')

        return app_port, code_from, init_size, env_vars

    def activate_related_cluster(self, data):
        # If the cluster name is informed in data, active the cluster
        if('cluster_name' in data.keys()):
            api.v10.activate_cluster(data['cluster_name'], data)

    def update_env_vars(self, data):
        # inject REDIS_HOST in the environment
        data['env_vars']['REDIS_HOST'] = 'redis-%s' % self.app_id

        # inject SCONE_CONFIG_ID in the environment
        config_id = data.get('config_id')
        if config_id is not None:
            KUBEAPPS_LOG.log("You should pass the config_id variable "
                             "through the env_var parameter. Passing this way"
                             "is deprecated.")
            data['env_vars']['SCONE_CONFIG_ID'] = config_id

    def synchronize(self):
        pass

    def validate(self, data):
        data_model = {
            "cmd": list,
            "control_parameters": dict,
            "control_plugin": six.string_types,
            "env_vars": dict,
            "img": six.string_types,
            "init_size": int,
            "monitor_info": dict,
            "monitor_plugin": six.string_types,
            "redis_workload": six.string_types,
            "enable_visualizer": bool
            # The parameters below are only needed if enable_visualizer is True
            # "visualizer_plugin": six.string_types
            # "visualizer_info":dict
        }

        for key in data_model:
            if (key not in data):
                raise ex.BadRequestException(
                    "Variable \"{}\" is missing".format(key))
            if (not isinstance(data[key], data_model[key])):
                raise ex.BadRequestException(
                    "\"{}\" has unexpected variable type: {}. Was expecting {}"
                    .format(key, type(data[key]), data_model[key]))

        if (data["enable_visualizer"]):
            if ("visualizer_plugin" not in data):
                raise ex.BadRequestException(
                    "Variable \"visualizer_plugin\" is missing")

            if (not isinstance(data["visualizer_plugin"], six.string_types)):
                raise ex.BadRequestException(
                    "\"visualizer_plugin\" has unexpected variable type: {}.\
                     Was expecting {}"
                    .format(type(data["visualizer_plugin"]),
                            data_model["visualizer_plugin"]))

            if ("visualizer_info" not in data):
                raise ex.BadRequestException(
                    "Variable \"visualizer_info\" is missing")

            if (not isinstance(data["visualizer_info"], dict)):
                raise ex.BadRequestException(
                    "\"visualizer_info\" has unexpected variable type: {}.\
                    Was expecting {}"
                    .format(type(data["visualizer_info"]),
                            data_model["visualizer_info"]))

        if (not data["init_size"] > 0):
            raise ex.BadRequestException(
                "Variable \"init_size\" should be greater than 0")


class KubeAppsProvider(base.PluginInterface):

    def __init__(self):
        self.id_generator = ids.ID_Generator()

    def get_title(self):
        return 'Kubernetes App Deploy Plugin'

    def get_description(self):
        return ('Plugin that allows utilization of '
                'Batch Jobs over a k8s cluster')

    def to_dict(self):
        return {
            'name': self.name,
            'title': self.get_title(),
            'description': self.get_description(),
        }

    def execute(self, data):
        app_id = 'ka-' + str(uuid.uuid4())[0:7]
        executor = KubeAppsExecutor(app_id)

        handling_thread = threading.Thread(target=executor.start_application,
                                           args=(data,))
        handling_thread.start()
        return app_id, executor

PLUGIN = KubeAppsProvider
