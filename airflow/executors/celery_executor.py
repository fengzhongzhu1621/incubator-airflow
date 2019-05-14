# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import subprocess
import os

from celery import Celery
from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG
from airflow.exceptions import AirflowException
from airflow import configuration
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.module_loading import import_string
from xTool.executors.celery_executor import CeleryExecutor

'''
To start the celery worker, run the command:
airflow worker
'''

# 获得配置文件的路径，并导入celery默认配置
if configuration.conf.has_option('celery', 'celery_config_options'):
    celery_configuration = import_string(
        configuration.conf.get('celery', 'celery_config_options')
    )
else:
    celery_configuration = DEFAULT_CELERY_CONFIG

# 创建一个celery客户端
celery_app_name = configuration.conf.get('celery', 'CELERY_APP_NAME')
app = Celery(
    celery_app_name,
    config_source=celery_configuration)


@app.task
def execute_command(command):
    """执行shell命令 ."""
    log = LoggingMixin().log
    log.info("Executing command in Celery: %s", command)
    env = os.environ.copy()
    try:
        subprocess.check_call(command, shell=True, stderr=subprocess.STDOUT,
                              close_fds=True, env=env)
    except subprocess.CalledProcessError as e:
        log.exception('execute_command encountered a CalledProcessError')
        log.error(e.output)

        raise AirflowException('Celery command failed')
