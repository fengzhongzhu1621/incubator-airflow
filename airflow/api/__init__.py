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

from __future__ import print_function

from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowConfigException
from xTool.exceptions import XToolConfigException
from airflow import configuration as conf
from importlib import import_module

from xTool.utils.log.logging_mixin import LoggingMixin

api_auth = None

log = LoggingMixin().log


def load_auth():
    # 获得默认认证模块
    auth_backend = 'airflow.api.auth.backend.default'
    try:
        auth_backend = conf.get("api", "auth_backend")
    except (AirflowConfigException, XToolConfigException):
        # 没有配置api/auth_backend
        pass

    # 加载认证模块，加载失败抛出异常
    try:
        global api_auth
        api_auth = import_module(auth_backend)
    except ImportError as err:
        log.critical(
            "Cannot import %s for API authentication due to: %s",
            auth_backend, err
        )
        raise AirflowException(err)
