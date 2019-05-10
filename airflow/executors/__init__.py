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

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor # noqa
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.sequential_executor import SequentialExecutor
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.module_loading import integrate_plugins
from xTool.utils.module_loading import get_class_from_plugin_module


DEFAULT_EXECUTOR = None
PARALLELISM = configuration.conf.getint('core', 'PARALLELISM')


def _integrate_plugins():
    """Integrate plugins to the context."""
    # 加载插件模块
    from airflow.plugins_manager import executors_modules
    # 将模块加入到系统模块变量中
    integrate_plugins(executors_modules)


def GetDefaultExecutor():
    """Creates a new instance of the configured executor if none exists and returns it"""
    global DEFAULT_EXECUTOR

    if DEFAULT_EXECUTOR is not None:
        return DEFAULT_EXECUTOR

    executor_name = configuration.conf.get('core', 'EXECUTOR')

    DEFAULT_EXECUTOR = _get_executor(executor_name)

    log = LoggingMixin().log
    log.info("Using executor %s", executor_name)

    return DEFAULT_EXECUTOR


class Executors:
    LocalExecutor = "LocalExecutor"
    SequentialExecutor = "SequentialExecutor"
    CeleryExecutor = "CeleryExecutor"
    DaskExecutor = "DaskExecutor"
    MesosExecutor = "MesosExecutor"
    KubernetesExecutor = "KubernetesExecutor"


def _get_executor(executor_name):
    """
    Creates a new instance of the named executor.
    In case the executor name is not know in airflow,
    look for it in the plugins
    """
    if executor_name == Executors.LocalExecutor:
        return LocalExecutor(parallelism=PARALLELISM)
    elif executor_name == Executors.SequentialExecutor:
        return SequentialExecutor(parallelism=PARALLELISM)
    elif executor_name == Executors.CeleryExecutor:
        from airflow.executors.celery_executor import CeleryExecutor
        return CeleryExecutor(parallelism=PARALLELISM)
    elif executor_name == Executors.DaskExecutor:
        from airflow.executors.dask_executor import DaskExecutor
        return DaskExecutor(parallelism=PARALLELISM)
    elif executor_name == Executors.MesosExecutor:
        from airflow.contrib.executors.mesos_executor import MesosExecutor
        return MesosExecutor(parallelism=PARALLELISM)
    elif executor_name == Executors.KubernetesExecutor:
        from airflow.contrib.executors.kubernetes_executor import KubernetesExecutor
        return KubernetesExecutor(parallelism=PARALLELISM)
    else:
        # Loading plugins
        _integrate_plugins()
        # 从插件模块中获取指定类
        args = []
        kwargs = {'parallelism': PARALLELISM}
        return get_class_from_plugin_module(executor_name, *args, **kwargs)
