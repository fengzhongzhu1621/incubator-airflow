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
#
"""
Utilities module for cli
"""
from __future__ import absolute_import

import functools
import getpass
import json
import socket
import sys
from argparse import Namespace
from datetime import datetime

import airflow.models
import airflow.settings

from xTool.utils.cli import (on_pre_execution,
                             on_post_execution,
                             register_pre_exec_callback)
from xTool.misc import get_local_host_ip


def default_action_log(log, **_):
    """
    A default action logger callback that behave same as www.utils.action_logging
    which uses global session and pushes log ORM object.
    :param log: An log ORM instance
    :param **_: other keyword arguments that is not being used by this function
    :return: None
    """
    # 将日志记录到数据库中
    session = airflow.settings.Session()
    session.add(log)
    session.commit()


# By default, register default action log into pre-execution callback
register_pre_exec_callback(default_action_log)


def action_logging(f):
    """
    Decorates function to execute function at the same time submitting action_logging
    but in CLI context. It will call action logger callbacks twice,
    one for pre-execution and the other one for post-execution.

    Action logger will be called with below keyword parameters:
        sub_command : name of sub-command
        start_datetime : start datetime instance by utc
        end_datetime : end datetime instance by utc
        full_command : full command line arguments
        user : current user
        log : airflow.models.Log ORM instance
        dag_id : dag id (optional)
        task_id : task_id (optional)
        execution_date : execution date (optional)
        error : exception instance if there's an exception

    :param f: function instance
    :return: wrapped function
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        """
        An wrapper for cli functions. It assumes to have Namespace instance
        at 1st positional argument
        :param args: Positional argument. It assumes to have Namespace instance
        at 1st positional argument
        :param kwargs: A passthrough keyword argument
        """
        # 第一个参数必须是argparse命名空间实例
        assert args
        assert isinstance(args[0], Namespace), \
            "1st positional argument should be argparse.Namespace instance, " \
            "but {}".format(args[0])
        # 创建命令行参数的上下文
        metrics = _build_metrics(f.__name__, args[0])
        on_pre_execution(**metrics)
        # 执行命令行
        try:
            return f(*args, **kwargs)
        except Exception as e:
            metrics['error'] = e
            raise
        finally:
            # 记录命令行结束时间
            metrics['end_datetime'] = datetime.now()
            on_post_execution(**metrics)

    return wrapper


def _build_metrics(func_name, namespace):
    """
    Builds metrics dict from function args
    It assumes that function arguments is from airflow.bin.cli module's function
    and has Namespace instance where it optionally contains "dag_id", "task_id",
    and "execution_date".

    :param func_name: name of function
    :param namespace: Namespace instance from argparse
    :return: dict with metrics
    """

    metrics = {'sub_command': func_name}
    metrics['start_datetime'] = datetime.now()
    metrics['full_command'] = '{}'.format(list(sys.argv))
    metrics['user'] = getpass.getuser()

    assert isinstance(namespace, Namespace)
    tmp_dic = vars(namespace)
    metrics['dag_id'] = tmp_dic.get('dag_id')
    metrics['task_id'] = tmp_dic.get('task_id')
    metrics['execution_date'] = tmp_dic.get('execution_date')
    # TODO 记录IP地址
    metrics['host_name'] = get_local_host_ip()

    extra = json.dumps(dict((k, metrics[k]) for k in ('host_name', 'full_command')))

    # 构造需要记录在DB中的日志
    log = airflow.models.Log(
        event='cli_{}'.format(func_name),
        task_instance=None,
        owner=metrics['user'],
        extra=extra,
        task_id=metrics.get('task_id'),
        dag_id=metrics.get('dag_id'),
        execution_date=metrics.get('execution_date'))
    metrics['log'] = log
    return metrics
