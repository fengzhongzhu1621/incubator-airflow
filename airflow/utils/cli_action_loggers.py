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
An Action Logger module. Singleton pattern has been applied into this module
so that registered callbacks can be used all through the same python process.
"""
from __future__ import absolute_import

import logging

import airflow.settings


def register_pre_exec_callback(action_logger):
    """
    Registers more action_logger function callback for pre-execution.
    This function callback is expected to be called with keyword args.
    For more about the arguments that is being passed to the callback,
    refer to airflow.utils.cli.action_logging()
    :param action_logger: An action logger function
    :return: None
    """
    logging.debug("Adding {} to pre execution callback".format(action_logger))
    __pre_exec_callbacks.append(action_logger)


def register_post_exec_callback(action_logger):
    """
    Registers more action_logger function callback for post-execution.
    This function callback is expected to be called with keyword args.
    For more about the arguments that is being passed to the callback,
    refer to airflow.utils.cli.action_logging()
    :param action_logger: An action logger function
    :return: None
    """
    logging.debug("Adding {} to post execution callback".format(action_logger))
    __post_exec_callbacks.append(action_logger)


def on_pre_execution(**kwargs):
    """
    Calls callbacks before execution.
    Note that any exception from callback will be logged but won't be propagated.
    :param kwargs:
    :return: None
    """
    logging.debug("Calling callbacks: {}".format(__pre_exec_callbacks))
    for cb in __pre_exec_callbacks:
        try:
            cb(**kwargs)
        except Exception:
            logging.exception('Failed on pre-execution callback using {}'.format(cb))


def on_post_execution(**kwargs):
    """
    Calls callbacks after execution.
    As it's being called after execution, it can capture status of execution,
    duration, etc. Note that any exception from callback will be logged but
    won't be propagated.
    :param kwargs:
    :return: None
    """
    logging.debug("Calling callbacks: {}".format(__post_exec_callbacks))
    for cb in __post_exec_callbacks:
        try:
            cb(**kwargs)
        except Exception:
            logging.exception('Failed on post-execution callback using {}'.format(cb))


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


__pre_exec_callbacks = []
__post_exec_callbacks = []

# By default, register default action log into pre-execution callback
register_pre_exec_callback(default_action_log)
