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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import atexit
import logging
import os
import pendulum

from sqlalchemy import create_engine, exc
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from airflow import configuration as conf
from airflow.logging_config import configure_logging
from airflow.utils.helpers import create_statsclient
from airflow.utils.helpers import set_default_timezone
from airflow.exceptions import AirflowConfigException

from xTool.db.alchemy_orm import setup_event_handlers
from xTool.db.alchemy_orm import configure_adapters
from xTool.db import alchemy_orm
from xTool.exceptions import XToolConfigException


log = logging.getLogger(__name__)

RBAC = conf.getboolean('webserver', 'rbac')

# 设置默认时区
TIMEZONE = set_default_timezone()

# 采集到的数据会走 UDP 协议发给 StatsD，由 StatsD 解析、提取、计算处理后，周期性地发送给 Graphite。
Stats = create_statsclient()

HEADER = """\
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
 """

LOGGING_LEVEL = logging.INFO

# the prefix to append to gunicorn worker processes after init
GUNICORN_WORKER_READY_PREFIX = "[ready] "

# 日志格式
LOG_FORMAT = conf.get('core', 'log_format')
SIMPLE_LOG_FORMAT = conf.get('core', 'simple_log_format')

AIRFLOW_HOME = None
SQL_ALCHEMY_CONN = None
DAGS_FOLDER = None

engine = None
Session = None


def policy(task_instance):
    """
    This policy setting allows altering task instances right before they
    are executed. It allows administrator to rewire some task parameters.

    Note that the ``TaskInstance`` object has an attribute ``task`` pointing
    to its related task object, that in turns has a reference to the DAG
    object. So you can use the attributes of all of these to define your
    policy.

    To define policy, add a ``airflow_local_settings`` module
    to your PYTHONPATH that defines this ``policy`` function. It receives
    a ``TaskInstance`` object and can alter it where needed.

    Here are a few examples of how this can be useful:

    * You could enforce a specific queue (say the ``spark`` queue)
        for tasks using the ``SparkOperator`` to make sure that these
        task instances get wired to the right workers
    * You could force all task instances running on an
        ``execution_date`` older than a week old to run in a ``backfill``
        pool.
    * ...
    """
    pass


def configure_vars():
    global AIRFLOW_HOME
    global SQL_ALCHEMY_CONN
    global DAGS_FOLDER
    AIRFLOW_HOME = os.path.expanduser(conf.get('core', 'AIRFLOW_HOME'))
    SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
    DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))


def configure_orm(disable_connection_pool=False):
    pool_enabled = conf.getboolean('core', 'SQL_ALCHEMY_POOL_ENABLED')
    try:
        pool_size = conf.getint('core', 'SQL_ALCHEMY_POOL_SIZE')
    except AirflowConfigException:
        pool_size = 5
    try:
        pool_recycle = conf.getint('core', 'SQL_ALCHEMY_POOL_RECYCLE')
    except AirflowConfigException:
        pool_recycle = 1800
    try:
        encoding = conf.get('core', 'SQL_ENGINE_ENCODING')
    except AirflowConfigException:
        encoding = 'utf-8'
    echo = conf.getboolean('core', 'SQL_ALCHEMY_ECHO')
    reconnect_timeout = conf.getint('core', 'SQL_ALCHEMY_RECONNECT_TIMEOUT')
    autocommit = False
    sql_alchemy_conn = conf.get('core', 'SQL_ALCHEMY_CONN')

    global engine
    global Session
    (engine, Session) = alchemy_orm.configure_orm(sql_alchemy_conn,
                                                  pool_enabled=pool_enabled,
                                                  pool_size=pool_size,
                                                  pool_recycle=pool_recycle,
                                                  reconnect_timeout=reconnect_timeout,
                                                  autocommit=autocommit,
                                                  disable_connection_pool=disable_connection_pool,
                                                  encoding=encoding,
                                                  echo=echo)
    alchemy_orm.Session = Session


def dispose_orm():
    """ Properly close pooled database connections """
    log.debug("Disposing DB connection pool (PID %s)", os.getpid())
    global engine
    global Session

    if Session:
        Session.remove()
        Session = None
    if engine:
        engine.dispose()
        engine = None


def validate_session():
    """验证数据库是否可用 ."""
    try:
        worker_precheck = conf.getboolean('core', 'worker_precheck')
    except AirflowConfigException:
        worker_precheck = False
    return alchemy_orm.validate_session(engine, worker_precheck)


def configure_action_logging():
    """
    Any additional configuration (register callback) for airflow.utils.action_loggers
    module
    :return: None
    """
    pass


try:
    from airflow_local_settings import *  # noqa F403 F401
    log.info("Loaded airflow_local_settings.")
except Exception:
    pass

configure_logging()
configure_vars()
configure_adapters()
# The webservers import this file from models.py with the default settings.
configure_orm()
configure_action_logging()

# Ensure we close DB connections at scheduler and gunicon worker terminations
atexit.register(dispose_orm)

# Const stuff
KILOBYTE = 1024
MEGABYTE = KILOBYTE * KILOBYTE
WEB_COLORS = {'LIGHTBLUE': '#4d9de0',
              'LIGHTORANGE': '#FF9933'}
