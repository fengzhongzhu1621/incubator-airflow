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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from future.standard_library import install_aliases

from builtins import str, object, bytes, ImportError as BuiltinImportError
import copy
from collections import namedtuple, defaultdict
from datetime import timedelta

import dill
import functools
import getpass
import imp
import importlib
import itertools
import zipfile
import jinja2
import json
import logging
import numbers
import os
import pickle
import re
import signal
import sys
import textwrap
import traceback
import warnings
import hashlib

import uuid
from datetime import datetime
from urllib.parse import urlparse, quote, parse_qsl

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float, LargeBinary, UniqueConstraint)
from sqlalchemy import func, or_, and_, true as sqltrue
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import reconstructor, relationship, synonym

from croniter import (
    croniter, CroniterBadCronError, CroniterBadDateError, CroniterNotAlphaError
)
import six
from xTool.utils.timeout import timeout
from xTool.exceptions import XToolTimeoutError
from xTool.exceptions import XToolException
from xTool.utils.file import list_py_file_paths

from airflow import settings, utils
from airflow.executors import GetDefaultExecutor, LocalExecutor
from airflow import configuration
from airflow.exceptions import AirflowConfigException
from xTool.exceptions import XToolConfigException
from airflow import configuration as conf
from airflow.exceptions import (
    AirflowDagCycleException, AirflowException, AirflowSkipException
)
from airflow.dag.base_dag import BaseDag, BaseDagBag
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep

from airflow.models.dagpickle import DagPickle
from airflow.models.errors import ImportError
from airflow.models.kubernetes import KubeWorkerIdentifier, KubeResourceVersion
from airflow.models.log import Log
from airflow.models.slamiss import SlaMiss
from airflow.models.taskfail import TaskFail
from airflow.models.pool import Pool
from airflow.models.xcom import XCom
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.models.dagstat import DagStat
from airflow.models.variable import Variable
from airflow.models.knownevent import KnownEventType, KnownEvent
from airflow.models.chart import Chart
from airflow.models.dag import DAG
from airflow.models.dagmodel import DagModel
from airflow.models.baseoperator import BaseOperator
from airflow.models.user import User
from airflow.models.dagbag import DagBag

from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS
from xTool.utils.dates import cron_presets, date_range as utils_date_range
from xTool.decorators.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.email import send_email
from xTool.utils.helpers import pprinttable
from xTool.utils.helpers import as_tuple
from xTool.utils.helpers import is_container
from xTool.utils.helpers import validate_key
from xTool.utils.helpers import ask_yesno
from xTool.utils.operator_resources import Resources
from xTool.utils.state import State
from xTool.rules.trigger_rule import TriggerRule
from xTool.rules.weight_rule import WeightRule
from xTool.utils.net import get_hostname
from xTool.utils.log.logging_mixin import LoggingMixin

from xTool.misc import USE_WINDOWS


install_aliases()

Base = declarative_base()
# 主键ID的长度
ID_LEN = 250
# 中间表的默认key
XCOM_RETURN_KEY = 'return_value'

Stats = settings.Stats


class InvalidFernetToken(Exception):
    # If Fernet isn't loaded we need a valid exception class to catch. If it is
    # loaded this will get reset to the actual class once get_fernet() is called
    pass


class NullFernet(object):
    """
    A "Null" encryptor class that doesn't encrypt or decrypt but that presents
    a similar interface to Fernet.

    The purpose of this is to make the rest of the code not have to know the
    difference, and to only display the message once, not 20 times when
    `airflow initdb` is ran.
    """
    is_encrypted = False

    def decrpyt(self, b):
        return b

    def encrypt(self, b):
        return b


_fernet = None


def get_fernet():
    """
    Deferred load of Fernet key.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: AirflowException if there's a problem trying to load Fernet
    """
    global _fernet
    log = LoggingMixin().log

    if _fernet:
        return _fernet
    try:
        from cryptography.fernet import Fernet, InvalidToken
        global InvalidFernetToken
        InvalidFernetToken = InvalidToken

    except BuiltinImportError:
        log.warning(
            "cryptography not found - values will not be stored encrypted."
        )
        _fernet = NullFernet()
        return _fernet

    try:
        fernet_key = configuration.conf.get('core', 'FERNET_KEY')
        if not fernet_key:
            log.warning(
                "empty cryptography key - values will not be stored encrypted."
            )
            _fernet = NullFernet()
        else:
            _fernet = Fernet(fernet_key.encode('utf-8'))
            _fernet.is_encrypted = True
    except (ValueError, TypeError) as ve:
        raise AirflowException("Could not create Fernet object: {}".format(ve))

    return _fernet


# Used by DAG context_managers
_CONTEXT_MANAGER_DAG = None


def clear_task_instances(tis,
                         session,
                         activate_dag_runs=True,
                         dag=None,
                         ):
    """重置任务实例
    - 正在运行的任务改为关闭状态，相关的job设置为关闭状态
    - 非正在运行的任务改为 None 状态，并修改任务实例的最大重试次数 max_tries
    - 任务相关的dagrun设置为RUNNING状态

    Clears a set of task instances, but makes sure the running ones
    get killed.

    :param tis: a list of task instances
    :param session: current session
    :param activate_dag_runs: flag to check for active dag run
    :param dag: DAG object
    """
    job_ids = []
    for ti in tis:
        if ti.state == State.RUNNING:
            # 正在运行的任务是实例，则设置为关闭状态
            if ti.job_id:
                # 将任务实例状态改为SHUTDOWN
                ti.state = State.SHUTDOWN
                # 记录job实例
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                # 获得任务
                task = dag.get_task(task_id)
                # 获得任务的重试次数
                task_retries = task.retries
                # 重新设置任务实例的最大重试次数
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # DB中存在任务实例，但是dag中可能变更了代码，导致dag中不存在此任务了
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the current task try number.
                ti.max_tries = max(ti.max_tries, ti.try_number - 1)
            # 将任务设置为None
            ti.state = State.NONE
            # 立即变更任务实例在DB中的状态
            session.merge(ti)

    # 将正在运行的任务实例，关联的job设置为关闭状态
    if job_ids:
        from airflow.jobs import BaseJob as BJ
        for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
            job.state = State.SHUTDOWN

    # 将dagrun重置为运行状态
    if activate_dag_runs and tis:
        drs = session.query(DagRun).filter(
            DagRun.dag_id.in_({ti.dag_id for ti in tis}),
            DagRun.execution_date.in_({ti.execution_date for ti in tis}),
        ).all()
        for dr in drs:
            # 设置为运行态，并重置开始时间
            dr.state = State.RUNNING
            dr.start_date = datetime.now()


# To avoid circular import on Python2.7 we need to define this at the _bottom_
from airflow.models.connection import Connection  # noqa: E402,F401
from airflow.models.skipmixin import SkipMixin  # noqa: F401
