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
from airflow.models.dagstate import DagStat
from airflow.models.variable import Variable
from airflow.models.knownevent import KnownEventType, KnownEvent
from airflow.models.chart import Chart
from airflow.models.dag import DAG
from airflow.models.dagmodel import DagModel
from airflow.models.baseoperator import BaseOperator
from airflow.models.user import User

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


class DagBag(BaseDagBag, LoggingMixin):
    """
    A dagbag is a collection of dags, parsed out of a folder tree and has high
    level configuration settings, like what database to use as a backend and
    what executor to use to fire off tasks. This makes it easier to run
    distinct environments for say production and development, tests, or for
    different teams or security profiles. What would have been system level
    settings are now dagbag level so that one system can run multiple,
    independent settings sets.

    :param dag_folder: the folder to scan to find DAGs
    :type dag_folder: unicode
    :param executor: the executor to use when executing task instances
        in this DagBag
    :param include_examples: whether to include the examples that ship
        with airflow or not
    :type include_examples: bool
    :param has_logged: an instance boolean that gets flipped from False to True after a
        file has been skipped. This is to prevent overloading the user with logging
        messages about skipped files. Therefore only once per DagBag is a file logged
        being skipped.
    """

    # static class variables to detetct dag cycle
    # 用于检测DAG是否有环
    CYCLE_NEW = 0
    CYCLE_IN_PROGRESS = 1
    CYCLE_DONE = 2

    def __init__(
            self,
            dag_folder=None,
            executor=None,
            include_examples=configuration.conf.getboolean('core', 'LOAD_EXAMPLES')):

        # do not use default arg in signature, to fix import cycle on plugin load
        if executor is None:
            executor = GetDefaultExecutor()
        dag_folder = dag_folder or settings.DAGS_FOLDER
        self.log.info("Filling up the DagBag from %s", dag_folder)
        self.dag_folder = dag_folder
        self.dags = {}
        # the file's last modified timestamp when we last read it
        # 记录文件的最后修改时间
        self.file_last_changed = {}
        self.executor = executor
        self.import_errors = {}
        self.has_logged = False

        # 从目录中加载dag
        if include_examples:
            example_dag_folder = os.path.join(
                os.path.dirname(__file__),
                'example_dags')
            self.collect_dags(example_dag_folder)
        self.collect_dags(dag_folder)

    def size(self):
        """
        :return: the amount of dags contained in this dagbag
        """
        return len(self.dags)

    def get_dag(self, dag_id):
        """
        Gets the DAG out of the dictionary, and refreshes it if expired
        """
        # If asking for a known subdag, we want to refresh the parent
        root_dag_id = dag_id
        if dag_id in self.dags:
            dag = self.dags[dag_id]
            if dag.is_subdag:
                root_dag_id = dag.parent_dag.dag_id

        # If the dag corresponding to root_dag_id is absent or expired
        # 从DB中获取dag orm model
        orm_dag = DagModel.get_current(root_dag_id)
        
        # 如果用户在界面上点击了刷新按钮，会将当前时间记录到last_expired字段
        # 如果刷新时间大于上次的文件加载时间，则重新加载文件
        # 这样就不需要等到下一次
        if orm_dag and (
                root_dag_id not in self.dags or
                (
                    orm_dag.last_expired and
                    dag.last_loaded < orm_dag.last_expired
                )
        ):
            # Reprocess source file
            found_dags = self.process_file(
                filepath=orm_dag.fileloc, only_if_updated=False)

            # If the source file no longer exports `dag_id`, delete it from self.dags
            if found_dags and dag_id in [found_dag.dag_id for found_dag in found_dags]:
                return self.dags[dag_id]
            elif dag_id in self.dags:
                del self.dags[dag_id]
        
        # 返回最新的dag，因为此dag可能会重新加载
        return self.dags.get(dag_id)

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """根据文件的修改时间重新加载文件，
        Given a path to a python module or zip file, this method imports
        the module and look for dag objects within it.
        """
        found_dags = []

        # if the source file no longer exists in the DB or in the filesystem,
        # return an empty list
        # todo: raise exception?
        if filepath is None or not os.path.isfile(filepath):
            return found_dags

        try:
            # This failed before in what may have been a git sync
            # race condition
            # 获得文件的修改时间
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(filepath))
            # 判断文件最近是否被修改
            if only_if_updated \
                    and filepath in self.file_last_changed \
                    and file_last_changed_on_disk == self.file_last_changed[filepath]:
                return found_dags

        except Exception as e:
            self.log.exception(e)
            return found_dags

        mods = []
        is_zipfile = zipfile.is_zipfile(filepath)
        if not is_zipfile:
            # 判断文件是否是可解析的DAG文件
            if safe_mode and os.path.isfile(filepath):
                with open(filepath, 'rb') as f:
                    content = f.read()
                    if not all([s in content for s in (b'DAG', b'airflow')]):
                        self.file_last_changed[filepath] = file_last_changed_on_disk
                        # Don't want to spam user with skip messages
                        if not self.has_logged:
                            self.has_logged = True
                            self.log.info(
                                "File %s assumed to contain no DAGs. Skipping.",
                                filepath)
                        return found_dags

            self.log.debug("Importing %s", filepath)
            # 获得文件名和后缀名
            org_mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
            mod_name = ('unusual_prefix_' +
                        hashlib.sha1(filepath.encode('utf-8')).hexdigest() +
                        '_' + org_mod_name)

            if mod_name in sys.modules:
                del sys.modules[mod_name]

            # 导入DAG文件
            with timeout(configuration.conf.getint('core', "DAGBAG_IMPORT_TIMEOUT")):
                try:
                    m = imp.load_source(mod_name, filepath)
                    mods.append(m)
                except Exception as e:
                    self.log.exception("Failed to import: %s", filepath)
                    self.import_errors[filepath] = str(e)
                    self.file_last_changed[filepath] = file_last_changed_on_disk

        else:
            zip_file = zipfile.ZipFile(filepath)
            for mod in zip_file.infolist():
                head, _ = os.path.split(mod.filename)
                mod_name, ext = os.path.splitext(mod.filename)
                if not head and (ext == '.py' or ext == '.pyc'):
                    if mod_name == '__init__':
                        self.log.warning("Found __init__.%s at root of %s", ext, filepath)
                    if safe_mode:
                        with zip_file.open(mod.filename) as zf:
                            self.log.debug("Reading %s from %s", mod.filename, filepath)
                            content = zf.read()
                            if not all([s in content for s in (b'DAG', b'airflow')]):
                                self.file_last_changed[filepath] = (
                                    file_last_changed_on_disk)
                                # todo: create ignore list
                                # Don't want to spam user with skip messages
                                if not self.has_logged:
                                    self.has_logged = True
                                    self.log.info(
                                        "File %s assumed to contain no DAGs. Skipping.",
                                        filepath)

                    if mod_name in sys.modules:
                        del sys.modules[mod_name]

                    try:
                        sys.path.insert(0, filepath)
                        m = importlib.import_module(mod_name)
                        mods.append(m)
                    except Exception as e:
                        self.log.exception("Failed to import: %s", filepath)
                        self.import_errors[filepath] = str(e)
                        self.file_last_changed[filepath] = file_last_changed_on_disk

        # 遍历加载的模块
        for m in mods:
            for dag in list(m.__dict__.values()):
                if isinstance(dag, DAG):
                    if not dag.full_filepath:
                        dag.full_filepath = filepath
                        if dag.fileloc != filepath and not is_zipfile:
                            dag.fileloc = filepath
                    try:
                        # 将dag重新加入到self.dags中
                        dag.is_subdag = False
                        self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                        if isinstance(dag._schedule_interval, six.string_types):
                            croniter(dag._schedule_interval)
                        found_dags.append(dag)
                        found_dags += dag.subdags
                    except (CroniterBadCronError,
                            CroniterBadDateError,
                            CroniterNotAlphaError) as cron_e:
                        self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                        self.import_errors[dag.full_filepath] = \
                            "Invalid Cron expression: " + str(cron_e)
                        self.file_last_changed[dag.full_filepath] = \
                            file_last_changed_on_disk
                    except AirflowDagCycleException as cycle_exception:
                        self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                        self.import_errors[dag.full_filepath] = str(cycle_exception)
                        self.file_last_changed[dag.full_filepath] = \
                            file_last_changed_on_disk
        # 记录所 加载文件的最近修改时间
        self.file_last_changed[filepath] = file_last_changed_on_disk
        return found_dags

    @provide_session
    def kill_zombies(self, session=None):
        """
        Fails tasks that haven't had a heartbeat in too long
        """
        secs = configuration.conf.getint('scheduler', 'scheduler_zombie_task_threshold')
        now = datetime.now()
        limit_dttm = now - timedelta(seconds=secs)
        self.log.info("Finding 'running' jobs without a recent heartbeat after %s", limit_dttm)

        # 任务实例正在运行，但是job已经停止，或者job的心跳长时间未更新
        begin_time = now - timedelta(days=configuration.conf.getint('core', 'sql_query_history_days'))
        TI = TaskInstance
        from airflow.jobs import LocalTaskJob as LJ
        # SELECT task_instance.try_number AS task_instance_try_number, task_instance.task_id AS task_instance_task_id, task_instance.dag_id AS task_instance_dag_id, task_instance.execution_date AS task_instance_execution_date, task_instance.start_date AS task_instance_start_date, task_instance.end_date AS task_instance_end_date, task_instance.duration AS task_instance_duration, task_instance.state AS task_instance_state, task_instance.max_tries AS task_instance_max_tries, task_instance.hostname AS task_instance_hostname, task_instance.unixname AS task_instance_unixname, task_instance.job_id AS task_instance_job_id, task_instance.pool AS task_instance_pool, task_instance.queue AS task_instance_queue, task_instance.priority_weight AS task_instance_priority_weight, task_instance.operator AS task_instance_operator, task_instance.queued_dttm AS task_instance_queued_dttm, task_instance.pid AS task_instance_pid, task_instance.executor_config AS task_instance_executor_config 
        # FROM task_instance INNER JOIN job ON task_instance.job_id = job.id AND job.job_type IN (LocalTaskJob) 
        # WHERE task_instance.execution_date > %s AND task_instance.state = 'running' AND (job.latest_heartbeat < %s OR job.state != 'running')
        tis = (
            session.query(TI)
            .join(LJ, TI.job_id == LJ.id)
            .filter(TI.execution_date > begin_time)
            .filter(TI.state == State.RUNNING)  # 任务实例正在运行
            .filter(
                or_(
                    LJ.latest_heartbeat < limit_dttm,   # 没有上报心跳
                    LJ.state != State.RUNNING,  # 但是job不是运行态
                ))
            .all()
        )

        for ti in tis:
            if ti and ti.dag_id in self.dags:
                # 根据任务实例获取dag
                dag = self.dags[ti.dag_id]
                # 获得dag中所有的任务
                if ti.task_id in dag.task_ids:
                    # 获得任务模型
                    task = dag.get_task(ti.task_id)

                    # now set non db backed vars on ti
                    ti.task = task
                    ti.test_mode = configuration.getboolean('core', 'unit_test_mode')
                    # 任务执行失败，发送通知，并执行失败回调函数
                    ti.handle_failure("{} detected as zombie".format(ti),
                                      ti.test_mode, ti.get_template_context())
                    self.log.info(
                        'Marked zombie job %s as %s', ti, ti.state)
                    Stats.incr('zombies_killed')
        session.commit()

    def bag_dag(self, dag, parent_dag, root_dag):
        """
        Adds the DAG into the bag, recurses into sub dags.
        Throws AirflowDagCycleException if a cycle is detected in this dag or its subdags
        """
        # 测试是否有环
        dag.test_cycle()  # throws if a task cycle is found
        # 从指定的属性中获取设置的模板文件名，渲染模板并将此属性的值设置为渲染后的模板的内容
        dag.resolve_template_files()
        dag.last_loaded = datetime.now()

        # 通用预处理
        for task in dag.tasks:
            settings.policy(task)

        subdags = dag.subdags

        try:
            for subdag in subdags:
                subdag.full_filepath = dag.full_filepath
                subdag.parent_dag = dag
                subdag.is_subdag = True
                self.bag_dag(subdag, parent_dag=dag, root_dag=root_dag)

            # 将新的dag加入到self.dags中
            self.dags[dag.dag_id] = dag
            self.log.debug('Loaded DAG {dag}'.format(**locals()))
        except AirflowDagCycleException as cycle_exception:
            # There was an error in bagging the dag. Remove it from the list of dags
            self.log.exception('Exception bagging dag: {dag.dag_id}'.format(**locals()))
            # Only necessary at the root level since DAG.subdags automatically
            # performs DFS to search through all subdags
            if dag == root_dag:
                for subdag in subdags:
                    if subdag.dag_id in self.dags:
                        del self.dags[subdag.dag_id]
            raise cycle_exception

    def collect_dags(
            self,
            dag_folder=None,
            only_if_updated=True):
        """根据dag文件夹路径加载dag
        Given a file path or a folder, this method looks for python modules,
        imports them and adds them to the dagbag collection.

        Note that if a ``.airflowignore`` file is found while processing
        the directory, it will behave much like a ``.gitignore``,
        ignoring files that match any of the regex patterns specified
        in the file.

        **Note**: The patterns in .airflowignore are treated as
        un-anchored regexes, not shell-like glob patterns.
        """
        start_dttm = datetime.now()
        dag_folder = dag_folder or self.dag_folder

        # Used to store stats around DagBag processing
        stats = []
        FileLoadStat = namedtuple(
            'FileLoadStat', "file duration dag_num task_num dags")
            
        known_file_paths = list_py_file_paths(dag_folder,
                                              ignore_filename=".airflowignore",
                                              safe_mode=True,
                                              safe_filters=(b'DAG', b'airflow'))
        for filepath in known_file_paths:
            try:
                ts = datetime.now()
                found_dags = self.process_file(
                    filepath, only_if_updated=only_if_updated)

                td = datetime.now() - ts
                td = td.total_seconds() + (
                    float(td.microseconds) / 1000000)
                stats.append(FileLoadStat(
                    filepath.replace(dag_folder, ''),
                    td,
                    len(found_dags),
                    sum([len(dag.tasks) for dag in found_dags]),
                    str([dag.dag_id for dag in found_dags]),
                ))
            except Exception as e:
                self.log.exception(e)
        Stats.gauge(
            'collect_dags', (datetime.now() - start_dttm).total_seconds(), 1)
        Stats.gauge(
            'dagbag_size', len(self.dags), 1)
        Stats.gauge(
            'dagbag_import_errors', len(self.import_errors), 1)
        self.dagbag_stats = sorted(
            stats, key=lambda x: x.duration, reverse=True)

    def dagbag_report(self):
        """Prints a report around DagBag loading stats"""
        report = textwrap.dedent("""\n
        -------------------------------------------------------------------
        DagBag loading stats for {dag_folder}
        -------------------------------------------------------------------
        Number of DAGs: {dag_num}
        Total task number: {task_num}
        DagBag parsing time: {duration}
        {table}
        """)
        stats = self.dagbag_stats
        return report.format(
            dag_folder=self.dag_folder,
            duration=sum([o.duration for o in stats]),
            dag_num=sum([o.dag_num for o in stats]),
            task_num=sum([o.task_num for o in stats]),
            table=pprinttable(stats),
        )

    @provide_session
    def deactivate_inactive_dags(self, session=None):
        """删除非活动的dag ."""
        active_dag_ids = [dag.dag_id for dag in list(self.dags.values())]
        for dag in session.query(
                DagModel).filter(~DagModel.dag_id.in_(active_dag_ids)).all():
            dag.is_active = False
            session.merge(dag)
        session.commit()

    @provide_session
    def paused_dags(self, session=None):
        """获得所有已暂停的dag ."""
        dag_ids = [dp.dag_id for dp in session.query(DagModel).filter(
            DagModel.is_paused.__eq__(True))]
        return dag_ids


# To avoid circular import on Python2.7 we need to define this at the _bottom_
from airflow.models.connection import Connection  # noqa: E402,F401
from airflow.models.skipmixin import SkipMixin  # noqa: F401
