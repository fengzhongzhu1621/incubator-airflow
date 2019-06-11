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


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(ID_LEN), unique=True)
    email = Column(String(500))
    superuser = False

    def __repr__(self):
        return self.username

    def get_id(self):
        return str(self.id)

    def is_superuser(self):
        return self.superuser


@functools.total_ordering
class BaseOperator(LoggingMixin):
    """
    Abstract base class for all operators. Since operators create objects that
    become nodes in the dag, BaseOperator contains many recursive methods for
    dag crawling behavior. To derive this class, you are expected to override
    the constructor as well as the 'execute' method.

    Operators derived from this class should perform or trigger certain tasks
    synchronously (wait for completion). Example of operators could be an
    operator that runs a Pig job (PigOperator), a sensor operator that
    waits for a partition to land in Hive (HiveSensorOperator), or one that
    moves data from Hive to MySQL (Hive2MySqlOperator). Instances of these
    operators (tasks) target specific operations, running specific scripts,
    functions or data transfers.

    This class is abstract and shouldn't be instantiated. Instantiating a
    class derived from this one results in the creation of a task object,
    which ultimately becomes a node in DAG objects. Task dependencies should
    be set by using the set_upstream and/or set_downstream methods.

    :param task_id: a unique, meaningful id for the task
    :type task_id: string
    :param owner: the owner of the task, using the unix username is recommended
    :type owner: string
    :param retries: the number of retries that should be performed before
        failing the task
    :type retries: int
    :param retry_delay: delay between retries
    :type retry_delay: timedelta
    :param retry_exponential_backoff: allow progressive longer waits between
        retries by using exponential backoff algorithm on retry delay (delay
        will be converted into seconds)
    :type retry_exponential_backoff: bool
    :param max_retry_delay: maximum delay interval between retries
    :type max_retry_delay: timedelta
    :param start_date: The ``start_date`` for the task, determines
        the ``execution_date`` for the first task instance. The best practice
        is to have the start_date rounded
        to your DAG's ``schedule_interval``. Daily jobs have their start_date
        some day at 00:00:00, hourly jobs have their start_date at 00:00
        of a specific hour. Note that Airflow simply looks at the latest
        ``execution_date`` and adds the ``schedule_interval`` to determine
        the next ``execution_date``. It is also very important
        to note that different tasks' dependencies
        need to line up in time. If task A depends on task B and their
        start_date are offset in a way that their execution_date don't line
        up, A's dependencies will never be met. If you are looking to delay
        a task, for example running a daily task at 2AM, look into the
        ``TimeSensor`` and ``TimeDeltaSensor``. We advise against using
        dynamic ``start_date`` and recommend using fixed ones. Read the
        FAQ entry about start_date for more information.
    :type start_date: datetime
    :param end_date: if specified, the scheduler won't go beyond this date
    :type end_date: datetime
    :param depends_on_past: when set to true, task instances will run
        sequentially while relying on the previous task's schedule to
        succeed. The task instance for the start_date is allowed to run.
    :type depends_on_past: bool
    :param wait_for_downstream: when set to true, an instance of task
        X will wait for tasks immediately downstream of the previous instance
        of task X to finish successfully before it runs. This is useful if the
        different instances of a task X alter the same asset, and this asset
        is used by tasks downstream of task X. Note that depends_on_past
        is forced to True wherever wait_for_downstream is used.
    :type wait_for_downstream: bool
    :param queue: which queue to target when running this job. Not
        all executors implement queue management, the CeleryExecutor
        does support targeting specific queues.
    :type queue: str
    :param dag: a reference to the dag the task is attached to (if any)
    :type dag: DAG
    :param priority_weight: priority weight of this task against other task.
        This allows the executor to trigger higher priority tasks before
        others when things get backed up.
    :type priority_weight: int
    :param weight_rule: weighting method used for the effective total
        priority weight of the task. Options are:
        ``{ downstream | upstream | absolute }`` default is ``downstream``
        When set to ``downstream`` the effective weight of the task is the
        aggregate sum of all downstream descendants. As a result, upstream
        tasks will have higher weight and will be scheduled more aggressively
        when using positive weight values. This is useful when you have
        multiple dag run instances and desire to have all upstream tasks to
        complete for all runs before each dag can continue processing
        downstream tasks. When set to ``upstream`` the effective weight is the
        aggregate sum of all upstream ancestors. This is the opposite where
        downtream tasks have higher weight and will be scheduled more
        aggressively when using positive weight values. This is useful when you
        have multiple dag run instances and prefer to have each dag complete
        before starting upstream tasks of other dags.  When set to
        ``absolute``, the effective weight is the exact ``priority_weight``
        specified without additional weighting. You may want to do this when
        you know exactly what priority weight each task should have.
        Additionally, when set to ``absolute``, there is bonus effect of
        significantly speeding up the task creation process as for very large
        DAGS. Options can be set as string or using the constants defined in
        the static class ``airflow.utils.WeightRule``
    :type weight_rule: str
    :param pool: the slot pool this task should run in, slot pools are a
        way to limit concurrency for certain tasks
    :type pool: str
    :param sla: time by which the job is expected to succeed. Note that
        this represents the ``timedelta`` after the period is closed. For
        example if you set an SLA of 1 hour, the scheduler would send an email
        soon after 1:00AM on the ``2016-01-02`` if the ``2016-01-01`` instance
        has not succeeded yet.
        The scheduler pays special attention for jobs with an SLA and
        sends alert
        emails for sla misses. SLA misses are also recorded in the database
        for future reference. All tasks that share the same SLA time
        get bundled in a single email, sent soon after that time. SLA
        notification are sent once and only once for each task instance.
    :type sla: datetime.timedelta
    :param execution_timeout: max time allowed for the execution of
        this task instance, if it goes beyond it will raise and fail.
    :type execution_timeout: datetime.timedelta
    :param on_failure_callback: a function to be called when a task instance
        of this task fails. a context dictionary is passed as a single
        parameter to this function. Context contains references to related
        objects to the task instance and is documented under the macros
        section of the API.
    :type on_failure_callback: callable
    :param on_retry_callback: much like the ``on_failure_callback`` except
        that it is executed when retries occur.
    :type on_retry_callback: callable
    :param on_success_callback: much like the ``on_failure_callback`` except
        that it is executed when the task succeeds.
    :type on_success_callback: callable
    :param trigger_rule: defines the rule by which dependencies are applied
        for the task to get triggered. Options are:
        ``{ all_success | all_failed | all_done | one_success |
        one_failed | dummy}``
        default is ``all_success``. Options can be set as string or
        using the constants defined in the static class
        ``airflow.utils.TriggerRule``
    :type trigger_rule: str
    :param resources: A map of resource parameter names (the argument names of the
        Resources constructor) to their values.
    :type resources: dict
    :param run_as_user: unix username to impersonate while running the task
    :type run_as_user: str
    :param task_concurrency: When set, a task will be able to limit the concurrent
        runs across execution_dates
    :type task_concurrency: int
    :param executor_config: Additional task-level configuration parameters that are
        interpreted by a specific executor. Parameters are namespaced by the name of
        executor.

        **Example**: to run this task in a specific docker container through
        the KubernetesExecutor ::

            MyOperator(...,
                executor_config={
                "KubernetesExecutor":
                    {"image": "myCustomDockerImage"}
                    }
            )

    :type executor_config: dict
    """

    # For derived classes to define which fields will get jinjaified
    template_fields = []
    # Defines which files extensions to look for in the templated fields
    template_ext = []
    # Defines the color in the UI
    ui_color = '#fff'
    ui_fgcolor = '#000'

    @apply_defaults
    def __init__(
            self,
            task_id,
            owner=configuration.conf.get('operators', 'DEFAULT_OWNER'),
            email=None,
            email_on_retry=True,
            email_on_failure=True,
            retries=0,
            retry_delay=timedelta(seconds=300),
            retry_exponential_backoff=False,
            max_retry_delay=None,
            start_date=None,
            end_date=None,
            schedule_interval=None,  # not hooked as of now
            depends_on_past=False,
            wait_for_downstream=False,
            dag=None,
            params=None,
            default_args=None,
            adhoc=False,
            priority_weight=1,
            weight_rule=WeightRule.DOWNSTREAM,
            queue=configuration.conf.get('celery', 'default_queue'),
            pool=None,
            sla=None,
            execution_timeout=None,
            on_failure_callback=None,
            on_success_callback=None,
            on_retry_callback=None,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            resources=None,
            run_as_user=None,
            task_concurrency=None,
            executor_config=None,
            inlets=None,
            outlets=None,
            *args,
            **kwargs):

        if args or kwargs:
            # TODO remove *args and **kwargs in Airflow 2.0
            warnings.warn(
                'Invalid arguments were passed to {c}. Support for '
                'passing such arguments will be dropped in Airflow 2.0. '
                'Invalid arguments were:'
                '\n*args: {a}\n**kwargs: {k}'.format(
                    c=self.__class__.__name__, a=args, k=kwargs),
                category=PendingDeprecationWarning
            )
        # 任务ID格式化处理
        validate_key(task_id)
        self.task_id = task_id
        self.owner = owner
        self.email = email
        # 任务重试时发送邮件 task.email_on_retry and task.email:
        self.email_on_retry = email_on_retry
        # 任务执行失败时发送邮件 task.email_on_failure and task.email
        self.email_on_failure = email_on_failure
        # 任务开始时间
        self.start_date = start_date
        if start_date and not isinstance(start_date, datetime):
            self.log.warning("start_date for %s isn't datetime.datetime", self)
        self.end_date = end_date
        # 判断触发规则是否有效
        if not TriggerRule.is_valid(trigger_rule):
            raise AirflowException(
                "The trigger_rule must be one of {all_triggers},"
                "'{d}.{t}'; received '{tr}'."
                .format(all_triggers=TriggerRule.all_triggers,
                        d=dag.dag_id if dag else "", t=task_id, tr=trigger_rule))

        self.trigger_rule = trigger_rule
        # 当前任务实例是否等待上一个调度的同一个任务的任务实例执行完成后才执行
        self.depends_on_past = depends_on_past
        # 当前任务实例是否等待上一个调度的同一个任务的所有下游任务全部完成后才执行
        self.wait_for_downstream = wait_for_downstream
        if wait_for_downstream:
            self.depends_on_past = True

        if schedule_interval:
            self.log.warning(
                "schedule_interval is used for %s, though it has "
                "been deprecated as a task parameter, you need to "
                "specify it as a DAG parameter instead",
                self
            )
        self._schedule_interval = schedule_interval
        # 重试次数，默认不重试
        self.retries = retries
        # 指定所运行的队列
        self.queue = queue
        self.pool = pool
        self.sla = sla
        # 任务实例的执行超时时间，如果超时则抛出异常XToolTaskTimeout
        self.execution_timeout = execution_timeout
        # 成功或失败回调
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback
        # 重试回调
        self.on_retry_callback = on_retry_callback
        # 重试间隔
        if isinstance(retry_delay, timedelta):
            self.retry_delay = retry_delay
        else:
            self.log.debug("Retry_delay isn't timedelta object, assuming secs")
            self.retry_delay = timedelta(seconds=retry_delay)
        self.retry_exponential_backoff = retry_exponential_backoff
        self.max_retry_delay = max_retry_delay
        self.params = params or {}  # Available in templates!
        # 即席查询（Ad Hoc）是用户根据自己的需求，灵活的选择查询条件，系统能够根据用户的选择生成相应的统计报表。
        # 即席查询与普通应用查询最大的不同是普通的应用查询是定制开发的，而即席查询是由用户自定义查询条件的。
        self.adhoc = adhoc
        # 任务优先级
        self.priority_weight = priority_weight
        if not WeightRule.is_valid(weight_rule):
            raise AirflowException(
                "The weight_rule must be one of {all_weight_rules},"
                "'{d}.{t}'; received '{tr}'."
                .format(all_weight_rules=WeightRule.all_weight_rules,
                        d=dag.dag_id if dag else "", t=task_id, tr=weight_rule))
        self.weight_rule = weight_rule

        # 给每个任务分配资源: 每个任务所分配的资源
        cpus=configuration.conf.getint('operators', 'default_cpus'),
        ram=configuration.conf.getint('operators', 'default_ram'),
        disk=configuration.conf.getint('operators', 'default_disk'),
        gpus=configuration.conf.getint('operators', 'default_gpus')
        if not resources:
            resources_kwargs = {"cpus": cpus, "ram": ram, "disk": disk, "gpus": gpus}
        else:
            resources.setdefault("cpus", cpus)
            resources.setdefault("ram", ram)
            resources.setdefault("disk", disk)
            resources.setdefault("gpus", gpus)    
        self.resources = Resources(**resources_kwargs)
        self.run_as_user = run_as_user
        # 任务并发数限制，State.RUNNING 状态的任务实例的数量不能超过此阈值，默认为None
        self.task_concurrency = task_concurrency
        self.executor_config = executor_config or {}

        # Private attributes
        self._upstream_task_ids = set()
        self._downstream_task_ids = set()

        # 用于上下文with dag
        if not dag and _CONTEXT_MANAGER_DAG:
            dag = _CONTEXT_MANAGER_DAG

        # 将任务添加到DAG中
        if dag:
            self.dag = dag

        self._log = logging.getLogger("airflow.task.operators")

        # lineage
        self.inlets = []
        self.outlets = []
        self.lineage_data = None

        self._inlets = {
            "auto": False,
            "task_ids": [],
            "datasets": [],
        }

        self._outlets = {
            "datasets": [],
        }

        if inlets:
            self._inlets.update(inlets)

        if outlets:
            self._outlets.update(outlets)

        self._comps = {
            'task_id',
            'dag_id',
            'owner',
            'email',
            'email_on_retry',
            'retry_delay',
            'retry_exponential_backoff',
            'max_retry_delay',
            'start_date',
            'schedule_interval',
            'depends_on_past',
            'wait_for_downstream',
            'adhoc',
            'priority_weight',
            'sla',
            'execution_timeout',
            'on_failure_callback',
            'on_success_callback',
            'on_retry_callback',
        }

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            all(self.__dict__.get(c, None) == other.__dict__.get(c, None)
                for c in self._comps))

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.task_id < other.task_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    # Composing Operators -----------------------------------------------

    def __rshift__(self, other):
        """
        Implements Self >> Other == self.set_downstream(other)

        If "Other" is a DAG, the DAG is assigned to the Operator.
        """
        # 如果other是DAG，则将当前任务与指定的DAG对象关联
        if isinstance(other, DAG):
            # if this dag is already assigned, do nothing
            # otherwise, do normal dag assignment
            if not (self.has_dag() and self.dag is other):
                self.dag = other
        else:
            # 如果other是其他的任务，则设置依赖关系
            self.set_downstream(other)
        return other

    def __lshift__(self, other):
        """
        Implements Self << Other == self.set_upstream(other)

        If "Other" is a DAG, the DAG is assigned to the Operator.
        """
        if isinstance(other, DAG):
            # if this dag is already assigned, do nothing
            # otherwise, do normal dag assignment
            if not (self.has_dag() and self.dag is other):
                self.dag = other
        else:
            self.set_upstream(other)
        return other

    def __rrshift__(self, other):
        """
        Called for [DAG] >> [Operator] because DAGs don't have
        __rshift__ operators.
        """
        self.__lshift__(other)
        return self

    def __rlshift__(self, other):
        """
        Called for [DAG] << [Operator] because DAGs don't have
        __lshift__ operators.
        """
        self.__rshift__(other)
        return self

    # /Composing Operators ---------------------------------------------

    @property
    def dag(self):
        """
        Returns the Operator's DAG if set, otherwise raises an error
        """
        if self.has_dag():
            return self._dag
        else:
            raise AirflowException(
                'Operator {} has not been assigned to a DAG yet'.format(self))

    @dag.setter
    def dag(self, dag):
        """将任务添加到dag中
        Operators can be assigned to one DAG, one time. Repeat assignments to
        that same DAG are ok.
        """
        if not isinstance(dag, DAG):
            raise TypeError(
                'Expected DAG; received {}'.format(dag.__class__.__name__))
        elif self.has_dag() and self.dag is not dag:
            raise AirflowException(
                "The DAG assigned to {} can not be changed.".format(self))
        elif self.task_id not in dag.task_dict:
            # 在dag中添加任务
            dag.add_task(self)

        self._dag = dag

    def has_dag(self):
        """
        Returns True if the Operator has been assigned to a DAG.
        """
        return getattr(self, '_dag', None) is not None

    @property
    def dag_id(self):
        if self.has_dag():
            return self.dag.dag_id
        else:
            # 任务有可能是孤立的，没有与DAG关联
            return 'adhoc_' + self.owner

    @property
    def deps(self):
        """
        Returns the list of dependencies for the operator. These differ from execution
        context dependencies in that they are specific to tasks and can be
        extended/overridden by subclasses.
        """
        return {
            # 验证重试时间： 任务实例已经标记为重试，但是还没有到下一次重试时间，如果运行就会失败
            NotInRetryPeriodDep(),
            # 验证任务实例是否依赖上一个周期的任务实例
            PrevDagrunDep(),
            # 验证上游依赖任务
            TriggerRuleDep(),
        }

    @property
    def schedule_interval(self):
        """
        The schedule interval of the DAG always wins over individual tasks so
        that tasks within a DAG always line up. The task still needs a
        schedule_interval as it may not be attached to a DAG.
        """
        if self.has_dag():
            return self.dag._schedule_interval
        else:
            return self._schedule_interval

    @property
    def priority_weight_total(self):
        if self.weight_rule == WeightRule.ABSOLUTE:
            return self.priority_weight
        elif self.weight_rule == WeightRule.DOWNSTREAM:
            upstream = False
        elif self.weight_rule == WeightRule.UPSTREAM:
            upstream = True
        else:
            upstream = False

        return self.priority_weight + sum(
            map(lambda task_id: self._dag.task_dict[task_id].priority_weight,
                self.get_flat_relative_ids(upstream=upstream))
        )

    @prepare_lineage
    def pre_execute(self, context):
        """
        This hook is triggered right before self.execute() is called.
        """
        pass

    def execute(self, context):
        """
        This is the main method to derive when creating an operator.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise NotImplementedError()

    @apply_lineage
    def post_execute(self, context, result=None):
        """
        This hook is triggered right after self.execute() is called.
        It is passed the execution context and any results returned by the
        operator.
        """
        pass

    def on_kill(self):
        """
        Override this method to cleanup subprocesses when a task instance
        gets killed. Any use of the threading, subprocess or multiprocessing
        module within an operator needs to be cleaned up or it will leave
        ghost processes behind.
        """
        pass

    def __deepcopy__(self, memo):
        """
        Hack sorting double chained task lists by task_id to avoid hitting
        max_depth on deepcopy operations.
        """
        sys.setrecursionlimit(5000)  # TODO fix this in a better way
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for k, v in list(self.__dict__.items()):
            if k not in ('user_defined_macros', 'user_defined_filters',
                         'params', '_log'):
                setattr(result, k, copy.deepcopy(v, memo))
        result.params = self.params
        if hasattr(self, 'user_defined_macros'):
            result.user_defined_macros = self.user_defined_macros
        if hasattr(self, 'user_defined_filters'):
            result.user_defined_filters = self.user_defined_filters
        if hasattr(self, '_log'):
            result._log = self._log
        return result

    def __getstate__(self):
        """你可以自定义对象被pickle时被存储的状态，而不使用对象的 __dict__ 属性。
        这个状态在对象被反pickle时会被 __setstate__ 使用。 """
        state = dict(self.__dict__)
        del state['_log']

        return state

    def __setstate__(self, state):
        """当一个对象被反pickle时 ."""
        self.__dict__ = state
        self._log = logging.getLogger("airflow.task.operators")

    def render_template_from_field(self, attr, content, context, jinja_env):
        """使用上下文渲染模板，返回渲染后的模板内容
        Renders a template from a field. If the field is a string, it will
        simply render the string and return the result. If it is a collection or
        nested set of collections, it will traverse the structure and render
        all strings in it.
        """
        rt = self.render_template
        if isinstance(content, six.string_types):
            # 根据字符串加载模板
            result = jinja_env.from_string(content).render(**context)
        elif isinstance(content, (list, tuple)):
            result = [rt(attr, e, context) for e in content]
        elif isinstance(content, numbers.Number):
            result = content
        elif isinstance(content, dict):
            result = {
                k: rt("{}[{}]".format(attr, k), v, context)
                for k, v in list(content.items())}
        else:
            param_type = type(content)
            msg = (
                "Type '{param_type}' used for parameter '{attr}' is "
                "not supported for templating").format(**locals())
            raise AirflowException(msg)
        return result

    def render_template(self, attr, content, context):
        """
        Renders a template either from a file or directly in a field, and returns
        the rendered result.
        """
        jinja_env = self.dag.get_template_env() \
            if hasattr(self, 'dag') \
            else jinja2.Environment(cache_size=0)

        # 获得模板文件后缀，可以有多个
        exts = self.__class__.template_ext
        if (
                isinstance(content, six.string_types) and
                any([content.endswith(ext) for ext in exts])):
            # 根据路径，加载模板文件
            return jinja_env.get_template(content).render(**context)
        else:
            return self.render_template_from_field(attr, content, context, jinja_env)

    def prepare_template(self):
        """
        Hook that is triggered after the templated fields get replaced
        by their content. If you need your operator to alter the
        content of the file before the template is rendered,
        it should override this method to do so.
        """
        pass

    def resolve_template_files(self):
        """从指定的属性中获取设置的模板文件名，渲染模板并将此属性的值设置为渲染后的模板的内容 ."""
        # Getting the content of files for template_field / template_ext
        for attr in self.template_fields:
            content = getattr(self, attr)
            if content is not None and \
                    isinstance(content, six.string_types) and \
                    any([content.endswith(ext) for ext in self.template_ext]):
                # 获得指定后缀的模版文件, 此时content是模板文件的名称
                env = self.dag.get_template_env()
                try:
                    # 设置属性值为模板文件的内容
                    setattr(self, attr, env.loader.get_source(env, content)[0])
                except Exception as e:
                    self.log.exception(e)
        self.prepare_template()

    @property
    def upstream_list(self):
        """@property: list of tasks directly upstream"""
        return [self.dag.get_task(tid) for tid in self._upstream_task_ids]

    @property
    def upstream_task_ids(self):
        return self._upstream_task_ids

    @property
    def downstream_list(self):
        """@property: list of tasks directly downstream"""
        return [self.dag.get_task(tid) for tid in self._downstream_task_ids]

    @property
    def downstream_task_ids(self):
        return self._downstream_task_ids

    @provide_session
    def clear(self,
              start_date=None,
              end_date=None,
              upstream=False,
              downstream=False,
              session=None):
        """
        Clears the state of task instances associated with the task, following
        the parameters specified.
        """
        TI = TaskInstance
        qry = session.query(TI).filter(TI.dag_id == self.dag_id)

        if start_date:
            qry = qry.filter(TI.execution_date >= start_date)
        if end_date:
            qry = qry.filter(TI.execution_date <= end_date)

        tasks = [self.task_id]

        if upstream:
            tasks += [
                t.task_id for t in self.get_flat_relatives(upstream=True)]

        if downstream:
            tasks += [
                t.task_id for t in self.get_flat_relatives(upstream=False)]

        qry = qry.filter(TI.task_id.in_(tasks))

        count = qry.count()

        clear_task_instances(qry.all(), session, dag=self.dag)

        session.commit()

        return count

    def get_task_instances(self, session, start_date=None, end_date=None):
        """
        Get a set of task instance related to this task for a specific date
        range.
        """
        TI = TaskInstance
        end_date = end_date or datetime.now()
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
        ).order_by(TI.execution_date).all()

    def get_flat_relative_ids(self, upstream=False, found_descendants=None):
        """递归获得当前任务的所有依赖任务
        Get a flat list of relatives' ids, either upstream or downstream.
        """

        if not found_descendants:
            found_descendants = set()
        # 获得上游任务ID集合，或下游任务ID集合
        relative_ids = self.get_direct_relative_ids(upstream)

        for relative_id in relative_ids:
            if relative_id not in found_descendants:
                found_descendants.add(relative_id)
                # 获得依赖任务
                relative_task = self._dag.task_dict[relative_id]
                # 递归获得依赖任务的依赖任务
                relative_task.get_flat_relative_ids(upstream,
                                                    found_descendants)

        return found_descendants

    def get_flat_relatives(self, upstream=False):
        """根据upstream参数决定获取任务的上游任务列表，还是下游任务列表
        Get a flat list of relatives, either upstream or downstream.

        upstream=False 包含匹配任务的所有下游任务

        upstream=True  包含匹配任务的所有上游任务
        """
        return list(map(lambda task_id: self._dag.task_dict[task_id],
                        self.get_flat_relative_ids(upstream)))

    def run(
            self,
            start_date=None,
            end_date=None,
            ignore_first_depends_on_past=False,
            ignore_ti_state=False,
            mark_success=False):
        """
        Run a set of task instances for a date range.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date or datetime.now()
        # 根据DAG调度配置，开始时间、结束时间获得所有的计划调度时间
        for dt in self.dag.date_range(start_date, end_date=end_date):
            TaskInstance(self, dt).run(
                mark_success=mark_success,
                ignore_depends_on_past=(
                    dt == start_date and ignore_first_depends_on_past),
                ignore_ti_state=ignore_ti_state)

    def dry_run(self):
        """模拟执行任务 ."""
        self.log.info('Dry run')
        for attr in self.template_fields:
            # 根据模版参数，获得任务属性值
            content = getattr(self, attr)
            if content and isinstance(content, six.string_types):
                self.log.info('Rendering template for %s', attr)
                self.log.info(content)

    def get_direct_relative_ids(self, upstream=False):
        """获得上游任务ID集合，或下游任务ID集合
        Get the direct relative ids to the current task, upstream or
        downstream.

        upstream=False 包含匹配任务的所有下游任务

        upstream=True  包含匹配任务的所有上游任务
        """
        if upstream:
            return self._upstream_task_ids
        else:
            return self._downstream_task_ids

    def get_direct_relatives(self, upstream=False):
        """
        Get the direct relatives to the current task, upstream or
        downstream.
        """
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def __repr__(self):
        return "<Task({self.__class__.__name__}): {self.task_id}>".format(
            self=self)

    @property
    def task_type(self):
        return self.__class__.__name__

    def add_only_new(self, item_set, item):
        if item in item_set:
            raise AirflowException(
                'Dependency {self}, {item} already registered'
                ''.format(**locals()))
        else:
            item_set.add(item)

    def _set_relatives(self, task_or_task_list, upstream=False):
        try:
            task_list = list(task_or_task_list)
        except TypeError:
            task_list = [task_or_task_list]

        for t in task_list:
            if not isinstance(t, BaseOperator):
                raise AirflowException(
                    "Relationships can only be set between "
                    "Operators; received {}".format(t.__class__.__name__))

        # relationships can only be set if the tasks share a single DAG. Tasks
        # without a DAG are assigned to that DAG.
        # 所有依赖的任务必须属于同一个dag
        dags = {t._dag.dag_id: t._dag for t in [self] + task_list if t.has_dag()}

        if len(dags) > 1:
            raise AirflowException(
                'Tried to set relationships between tasks in '
                'more than one DAG: {}'.format(dags.values()))
        elif len(dags) == 1:
            dag = dags.popitem()[1]
        else:
            raise AirflowException(
                "Tried to create relationships between tasks that don't have "
                "DAGs yet. Set the DAG for at least one "
                "task  and try again: {}".format([self] + task_list))

        if dag and not self.has_dag():
            self.dag = dag

        for task in task_list:
            if dag and not task.has_dag():
                task.dag = dag
            if upstream:
                task.add_only_new(task._downstream_task_ids, self.task_id)
                self.add_only_new(self._upstream_task_ids, task.task_id)
            else:
                self.add_only_new(self._downstream_task_ids, task.task_id)
                task.add_only_new(task._upstream_task_ids, self.task_id)

    def set_downstream(self, task_or_task_list):
        """
        Set a task or a task list to be directly downstream from the current
        task.
        """
        self._set_relatives(task_or_task_list, upstream=False)

    def set_upstream(self, task_or_task_list):
        """
        Set a task or a task list to be directly upstream from the current
        task.
        """
        self._set_relatives(task_or_task_list, upstream=True)

    def xcom_push(
            self,
            context,
            key,
            value,
            execution_date=None):
        """
        See TaskInstance.xcom_push()
        """
        context['ti'].xcom_push(
            key=key,
            value=value,
            execution_date=execution_date)

    def xcom_pull(
            self,
            context,
            task_ids=None,
            dag_id=None,
            key=XCOM_RETURN_KEY,
            include_prior_dates=None):
        """
        See TaskInstance.xcom_pull()
        """
        return context['ti'].xcom_pull(
            key=key,
            task_ids=task_ids,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates)


class DagModel(Base):

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(String(ID_LEN), primary_key=True)
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = configuration.conf\
        .getboolean('core',
                    'dags_are_paused_at_creation')
    is_paused = Column(Boolean, default=is_paused_at_creation)
    # Whether the DAG is a subdag
    is_subdag = Column(Boolean, default=False)
    # Whether that DAG was seen on the last DagBag load
    is_active = Column(Boolean, default=False)
    # Last time the scheduler started
    last_scheduler_run = Column(DateTime)
    # Last time this DAG was pickled
    last_pickled = Column(DateTime)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired = Column(DateTime)
    # Whether (one  of) the scheduler is scheduling this DAG at the moment
    scheduler_lock = Column(Boolean)
    # Foreign key to the latest pickle_id
    pickle_id = Column(Integer)
    # The location of the file containing the DAG object
    fileloc = Column(String(2000))
    # String representing the owners
    owners = Column(String(2000))

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    @classmethod
    @provide_session
    def get_current(cls, dag_id, session=None):
        """根据dag_id从DB中获得DagModel对象 ."""
        return session.query(cls).filter(cls.dag_id == dag_id).first()

    @staticmethod
    @provide_session
    def get_task_instances_by_dag(dag, execution_dates, limit=1000, state=None, session=None):
        """根据dag和调度时间获取任务实例 ."""
        # 获得当前dag的所有任务实例
        TI = TaskInstance
        tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.execution_date.in_(execution_dates),
        )

        # 设置状态过滤条件，支持状态为None
        if state:
            if isinstance(state, six.string_types):
                tis = tis.filter(TI.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    tis = tis.filter(
                        or_(TI.state.in_(state),
                            TI.state.is_(None))
                    )
                else:
                    tis = tis.filter(TI.state.in_(state))
                    
        # 如果dag是子集，则只需要获得部分任务实例
        if dag and dag.partial:
            tis = tis.filter(TI.task_id.in_(dag.task_ids))

        if limit:
            return tis.limit(limit).all()
        else:
            return tis.all()

        
# To avoid circular import on Python2.7 we need to define this at the _bottom_
from airflow.models.connection import Connection  # noqa: E402,F401
from airflow.models.skipmixin import SkipMixin  # noqa: F401
