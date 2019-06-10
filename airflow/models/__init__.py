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

        
@functools.total_ordering
class DAG(BaseDag, LoggingMixin):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional
    dependencies. A dag also has a schedule, a start end an end date
    (optional). For each schedule, (say daily or hourly), the DAG needs to run
    each individual tasks as their dependencies are met. Certain tasks have
    the property of depending on their own past, meaning that they can't run
    until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    :param dag_id: The id of the DAG
    :type dag_id: string
    :param description: The description for the DAG to e.g. be shown on the webserver
    :type description: string
    :param schedule_interval: Defines how often that DAG runs, this
        timedelta object gets added to your latest task instance's
        execution_date to figure out the next schedule
    :type schedule_interval: datetime.timedelta or
        dateutil.relativedelta.relativedelta or str that acts as a cron
        expression
    :param start_date: The timestamp from which the scheduler will
        attempt to backfill
    :type start_date: datetime.datetime
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open ended scheduling
    :type end_date: datetime.datetime
    :param template_searchpath: This list of folders (non relative)
        defines where jinja will look for your templates. Order matters.
        Note that jinja/airflow includes the path of your DAG file by
        default
    :type template_searchpath: string or list of stings
    :param user_defined_macros: a dictionary of macros that will be exposed
        in your jinja templates. For example, passing ``dict(foo='bar')``
        to this argument allows you to ``{{ foo }}`` in all jinja
        templates related to this DAG. Note that you can pass any
        type of object here.
    :type user_defined_macros: dict
    :param user_defined_filters: a dictionary of filters that will be exposed
        in your jinja templates. For example, passing
        ``dict(hello=lambda name: 'Hello %s' % name)`` to this argument allows
        you to ``{{ 'world' | hello }}`` in all jinja templates related to
        this DAG.
    :type user_defined_filters: dict
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :type default_args: dict
    :param params: a dictionary of DAG level parameters that are made
        accessible in templates, namespaced under `params`. These
        params can be overridden at the task level.
    :type params: dict
    :param concurrency: the number of task instances allowed to run
        concurrently
    :type concurrency: int
    :param max_active_runs: maximum number of active DAG runs, beyond this
        number of DAG runs in a running state, the scheduler won't create
        new active DAG runs
    :type max_active_runs: int
    :param dagrun_timeout: specify how long a DagRun should be up before
        timing out / failing, so that new DagRuns can be created
    :type dagrun_timeout: datetime.timedelta
    :param sla_miss_callback: specify a function to call when reporting SLA
        timeouts.
    :type sla_miss_callback: types.FunctionType
    :param default_view: Specify DAG default view (tree, graph, duration,
                                                   gantt, landing_times)
    :type default_view: string
    :param orientation: Specify DAG orientation in graph view (LR, TB, RL, BT)
    :type orientation: string
    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to True
    :type catchup: bool
    :param on_failure_callback: A function to be called when a DagRun of this dag fails.
        A context dictionary is passed as a single parameter to this function.
    :type on_failure_callback: callable
    :param on_success_callback: Much like the ``on_failure_callback`` except
        that it is executed when the dag succeeds.
    :type on_success_callback: callable
    """

    def __init__(
            self, dag_id,
            description='',
            schedule_interval=timedelta(days=1),
            start_date=None, end_date=None,
            full_filepath=None,
            template_searchpath=None,
            user_defined_macros=None,
            user_defined_filters=None,
            default_args=None,
            concurrency=configuration.conf.getint('core', 'dag_concurrency'),
            max_active_runs=configuration.conf.getint(
                'core', 'max_active_runs_per_dag'),
            dagrun_timeout=None,
            sla_miss_callback=None,
            default_view=configuration.conf.get('webserver', 'dag_default_view').lower(),
            orientation=configuration.conf.get('webserver', 'dag_orientation'),
            catchup=configuration.conf.getboolean('scheduler', 'catchup_by_default'),
            on_success_callback=None, on_failure_callback=None,
            params=None):

        # 宏
        self.user_defined_macros = user_defined_macros
        # 过滤器
        self.user_defined_filters = user_defined_filters
        # 默认参数
        self.default_args = default_args or {}
        # 可在模板中访问的参数，可以在任务级别修改
        self.params = params or {}

        # merging potentially conflicting default_args['params'] into params
        if 'params' in self.default_args:
            self.params.update(self.default_args['params'])
            del self.default_args['params']

        # 验证dagid字符串规则，如果是不支持的特殊字符，抛出 AirflowException
        validate_key(dag_id)

        # Properties from BaseDag
        self._dag_id = dag_id
        # dag文件的绝对路径
        self._full_filepath = full_filepath if full_filepath else ''
        # 每个dag同时执行的最大任务实例的数量
        self._concurrency = concurrency
        self._pickle_id = None

        # dag的详细描述信息
        self._description = description
        # set file location to caller source path
        # 获得代码所在的文件名
        self.fileloc = sys._getframe().f_back.f_code.co_filename

        # dag包含的所有任务
        self.task_dict = dict()

        # 获得指定时区
        # 如果开始时间为空，
        # 如果catchup==True, 则开始时间取所有任务的最小开始时间
        # 如果catchup==False, 则创建dagrun时，dag的开始时间设置为当前时间的上一个周期的上一个周期的时间
        # set timezone
        self.start_date = start_date
        self.end_date = end_date
        # crontab标准格式
        self.schedule_interval = schedule_interval
        # 将调度缩写转换为crontab标准格式
        if schedule_interval in cron_presets:
            self._schedule_interval = cron_presets.get(schedule_interval)
        elif schedule_interval == '@once':
            self._schedule_interval = None
        else:
            # 可以是标准的crontab格式
            # 也可以是一个 timedelta 对象，延迟多长时间后执行
            self._schedule_interval = schedule_interval

        # 模板搜索路径，可以设置多个
        if isinstance(template_searchpath, six.string_types):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath

        # 是否存在父dag
        self.parent_dag = None  # Gets set when DAGs are loaded
        # 最新加载时间
        self.last_loaded = datetime.now()
        # dag id格式化
        self.safe_dag_id = dag_id.replace('.', '__dot__')
        # 每个dag最多运行的dag_run数量
        self.max_active_runs = max_active_runs
        # dag实例的超时时间，如果超时则将当前正在运行的dag_run标记为失败，并重新创建一个新的dagrun
        self.dagrun_timeout = dagrun_timeout
        # 服务失效回调函数
        self.sla_miss_callback = sla_miss_callback
        # dag在视图中默认显示树状图
        self.default_view = default_view
        # dag中的任务默认从左到右展示
        self.orientation = orientation
        # 设置调度器的默认行为
        self.catchup = catchup
        # dag默认不是子dag
        self.is_subdag = False  # DagBag.bag_dag() will set this to True if appropriate

        # 默认包含所有任务
        self.partial = False

        # 成功和失败回调函数
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback

        # dag相等的判断依据
        self._comps = {
            'dag_id',
            'task_ids',
            'parent_dag',
            'start_date',
            'schedule_interval',
            'full_filepath',
            'template_searchpath',
            'last_loaded',
        }

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            # Use getattr() instead of __dict__ as __dict__ doesn't return
            # correct values for properties.
            all(getattr(self, c, None) == getattr(other, c, None)
                for c in self._comps))

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.dag_id < other.dag_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            # task_ids returns a list and lists can't be hashed
            if c == 'task_ids':
                val = tuple(self.task_dict.keys())
            else:
                val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    # Context Manager -----------------------------------------------

    def __enter__(self):
        global _CONTEXT_MANAGER_DAG
        self._old_context_manager_dag = _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self
        return self

    def __exit__(self, _type, _value, _tb):
        global _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self._old_context_manager_dag

    # /Context Manager ----------------------------------------------

    def date_range(self, start_date, num=None, end_date=datetime.now()):
        """根据调度配置获取dag实例运行的时间范围."""
        if num:
            end_date = None
        return utils_date_range(
            start_date=start_date, end_date=end_date,
            num=num, delta=self._schedule_interval)

    def is_fixed_time_schedule(self):
        """
        Figures out if the DAG schedule has a fixed time (e.g. 3 AM).

        :return: True if the schedule has a fixed time, False if not.
        """
        now = datetime.now()
        cron = croniter(self._schedule_interval, now)

        start = cron.get_next(datetime)
        cron_next = cron.get_next(datetime)

        if cron_next.minute == start.minute and cron_next.hour == start.hour:
            return True

        return False

    def following_schedule(self, dttm):
        """获得下一次调度时间，如果是单次调度，因为self._schedule_interval为None，所以函数默认返回None
        Calculates the following schedule for this dag in local time

        :param dttm:  datetime
        :return:  datetime
        """
        if isinstance(self._schedule_interval, six.string_types):
            # 创建cron
            cron = croniter(self._schedule_interval, dttm)
            # 获得下一次调度时间，添加时区信息
            return cron.get_next(datetime)
        elif isinstance(self._schedule_interval, timedelta):
            return dttm + self._schedule_interval

    def previous_schedule(self, dttm):
        """获得上一次调度时间，如果是单次调度，则返回None
        Calculates the previous schedule for this dag in local time

        :param dttm: datetime
        :return: datetime
        """
        if isinstance(self._schedule_interval, six.string_types):
            # 创建cron
            cron = croniter(self._schedule_interval, dttm)
            return cron.get_prev(datetime)
        elif self._schedule_interval is not None:
            return dttm - self._schedule_interval

    def get_run_dates(self, start_date, end_date=None):
        """获得指定范围内的execution_time，用于确定哪些dagrun需要补录
        Returns a list of dates between the interval received as parameter using this
        dag's schedule interval. Returned dates can be used for execution dates.

        :param start_date: the start date of the interval
        :type start_date: datetime
        :param end_date: the end date of the interval, defaults to datetime.now()
        :type end_date: datetime
        :return: a list of dates within the interval following the dag's schedule
        :rtype: list
        """
        run_dates = []

        using_start_date = start_date
        using_end_date = end_date

        # dates for dag runs
        # 如果开始时间为空，则取所有任务的最小开始时间
        using_start_date = using_start_date or min([t.start_date for t in self.tasks])
        # 结束时间为空，则取当前时间
        using_end_date = using_end_date or datetime.now()

        # next run date for a subdag isn't relevant (schedule_interval for subdags
        # is ignored) so we use the dag run's start date in the case of a subdag
        # 获得 execution_time
        # 1. 如果是子dag，下一次调度时间为开始时间
        # 2. 如果不是子dag，需要根据调度配置计算下一次调度时间
        next_run_date = (self.normalize_schedule(using_start_date)
                         if not self.is_subdag else using_start_date)

        # 获取开始时间和结束时间之间所有的execution_time
        #     next_run_date                                            using_end_date
        #          ||                                                       ||
        #          \/                                                       \/
        # |-------------------|-------------------|-------------------|---------------|
        #                run_dates[0]        run_dates[1]       run_dates[2]
        while next_run_date and next_run_date <= using_end_date:
            run_dates.append(next_run_date)
            # 如果是单次任务，则返回None
            # 如果不是单次任务，则返回下一个调度时间，知道到达结束时间
            next_run_date = self.following_schedule(next_run_date)

        return run_dates

    def normalize_schedule(self, dttm):
        """获得下一次调度时间，需要处理临界点
        Returns dttm + interval unless dttm is first interval then it returns dttm
        """
        following = self.following_schedule(dttm)

        # in case of @once
        # 如果是单次任务，返回开始时间
        if not following:
            return dttm
        # dttm不在调度时间点，即在两次调度的中间
        # 返回下一次调度时间
        #    dttm      is          2018-04-12 00:04:33.836000，
        #    following is          2018-04-12 01:00:00
        #    previous following is 2018-04-12 00:00:00
        if self.previous_schedule(following) != dttm:
            return following

        # 如果dttm刚好在调度时间点上
        # 则返回此时间
        #    dttm      is          2018-04-12 00:00:00
        #    following is          2018-04-12 01:00:00
        #    previous following is 2018-04-12 00:00:00
        return dttm

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        """根据dagid，获得最近的一个dagrun
        Returns the last dag run for this dag, None if there was none.
        Last dag run can be any type of run eg. scheduled or backfilled.
        Overridden DagRuns are ignored
        
        Args:
            include_externally_triggered: 是否包含外部触发的dagrun
        """
        begin_time = datetime.now() - timedelta(days=configuration.conf.getint('core', 'sql_query_history_days'))
        DR = DagRun
        qry = session.query(DR).filter(
            DR.dag_id == self.dag_id,
        ).filter(DagRun.execution_date > begin_time)
        # 是否获取外部触发的dagrun
        if not include_externally_triggered:
            qry = qry.filter(DR.external_trigger.__eq__(False))

        qry = qry.order_by(DR.execution_date.desc())

        last = qry.first()

        return last

    @property
    def dag_id(self):
        return self._dag_id

    @dag_id.setter
    def dag_id(self, value):
        self._dag_id = value

    @property
    def full_filepath(self):
        return self._full_filepath

    @full_filepath.setter
    def full_filepath(self, value):
        self._full_filepath = value

    @property
    def concurrency(self):
        """每个dag同时执行的任务实例的数量 ."""
        return self._concurrency

    @concurrency.setter
    def concurrency(self, value):
        self._concurrency = value

    @property
    def description(self):
        return self._description

    @property
    def pickle_id(self):
        return self._pickle_id

    @pickle_id.setter
    def pickle_id(self, value):
        self._pickle_id = value

    @property
    def tasks(self):
        """获得dag包含的所有任务 ."""
        return list(self.task_dict.values())

    @tasks.setter
    def tasks(self, val):
        raise AttributeError(
            'DAG.tasks can not be modified. Use dag.add_task() instead.')

    @property
    def task_ids(self):
        """获得dag包含的所有任务的IDs ."""
        return list(self.task_dict.keys())

    @property
    def active_task_ids(self):
        """获得dag包含的所有的有效任务Ids ."""
        return list(k for k, v in self.task_dict.items() if not v.adhoc)

    @property
    def active_tasks(self):
        """获得dag包含的所有的有效任务，不包含即席查询 ."""
        return [t for t in self.tasks if not t.adhoc]

    @property
    def filepath(self):
        """获得dag文件的相对路径
        File location of where the dag object is instantiated
        """
        # 获得相对路径
        fn = self.full_filepath.replace(settings.DAGS_FOLDER + '/', '')
        fn = fn.replace(os.path.dirname(__file__) + '/', '')
        return fn

    @property
    def folder(self):
        """获得dag文件所在的目录
        Folder location of where the dag object is instantiated
        """
        return os.path.dirname(self.full_filepath)

    @property
    def owner(self):
        """获得所有任务的拥有者，用逗号加空格分割 ."""
        return ", ".join(list(set([t.owner for t in self.tasks])))

    @property
    @provide_session
    def concurrency_reached(self, session=None):
        """根据dagid，判断正在运行的任务实例是否超出了阈值
        每个dag同时执行的最大任务实例的数量为 configuration.conf.getint('core', 'dag_concurrency')
        Returns a boolean indicating whether the concurrency limit for this DAG
        has been reached
        """
        # 获得正在运行的任务任务实例的数量
        TI = TaskInstance
        qry = session.query(func.count(TI.task_id)).filter(
            TI.dag_id == self.dag_id,
            TI.state == State.RUNNING,
        )
        # 判断是否到达阈值
        return qry.scalar() >= self.concurrency

    @property
    @provide_session
    def is_paused(self, session=None):
        """判断dag是否是暂停状态
        Returns a boolean indicating whether this DAG is paused
        """
        qry = session.query(DagModel).filter(
            DagModel.dag_id == self.dag_id)
        return qry.value('is_paused')

    @provide_session
    def handle_callback(self, dagrun, success=True, reason=None, session=None):
        """执行dag成功或失败回调函数
        Triggers the appropriate callback depending on the value of success, namely the
        on_failure_callback or on_success_callback. This method gets the context of a
        single TaskInstance part of this DagRun and passes that to the callable along
        with a 'reason', primarily to differentiate DagRun failures.
        .. note::
            The logs end up in $AIRFLOW_HOME/logs/scheduler/latest/PROJECT/DAG_FILE.py.log
        :param dagrun: DagRun object
        :param success: Flag to specify if failure or success callback should be called
        :param reason: Completion reason
        :param session: Database session
        """
        # 获得回调函数
        callback = self.on_success_callback if success else self.on_failure_callback
        if callback:
            self.log.info('Executing dag callback function: {}'.format(callback))
            # 获得dagrun的所有任务实例
            tis = dagrun.get_task_instances(session=session)
            # 取最近的一条任务实例
            ti = tis[-1]  # get first TaskInstance of DagRun
            # 获得任务
            ti.task = self.get_task(ti.task_id)
            # 获得任务实例上下文，并添加原因描述
            context = ti.get_template_context(session=session)
            context.update({'reason': reason})
            # 执行回调函数
            callback(context)

    @provide_session
    def get_active_runs(self, session=None):
        """获得正在运行的dagrun的调度时间
        Returns a list of dag run execution dates currently running

        :param session:
        :return: List of execution dates
        """
        # 根据dagId， 获得dagrun的数量
        runs = DagRun.find(dag_id=self.dag_id, state=State.RUNNING)

        active_dates = []
        for run in runs:
            active_dates.append(run.execution_date)

        return active_dates

    @provide_session
    def get_num_active_runs(self, external_trigger=None, session=None):
        """根据dagid，获得正在运行的dagrun的数量
        Returns the number of active "running" dag runs

        :param external_trigger: True for externally triggered active dag runs
        :type external_trigger: bool
        :param session:
        :return: number greater than 0 for active dag runs
        """
        query = (session
                 .query(DagRun)
                 .filter(DagRun.dag_id == self.dag_id)
                 .filter(DagRun.state == State.RUNNING))

        if external_trigger is not None:
            query = query.filter(DagRun.external_trigger == external_trigger)

        return query.count()

    @provide_session
    def get_dagrun(self, execution_date, session=None):
        """根据dagid和调度时间，获得dagrun
        Returns the dag run for a given execution date if it exists, otherwise
        none.

        :param execution_date: The execution date of the DagRun to find.
        :param session:
        :return: The DagRun if found, otherwise None.
        """
        dagrun = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date == execution_date)
            .first())

        return dagrun

    @property
    @provide_session
    def latest_execution_date(self, session=None):
        """获得最近的一个dagrun的调度时间
        Returns the latest date for which at least one dag run exists
        """
        execution_date = session.query(func.max(DagRun.execution_date)).filter(
            DagRun.dag_id == self.dag_id
        ).scalar()
        return execution_date

    @property
    def subdags(self):
        """
        Returns a list of the subdag objects associated to this DAG
        """
        # Check SubDag for class but don't check class directly, see
        # https://github.com/airbnb/airflow/issues/1168
        from airflow.operators.subdag_operator import SubDagOperator
        subdag_lst = []
        for task in self.tasks:
            if (isinstance(task, SubDagOperator) or
                    # TODO remove in Airflow 2.0
                    type(task).__name__ == 'SubDagOperator'):
                subdag_lst.append(task.subdag)
                subdag_lst += task.subdag.subdags
        return subdag_lst

    def resolve_template_files(self):
        """删除所有任务的临时文件 ."""
        for t in self.tasks:
            t.resolve_template_files()

    def get_template_env(self):
        """返回jinja2模板的env，在支持用户自定义宏和自定义过滤器
        Returns a jinja2 Environment while taking into account the DAGs
        template_searchpath, user_defined_macros and user_defined_filters
        """
        # 获得dag所在的目录，并设置jinja2模板的搜索路径
        searchpath = [self.folder]
        if self.template_searchpath:
            searchpath += self.template_searchpath

        # 创建jinjia2的环境变量
        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath),
            extensions=["jinja2.ext.do"],
            cache_size=0)
        # 将用户自定义宏加入到jinj2的环境变量中
        if self.user_defined_macros:
            env.globals.update(self.user_defined_macros)
        # 将用户自定义过滤器加入到jinja2的环境变量中
        if self.user_defined_filters:
            env.filters.update(self.user_defined_filters)

        return env

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """设置任务依赖
        Simple utility method to set dependency between two tasks that
        already have been added to the DAG using add_task()
        """
        self.get_task(upstream_task_id).set_downstream(
            self.get_task(downstream_task_id))

    def get_task_instances(
            self, session, start_date=None, end_date=None, state=None):
        """根据查询条件获得任务实例 ."""
        TI = TaskInstance
        # 获得默认开始时间和结束时间
        if not start_date:
            start_date = (datetime.today() - timedelta(30)).date()
            start_date = datetime.combine(start_date, datetime.min.time())
        end_date = end_date or datetime.now()
        # 获得指定时间范围内的所有任务实例
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
            TI.task_id.in_([t.task_id for t in self.tasks]),
        )
        if state:
            tis = tis.filter(TI.state == state)
        tis = tis.order_by(TI.execution_date).all()
        return tis

    @property
    def roots(self):
        """获得所有的叶子节点，即没有下游节点 ."""
        return [t for t in self.tasks if not t.downstream_list]

    def topological_sort(self):
        """拓扑排序
        Sorts tasks in topographical order, such that a task comes after any of its
        upstream dependencies.

        Heavily inspired by:
        http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        :return: list of tasks in topological order
        """

        # copy the the tasks so we leave it unmodified
        graph_unsorted = self.tasks[:]

        graph_sorted = []

        # special case
        if len(self.tasks) == 0:
            return tuple(graph_sorted)

        # Run until the unsorted graph is empty.
        while graph_unsorted:
            # Go through each of the node/edges pairs in the unsorted
            # graph. If a set of edges doesn't contain any nodes that
            # haven't been resolved, that is, that are still in the
            # unsorted graph, remove the pair from the unsorted graph,
            # and append it to the sorted graph. Note here that by using
            # using the items() method for iterating, a copy of the
            # unsorted graph is used, allowing us to modify the unsorted
            # graph as we move through it. We also keep a flag for
            # checking that that graph is acyclic, which is true if any
            # nodes are resolved during each pass through the graph. If
            # not, we need to bail out as the graph therefore can't be
            # sorted.
            acyclic = False
            for node in list(graph_unsorted):
                for edge in node.upstream_list:
                    if edge in graph_unsorted:
                        break
                # no edges in upstream tasks
                else:
                    # 无环
                    acyclic = True
                    graph_unsorted.remove(node)
                    graph_sorted.append(node)

            if not acyclic:
                raise AirflowException("A cyclic dependency occurred in dag: {}"
                                       .format(self.dag_id))

        return tuple(graph_sorted)

    @provide_session
    def set_dag_runs_state(
            self,
            state=State.RUNNING,
            session=None,
            start_date=None,
            end_date=None,
    ):
        """在dag中，将dagrun的状态设置为RUNNING ."""
        # 更新dag每个状态的dag_run的数量，并设置dirty为False，实际上并没有执行
        query = session.query(DagRun).filter_by(dag_id=self.dag_id)
        if start_date:
            query = query.filter(DagRun.execution_date >= start_date)
        if end_date:
            query = query.filter(DagRun.execution_date <= end_date)
        drs = query.all()

        # 将dagrun的状态设置为RUNNING
        dirty_ids = []
        for dr in drs:
            dr.state = state
            dirty_ids.append(dr.dag_id)

        # 添加统计信息
        DagStat.update(dirty_ids, session=session)

    @provide_session
    def clear(
            self, start_date=None, end_date=None,
            only_failed=False,
            only_running=False,
            confirm_prompt=False,
            include_subdags=True,
            include_parentdag=True,
            reset_dag_runs=True,
            dry_run=False,
            session=None,
            get_tis=False,
    ):
        """在dag中，删除任务实例，重置dagruns，返回重置成功的任务实例的数量
        清除正在运行的任务实例和job，将dag_run设置为运行态

        execution_date in [start_date, end_date]

        - 正在运行的任务改为关闭状态，相关的job设置为关闭状态
        - 非正在运行的任务改为 None 状态，并修改任务实例的最大重试次数 max_tries
        - 任务相关的dagrun设置为RUNNING状态

        Clears a set of task instances associated with the current dag for
        a specified date range.
        """
        # 根据条件获得任务实例
        TI = TaskInstance
        tis = session.query(TI)
        if include_subdags:
            # Crafting the right filter for dag_id and task_ids combo
            conditions = []
            for dag in self.subdags + [self]:
                conditions.append(
                    TI.dag_id.like(dag.dag_id) &
                    TI.task_id.in_(dag.task_ids)
                )
            tis = tis.filter(or_(*conditions))
        else:
            tis = session.query(TI).filter(TI.dag_id == self.dag_id)
            tis = tis.filter(TI.task_id.in_(self.task_ids))

        if include_parentdag and self.is_subdag:

            p_dag = self.parent_dag.sub_dag(
                task_regex=self.dag_id.split('.')[1],
                include_upstream=False,
                include_downstream=True)

            tis = tis.union(p_dag.clear(
                start_date=start_date, end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                confirm_prompt=confirm_prompt,
                include_subdags=include_subdags,
                include_parentdag=False,
                reset_dag_runs=reset_dag_runs,
                get_tis=True,
                session=session,
            ))

        if start_date:
            tis = tis.filter(TI.execution_date >= start_date)
        if end_date:
            tis = tis.filter(TI.execution_date <= end_date)
        if only_failed:
            tis = tis.filter(TI.state == State.FAILED)
        if only_running:
            tis = tis.filter(TI.state == State.RUNNING)

        if get_tis:
            return tis

        # 模拟运行，用于判断是否存在任务实例
        if dry_run:
            # 不会删除dag_run
            tis = tis.all()
            # 清除session实例
            session.expunge_all()
            return tis

        # 获得需要清除的任务实例的数量
        count = tis.count()
        do_it = True
        if count == 0:
            return 0

        # 是否需要确认
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in tis])
            question = (
                "You are about to delete these {count} tasks:\n"
                "{ti_list}\n\n"
                "Are you sure? (yes/no): ").format(**locals())
            do_it = ask_yesno(question)

        if do_it:
            # 将任务实例和job关闭，将dag_run设置为运行态
            # 会把subdag中的dagrun也设置为RUNNNING
            # - 正在运行的任务改为关闭状态，相关的job设置为关闭状态
            # - 非正在运行的任务改为 None 状态，并修改任务实例的最大重试次数 max_tries
            # - 任务相关的dagrun设置为RUNNING状态
            clear_task_instances(tis.all(),
                                 session,
                                 dag=self,
                                 )
            # 重置dagrun，不包含subdag
            # TODO 重复操作
            if reset_dag_runs:
                # 在dag中，将dagrun的状态设置为RUNNING
                self.set_dag_runs_state(session=session,
                                        start_date=start_date,
                                        end_date=end_date,
                                        )
        else:
            count = 0
            print("Bail. Nothing was cleared.")

        session.commit()
        return count

    @classmethod
    def clear_dags(
            cls, dags,
            start_date=None,
            end_date=None,
            only_failed=False,      # 是否仅删除失败的任务
            only_running=False,     # 是否仅删除运行多个任务
            confirm_prompt=False,   # 是否需要用户确认
            include_subdags=True,   # 包含子dag
            include_parentdag=False,
            reset_dag_runs=True,    # 重置dagrun
            dry_run=False,
    ):
        """删除任务实例，重置dagruns，返回重置成功的任务实例的数量 ."""
        all_tis = []
        for dag in dags:
            # 获得待清除的任务实例
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                confirm_prompt=False,
                include_subdags=include_subdags,
                include_parentdag=include_parentdag,
                reset_dag_runs=reset_dag_runs,
                dry_run=True)
            all_tis.extend(tis)

        # 模拟测试：返回所有待清除的任务实例
        if dry_run:
            return all_tis

        count = len(all_tis)
        do_it = True
        if count == 0:
            print("Nothing to clear.")
            return 0
        
        # 用户确认
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in all_tis])
            question = (
                "You are about to delete these {} tasks:\n"
                "{}\n\n"
                "Are you sure? (yes/no): ").format(count, ti_list)
            do_it = ask_yesno(question)

        # 确认执行清除逻辑
        if do_it:
            for dag in dags:
                dag.clear(start_date=start_date,
                          end_date=end_date,
                          only_failed=only_failed,
                          only_running=only_running,
                          confirm_prompt=False,
                          include_subdags=include_subdags,
                          reset_dag_runs=reset_dag_runs,
                          dry_run=False,
                          )
        else:
            count = 0
            print("Bail. Nothing was cleared.")
        return count

    def __deepcopy__(self, memo):
        # Swiwtcharoo to go around deepcopying objects coming through the
        # backdoor
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in list(self.__dict__.items()):
            if k not in ('user_defined_macros', 'user_defined_filters', 'params'):
                setattr(result, k, copy.deepcopy(v, memo))

        result.user_defined_macros = self.user_defined_macros
        result.user_defined_filters = self.user_defined_filters
        result.params = self.params
        return result

    def sub_dag(self, task_regex, include_downstream=False,
                include_upstream=True):
        """根据任务的名称的正则表达式，返回当前dag的子集
        默认包含上游任务，补录时从上游任务开始运行，下游任务默认不执行
        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighbours based on the flag passed.
        """
        # 拷贝dag，新的dag的任务列表为空
        dag = copy.deepcopy(self)

        # 根据任务ID模糊匹配，获得匹配的任务
        regex_match = [
            t for t in dag.tasks if re.findall(task_regex, t.task_id)]
        also_include = []
        for t in regex_match:
            # 包含匹配任务的所有下游任务
            if include_downstream:
                also_include += t.get_flat_relatives(upstream=False)
            # 包含匹配任务的所有上游任务
            if include_upstream:
                also_include += t.get_flat_relatives(upstream=True)

        # Compiling the unique list of tasks that made the cut
        dag.task_dict = {t.task_id: t for t in regex_match + also_include}
        # dag.tasks 其实就是 regex_match + also_include
        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # made the cut
            # 通过交集，去掉原有任务的上下游关联任务
            t._upstream_task_ids = t._upstream_task_ids.intersection(dag.task_dict.keys())
            t._downstream_task_ids = t._downstream_task_ids.intersection(
                dag.task_dict.keys())

        # 如果dag子集的任务数量小于父集的数量，则标记子集的partial为True
        if len(dag.tasks) < len(self.tasks):
            dag.partial = True

        return dag

    def has_task(self, task_id):
        return task_id in (t.task_id for t in self.tasks)

    def get_task(self, task_id):
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise AirflowException("Task {task_id} not found".format(**locals()))

    @provide_session
    def pickle_info(self, session=None):
        d = {}
        d['is_picklable'] = True
        try:
            dttm = datetime.now()
            pickled = pickle.dumps(self)
            d['pickle_len'] = len(pickled)
            d['pickling_duration'] = "{}".format(datetime.now() - dttm)
        except Exception as e:
            self.log.debug(e)
            d['is_picklable'] = False
            d['stacktrace'] = traceback.format_exc()
        return d

    @provide_session
    def pickle(self, session=None):
        """将dag对象序列化到db中 ."""
        dag = session.query(
            DagModel).filter(DagModel.dag_id == self.dag_id).first()
        dp = None
        if dag and dag.pickle_id:
            dp = session.query(DagPickle).filter(
                DagPickle.id == dag.pickle_id).first()
        if not dp or dp.pickle != self:
            dp = DagPickle(dag=self)
            session.add(dp)
            self.last_pickled = datetime.now()
            session.commit()
            self.pickle_id = dp.id

        return dp

    def tree_view(self):
        """打印dag中的任务树
        Shows an ascii tree representation of the DAG
        """
        def get_downstream(task, level=0):
            print((" " * level * 4) + str(task))
            level += 1
            for t in task.upstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)

    def add_task(self, task):
        """
        Add a task to the DAG

        :param task: the task you want to add
        :type task: task
        """
        if not self.start_date and not task.start_date:
            raise AirflowException("Task is missing the start_date parameter")
        # if the task has no start date, assign it the same as the DAG
        elif not task.start_date:
            # 如果任务没有设置开始时间，则使用dag的开始时间
            task.start_date = self.start_date
        # otherwise, the task will start on the later of its own start date and
        # the DAG's start date
        elif self.start_date:
            # 如果任务设置了开始时间，则取任务和dag的最大时间
            task.start_date = max(task.start_date, self.start_date)

        # if the task has no end date, assign it the same as the dag
        if not task.end_date:
            # 如果任务结束时间没有设置，则取dag的结束时间
            task.end_date = self.end_date
        # otherwise, the task will end on the earlier of its own end date and
        # the DAG's end date
        elif task.end_date and self.end_date:
            task.end_date = min(task.end_date, self.end_date)

        if task.task_id in self.task_dict:
            # TODO: raise an error in Airflow 2.0
            warnings.warn(
                'The requested task could not be added to the DAG because a '
                'task with task_id {} is already in the DAG. Starting in '
                'Airflow 2.0, trying to overwrite a task will raise an '
                'exception.'.format(task.task_id),
                category=PendingDeprecationWarning)
        else:
            # 将任务添加到dag的任务列表中
            self.task_dict[task.task_id] = task
            task.dag = self

        # 重置任务数量
        self.task_count = len(self.task_dict)

    def add_tasks(self, tasks):
        """
        Add a list of tasks to the DAG

        :param tasks: a lit of tasks you want to add
        :type tasks: list of tasks
        """
        for task in tasks:
            self.add_task(task)

    @provide_session
    def db_merge(self, session=None):
        # 获得所有类型的任务
        BO = BaseOperator
        tasks = session.query(BO).filter(BO.dag_id == self.dag_id).all()
        for t in tasks:
            session.delete(t)
        session.commit()
        session.merge(self)
        session.commit()

    def run(
            self,
            start_date=None,
            end_date=None,
            mark_success=False,
            local=False,
            executor=None,
            donot_pickle=configuration.conf.getboolean('core', 'donot_pickle'),
            ignore_task_deps=False,
            ignore_first_depends_on_past=False,
            pool=None,
            delay_on_limit_secs=1.0,
            verbose=False,
            conf=None,
            rerun_failed_tasks=False,
    ):
        """通过BackfillJob的方式运行一个dag
        Runs the DAG.

        :param start_date: the start date of the range to run
        :type start_date: datetime
        :param end_date: the end date of the range to run
        :type end_date: datetime
        :param mark_success: True to mark jobs as succeeded without running them
        :type mark_success: bool
        :param local: True to run the tasks using the LocalExecutor
        :type local: bool
        :param executor: The executor instance to run the tasks
        :type executor: BaseExecutor
        :param donot_pickle: True to avoid pickling DAG object and send to workers
        :type donot_pickle: bool
        :param ignore_task_deps: True to skip upstream tasks
        :type ignore_task_deps: bool
        :param ignore_first_depends_on_past: True to ignore depends_on_past
            dependencies for the first set of tasks only
        :type ignore_first_depends_on_past: bool
        :param pool: Resource pool to use
        :type pool: string
        :param delay_on_limit_secs: Time in seconds to wait before next attempt to run
            dag run when max_active_runs limit has been reached
        :type delay_on_limit_secs: float
        :param verbose: Make logging output more verbose
        :type verbose: boolean
        :param conf: user defined dictionary passed from CLI
        :type conf: dict
        """
        # 获得执行器
        from airflow.jobs import BackfillJob
        if not executor and local:
            # 使用单机并发执行器 LocalExecutor
            executor = LocalExecutor()
        elif not executor:
            executor = GetDefaultExecutor()
        # 创建job
        job = BackfillJob(
            self,
            # 开始调度时间 (>=)
            start_date=start_date,
            # 结束调度时间 (<=)
            end_date=end_date,
            # 将任务实例标记为成功
            mark_success=mark_success,
            # 选择的执行器
            executor=executor,
            # 是否需要将dag对象序列化到db中，False表示需要，默认打开dag序列化开关，但是如果dag参数中没有配置pickle_id，也不会序列化
            donot_pickle=donot_pickle,
            # 是否忽略任务依赖，默认不跳过上游任务依赖
            ignore_task_deps=ignore_task_deps,
            # 所有补录dagruns的第一个dagrun默认不依赖与上一个dagrun
            ignore_first_depends_on_past=ignore_first_depends_on_past,
            # 任务实例插槽的数量，用于对任务实例的数量进行限制
            pool=pool,
            # 如果dag_run实例超过了阈值，job执行时需要循环等待其他的dag_run运行完成，设置循环的间隔
            delay_on_limit_secs=delay_on_limit_secs,
            verbose=verbose,
            # dagrun运行时配置
            conf=conf,
            # 是否重新运行失败的任务，默认不重新运行
            rerun_failed_tasks=rerun_failed_tasks,
        )
        # 运行job
        job.run()

    def cli(self):
        """
        Exposes a CLI specific to this DAG
        """
        from airflow.bin import cli
        parser = cli.CLIFactory.get_parser(dag_parser=True)
        args = parser.parse_args()
        args.func(args, self)

    @provide_session
    def create_dagrun(self,
                      run_id,
                      state,
                      execution_date=None,
                      start_date=None,
                      external_trigger=False,
                      conf=None,
                      only_create_dagrun=False,
                      session=None):
        """创建dagrun
        Creates a dag run from this dag including the tasks associated with this dag.
        Returns the dag run.

        :param run_id: defines the the run id for this dag run
        :type run_id: string
        :param execution_date: the execution date of this dag run
        :type execution_date: datetime
        :param state: the state of the dag run
        :type state: State
        :param start_date: the date this dag run should be evaluated
        :type start_date: datetime
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param session: database session
        :type session: Session
        """
        # 创建一个dag实例，插入到DB中
        # （dag_id, run_id）是唯一键，需要在调用函数中判断唯一性
        # 注意：同一个调度时间可能有多个不同的run_id
        run = DagRun(
            dag_id=self.dag_id,
            run_id=run_id,
            execution_date=execution_date,
            start_date=start_date,
            external_trigger=external_trigger,
            conf=conf,
            state=state
        )
        session.add(run)
        session.commit()
        
        # 添加dag状态统计记录
        DagStat.set_dirty(dag_id=self.dag_id, session=session)       

        # 因为dag_run中没有设置dag的外键，所以需要显式设置
        run.dag = self

        if not only_create_dagrun:
            # create the associated task instances
            # state is None at the moment of creation
            # 根据dag实例，创建所有的任务实例
            self.log.info("dagrun %s verify_integrity", run_id)
            run.verify_integrity(session=session)

            # 从DB中获取最新的dag_run状态和自增ID
            # SQLAlchemy的ORM方式将数据库中的记录映射成了我们定义好的模型类，
            # 但是带来一个问题是，这些类对象的实例只在数据库会话（session）的生命期内有效，
            # 假如我将数据库会话关闭了，再访问数据表类的对象就会报错。
            #self.log.info("dagrun %s refresh_from_db", run_id)
            #run.refresh_from_db()

        return run

    @provide_session
    def sync_to_db(self, owner=None, sync_time=None, session=None):
        """同步DagModel到db中
        Save attributes about this DAG to the DB. Note that this method
        can be called for both DAGs and SubDAGs. A SubDag is actually a
        SubDagOperator.

        :param dag: the DAG object to save to the DB
        :type dag: DAG
        :param sync_time: The time that the DAG should be marked as sync'ed
        :type sync_time: datetime
        :return: None
        """
        if owner is None:
            owner = self.owner
        if sync_time is None:
            sync_time = datetime.now()

        # 从DB中获取dag，判断dag是否存在
        orm_dag = session.query(
            DagModel).filter(DagModel.dag_id == self.dag_id).first()
        # 如果dag不在db中，则创建一个DagModel
        if not orm_dag:
            orm_dag = DagModel(dag_id=self.dag_id)
            self.log.info("Creating ORM DAG for %s", self.dag_id)
        orm_dag.fileloc = self.fileloc
        orm_dag.is_subdag = self.is_subdag
        orm_dag.owners = owner
        orm_dag.is_active = True
        # 记录上次的运行时间
        orm_dag.last_scheduler_run = sync_time
        session.merge(orm_dag)
        session.commit()

        # 同步子dag到DB中
        for subdag in self.subdags:
            subdag.sync_to_db(owner=owner, sync_time=sync_time, session=session)

    @staticmethod
    @provide_session
    def deactivate_unknown_dags(active_dag_ids, session=None):
        """除了指定的dagid外，其他的dag都删除
        Given a list of known DAGs, deactivate any other DAGs that are
        marked as active in the ORM

        :param active_dag_ids: list of DAG IDs that are active
        :type active_dag_ids: list[unicode]
        :return: None
        """

        if len(active_dag_ids) == 0:
            return
        for dag in session.query(
                DagModel).filter(~DagModel.dag_id.in_(active_dag_ids)).all():
            dag.is_active = False
            session.merge(dag)
        session.commit()

    @staticmethod
    @provide_session
    def deactivate_stale_dags(expiration_date, session=None):
        """根据过期时间，删除过期的dag
        Deactivate any DAGs that were last touched by the scheduler before
        the expiration date. These DAGs were likely deleted.

        :param expiration_date: set inactive DAGs that were touched before this
            time
        :type expiration_date: datetime
        :return: None
        """
        log = LoggingMixin().log
        for dag in session.query(
                DagModel).filter(DagModel.last_scheduler_run < expiration_date,
                                 DagModel.is_active).all():
            log.info(
                "Deactivating DAG ID %s since it was last touched by the scheduler at %s",
                dag.dag_id, dag.last_scheduler_run.isoformat()
            )
            dag.is_active = False
            session.merge(dag)
            session.commit()

    @staticmethod
    @provide_session
    def get_num_task_instances(dag_id, task_ids, states=None, session=None):
        """获得指定的任务实例的数量
        Returns the number of task instances in the given DAG.

        :param session: ORM session
        :param dag_id: ID of the DAG to get the task concurrency of
        :type dag_id: unicode
        :param task_ids: A list of valid task IDs for the given DAG
        :type task_ids: list[unicode]
        :param states: A list of states to filter by if supplied
        :type states: list[state]
        :return: The number of running tasks
        :rtype: int
        
        SELECT count(task_instance.task_id) AS count_1
        FROM task_instance
        WHERE task_instance.dag_id = %s
                AND task_instance.task_id IN (%s)
                AND task_instance.state IN ('running', 'queued')
        """
        begin_time = datetime.now() - timedelta(days=configuration.conf.getint('core', 'sql_query_history_days'))
        
        qry = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id.in_(task_ids),
            TaskInstance.execution_date > begin_time)
        if states is not None:
            if None in states:
                qry = qry.filter(or_(
                    TaskInstance.state.in_(states),
                    TaskInstance.state.is_(None)))
            else:
                qry = qry.filter(TaskInstance.state.in_(states))
        return qry.scalar()

    def test_cycle(self):
        """检验dag中是否有环
        Check to see if there are any cycles in the DAG. Returns False if no cycle found,
        otherwise raises exception.
        """

        # default of int is 0 which corresponds to CYCLE_NEW
        visit_map = defaultdict(int)
        for task_id in self.task_dict.keys():
            # print('starting %s' % task_id)
            if visit_map[task_id] == DagBag.CYCLE_NEW:
                self._test_cycle_helper(visit_map, task_id)
        return False

    def _test_cycle_helper(self, visit_map, task_id):
        """
        Checks if a cycle exists from the input task using DFS traversal
        """

        # print('Inspecting %s' % task_id)
        if visit_map[task_id] == DagBag.CYCLE_DONE:
            return False

        visit_map[task_id] = DagBag.CYCLE_IN_PROGRESS

        task = self.task_dict[task_id]
        # 遍历任务的所有上游任务
        for descendant_id in task.get_direct_relative_ids():
            if visit_map[descendant_id] == DagBag.CYCLE_IN_PROGRESS:
                msg = "Cycle detected in DAG. Faulty task: {0} to {1}".format(
                    task_id, descendant_id)
                raise AirflowDagCycleException(msg)
            else:
                self._test_cycle_helper(visit_map, descendant_id)

        visit_map[task_id] = DagBag.CYCLE_DONE


class Chart(Base):
    __tablename__ = "chart"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    conn_id = Column(String(ID_LEN), nullable=False)
    user_id = Column(Integer(), ForeignKey('users.id'), nullable=True)
    chart_type = Column(String(100), default="line")
    sql_layout = Column(String(50), default="series")
    sql = Column(Text, default="SELECT series, x, y FROM table")
    y_log_scale = Column(Boolean)
    show_datatable = Column(Boolean)
    show_sql = Column(Boolean, default=True)
    height = Column(Integer, default=600)
    default_params = Column(String(5000), default="{}")
    owner = relationship(
        "User", cascade=False, cascade_backrefs=False, backref='charts')
    x_is_date = Column(Boolean, default=True)
    iteration_no = Column(Integer, default=0)
    last_modified = Column(DateTime, default=func.now())

    def __repr__(self):
        return self.label


class KnownEventType(Base):
    """公告类型 ."""
    __tablename__ = "known_event_type"

    id = Column(Integer, primary_key=True)
    know_event_type = Column(String(200))

    def __repr__(self):
        return self.know_event_type


class KnownEvent(Base):
    """公告信息 ."""
    __tablename__ = "known_event"

    id = Column(Integer, primary_key=True)
    # 公告标题
    label = Column(String(200))
    # 公告开始时间
    start_date = Column(DateTime)
    # 公告结束时间
    end_date = Column(DateTime)
    # 公告的用户
    user_id = Column(Integer(), ForeignKey('users.id'),)
    # 公告事件类型
    known_event_type_id = Column(Integer(), ForeignKey('known_event_type.id'),)
    reported_by = relationship(
        "User", cascade=False, cascade_backrefs=False, backref='known_events')
    event_type = relationship(
        "KnownEventType",
        cascade=False,
        cascade_backrefs=False, backref='known_events')
    description = Column(Text)

    def __repr__(self):
        return self.label


class Variable(Base, LoggingMixin):
    __tablename__ = "variable"

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    _val = Column('val', Text)
    is_encrypted = Column(Boolean, unique=False, default=False)

    def __repr__(self):
        # Hiding the value
        return '{} : {}'.format(self.key, self._val)

    def get_val(self):
        log = LoggingMixin().log
        if self._val and self.is_encrypted:
            try:
                fernet = get_fernet()
                return fernet.decrypt(bytes(self._val, 'utf-8')).decode()
            except InvalidFernetToken:
                log.error("Can't decrypt _val for key={}, invalid token "
                          "or value".format(self.key))
                return None
            except Exception:
                log.error("Can't decrypt _val for key={}, FERNET_KEY "
                          "configuration missing".format(self.key))
                return None
        else:
            return self._val

    def set_val(self, value):
        if value:
            fernet = get_fernet()
            self._val = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def val(cls):
        return synonym('_val',
                       descriptor=property(cls.get_val, cls.set_val))

    @classmethod
    def setdefault(cls, key, default, deserialize_json=False):
        """
        Like a Python builtin dict object, setdefault returns the current value
        for a key, and if it isn't there, stores the default value and returns it.

        :param key: Dict key for this Variable
        :type key: String
        :param default: Default value to set and return if the variable
        isn't already in the DB
        :type default: Mixed
        :param deserialize_json: Store this as a JSON encoded value in the DB
         and un-encode it when retrieving a value
        :return: Mixed
        """
        default_sentinel = object()
        obj = Variable.get(key, default_var=default_sentinel,
                           deserialize_json=deserialize_json)
        # 如果是空的json对象
        if obj is default_sentinel:
            if default is not None:
                Variable.set(key, default, serialize_json=deserialize_json)
                return default
            else:
                raise ValueError('Default Value must be set')
        else:
            return obj

    @classmethod
    @provide_session
    def get(cls, key, default_var=None, deserialize_json=False, session=None):
        obj = session.query(cls).filter(cls.key == key).first()
        # 对象不存在获取默认值
        if obj is None:
            if default_var is not None:
                return default_var
            else:
                raise KeyError('Variable {} does not exist'.format(key))
        else:
            # 是否存储的是json数据
            if deserialize_json:
                return json.loads(obj.val)
            else:
                return obj.val

    @classmethod
    @provide_session
    def set(cls, key, value, serialize_json=False, session=None):

        if serialize_json:
            stored_value = json.dumps(value)
        else:
            stored_value = str(value)

        session.query(cls).filter(cls.key == key).delete()
        session.add(Variable(key=key, val=stored_value))
        # TODO 写数据到数据库，但是不commit，其它的事务无法看到这个更新后的结果
        # 作用是防止其它事务获取到正在修改的值
        session.flush()


# To avoid circular import on Python2.7 we need to define this at the _bottom_
from airflow.models.connection import Connection  # noqa: E402,F401
from airflow.models.skipmixin import SkipMixin  # noqa: F401
