# -*- coding: utf-8 -*-

import os
import logging
import getpass
import hashlib
import copy
import warnings
import signal
import functools
from datetime import datetime, timedelta

import dill
from sqlalchemy import Column, Integer, String, Float, PickleType, Index
from sqlalchemy import DateTime
from sqlalchemy.orm import reconstructor, relationship, synonym

from airflow.models.base import Base
from airflow.models.xcom import XCom
from airflow.models.log import Log
from airflow.models.dagrun import DagRun
from airflow import settings
from airflow.models.base import Base, ID_LEN, XCOM_RETURN_KEY
from airflow import configuration
from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS

from xTool.exceptions import XToolConfigException
from xTool.utils.state import State
from xTool.utils.net import get_hostname
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.misc import USE_WINDOWS
from xTool.decorators.db import provide_session


Stats = settings.Stats


class TaskInstance(Base, LoggingMixin):
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50))
    queue = Column(String(50))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(DateTime)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_state', state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
        Index('ti_pool', pool, state, priority_weight),
        Index('ti_job_id', job_id),
    )

    def __init__(self, task, execution_date, state=None):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.task = task
        self._log = logging.getLogger("airflow.task")
        self.execution_date = execution_date

        self.queue = task.queue
        # 任务插槽，用于对任务实例的数量进行限制
        self.pool = task.pool
        self.priority_weight = task.priority_weight_total
        # 记录了第几次重试
        self.try_number = 0
        # 最大重试次数
        self.max_tries = self.task.retries
        # 获得linux当前用户
        self.unixname = getpass.getuser()
        # 任务设置的执行用户
        self.run_as_user = task.run_as_user
        # 任务实例的初始状态为None
        if state:
            self.state = state
        self.hostname = ''
        self.executor_config = task.executor_config
        self.init_on_load()
        # Is this TaskInstance being currently running within `airflow run --raw`.
        # Not persisted to the database so only valid for the current process
        self.raw = False

    @reconstructor
    def init_on_load(self):
        """ Initialize the attributes that aren't stored in the DB. """
        self.test_mode = False  # can be changed when calling 'run'

    @property
    def try_number(self):
        """
        Return the try number that this task number will be when it is acutally
        run.

        If the TI is currently running, this will match the column in the
        databse, in all othercases this will be incremenetd
        """
        # This is designed so that task logs end up in the right file.
        if self.state == State.RUNNING:
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value):
        self._try_number = value

    @property
    def next_try_number(self):
        return self._try_number + 1

    def command(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """将任务实例转换为可执行的shell命令
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        return " ".join(self.command_as_list(
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path))

    def command_as_list(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_task_deps=False,
            ignore_depends_on_past=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        dag = self.task.dag

        # 如果没有设置pickle_id，则获取dag文件的绝对地址
        # 如果设置了pickle_id，则从其他存储介质中获取
        should_pass_filepath = not pickle_id and dag
        if should_pass_filepath and dag.full_filepath != dag.filepath:
            path = "DAGS_FOLDER/{}".format(dag.filepath)
        elif should_pass_filepath and dag.full_filepath:
            path = dag.full_filepath
        else:
            path = None

        return TaskInstance.generate_command(
            self.dag_id,
            self.task_id,
            self.execution_date,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            file_path=path,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path)

    @staticmethod
    def generate_command(dag_id,
                         task_id,
                         execution_date,
                         mark_success=False,
                         ignore_all_deps=False,
                         ignore_depends_on_past=False,
                         ignore_task_deps=False,
                         ignore_ti_state=False,
                         local=False,
                         pickle_id=None,
                         file_path=None,
                         raw=False,
                         job_id=None,
                         pool=None,
                         cfg_path=None
                         ):
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :type dag_id: unicode
        :param task_id: Task ID
        :type task_id: unicode
        :param execution_date: Execution date for the task
        :type execution_date: datetime
        :param mark_success: Whether to mark the task as successful
        :type mark_success: bool
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :type ignore_all_deps: boolean
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :type ignore_depends_on_past: boolean
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :type ignore_task_deps: boolean
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :type ignore_ti_state: boolean
        :param local: Whether to run the task locally
        :type local: bool
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :type pickle_id: unicode
        :param file_path: path to the file containing the DAG definition
        :param raw: raw mode (needs more details)
        :param job_id: job ID (needs more details)
        :param pool: the Airflow pool that the task should run in
        :type pool: unicode
        :param cfg_path: the Path to the configuration file
        :type cfg_path: basestring
        :return: shell command that can be used to run the task instance
        """
        if USE_WINDOWS:
            if file_path:
                file_path = file_path.replace("\\", "/")
            if cfg_path:
                cfg_path = cfg_path.replace("\\", "/")
        iso = execution_date.isoformat()
        if USE_WINDOWS:
            # 需要在环境变量->系统变量中设置 PYTHON_SCRIPT=C:/Users/user/AppData/Local/Programs/Python/Python36-32/Scripts
            cmd = ["python3.6", "%PYTHON_SCRIPT%/airflow", "run", str(dag_id), str(task_id), str(iso)]
        else:
            cmd = ["airflow", "run", str(dag_id), str(task_id), str(iso)]
        cmd.extend(["--mark_success"]) if mark_success else None
        cmd.extend(["--pickle", str(pickle_id)]) if pickle_id else None
        cmd.extend(["--job_id", str(job_id)]) if job_id else None
        cmd.extend(["-A"]) if ignore_all_deps else None
        cmd.extend(["-i"]) if ignore_task_deps else None
        cmd.extend(["-I"]) if ignore_depends_on_past else None
        cmd.extend(["--force"]) if ignore_ti_state else None
        cmd.extend(["--local"]) if local else None
        cmd.extend(["--pool", pool]) if pool else None
        cmd.extend(["--raw"]) if raw else None
        cmd.extend(["-sd", file_path]) if file_path else None
        cmd.extend(["--cfg_path", cfg_path]) if cfg_path else None
        return cmd

    @property
    def log_filepath(self):
        """获得任务实例日志文件路径 ."""
        iso = self.execution_date.isoformat()
        log = os.path.expanduser(configuration.conf.get('core', 'BASE_LOG_FOLDER'))
        return (
            "{log}/{self.dag_id}/{self.task_id}/{iso}.log".format(**locals()))

    @property
    def log_url(self):
        """获得任务实例日志的url ."""
        iso = quote(self.execution_date.isoformat())
        BASE_URL = configuration.conf.get('webserver', 'BASE_URL')
        if settings.RBAC:
            return BASE_URL + (
                "/log?"
                "execution_date={iso}"
                "&task_id={self.task_id}"
                "&dag_id={self.dag_id}"
            ).format(**locals())
        else:
            return BASE_URL + (
                "/admin/airflow/log"
                "?dag_id={self.dag_id}"
                "&task_id={self.task_id}"
                "&execution_date={iso}"
            ).format(**locals())

    @property
    def mark_success_url(self):
        iso = quote(self.execution_date.isoformat())
        BASE_URL = configuration.conf.get('webserver', 'BASE_URL')
        if settings.RBAC:
            return BASE_URL + (
                "/success"
                "?task_id={self.task_id}"
                "&dag_id={self.dag_id}"
                "&execution_date={iso}"
                "&upstream=false"
                "&downstream=false"
            ).format(**locals())
        else:
            return BASE_URL + (
                "/admin/airflow/success"
                "?task_id={self.task_id}"
                "&dag_id={self.dag_id}"
                "&execution_date={iso}"
                "&upstream=false"
                "&downstream=false"
            ).format(**locals())

    @provide_session
    def current_state(self, session=None):
        """从DB中获取任务实例的当前状态
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.
        """
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.task_id == self.task_id,
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
        ).all()
        if ti:
            state = ti[0].state
        else:
            state = None
        return state

    @provide_session
    def error(self, session=None):
        """将任务实例设置为失败状态
        Forces the task instance's state to FAILED in the database.
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()

    @provide_session
    def refresh_from_db(self, session=None, lock_for_update=False):
        """将db中任务实例记录更新到orm模型
        Refreshes the task instance from the database based on the primary key

        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
            
        SELECT *
        FROM task_instance
        WHERE task_instance.task_id = %s
                AND task_instance.dag_id = %s
                AND task_instance.execution_date = %s LIMIT 1
        """
        TI = TaskInstance

        # 从DB中获取任务实例
        qry = session.query(TI).filter(
            TI.task_id == self.task_id,
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date)

        # 添加行锁
        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()

        # 如果实例存在，将db数据更新到orm
        if ti:
            self.state = ti.state
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            # Get the raw value of try_number column, don't read through the
            # accessor here otherwise it will be incremeneted by one already.
            self.try_number = ti._try_number
            self.max_tries = ti.max_tries
            self.hostname = ti.hostname
            self.pid = ti.pid
            self.executor_config = ti.executor_config
        else:
            # 如果任务实例不存在，设置任务实例状态为None
            self.state = None

    @provide_session
    def clear_xcom_data(self, session=None):
        """
        Clears all XCom data from the database for the task instance
        """
        session.query(XCom).filter(
            XCom.dag_id == self.dag_id,
            XCom.task_id == self.task_id,
            XCom.execution_date == self.execution_date
        ).delete()
        session.commit()

    @property
    def key(self):
        """返回唯一键
        Returns a tuple that identifies the task instance uniquely
        """
        return self.dag_id, self.task_id, self.execution_date, self.try_number

    @provide_session
    def set_state(self, state, session=None):
        """变更任务实例的状态，并提交到DB中 ."""
        self.state = state
        self.start_date = datetime.now()
        self.end_date = datetime.now()
        session.merge(self)
        session.commit()

    @property
    def is_premature(self):
        """判断任务重试时间还没有到达
        Returns whether a task is in UP_FOR_RETRY state and its retry interval
        has elapsed.
        """
        # is the task still in the retry waiting period?
        return self.state == State.UP_FOR_RETRY and not self.ready_for_retry()

    @provide_session
    def are_dependents_done(self, session=None):
        """验证当前调度的任务实例的所有下游任务实例是否都执行完成
        Checks whether the dependents of this task instance have all succeeded.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.
        """
        task = self.task

        # 判断任务是否有下游任务
        if not task.downstream_task_ids:
            return True

        # 获得下游任务实例的状态为success的数量
        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state == State.SUCCESS,
        )
        count = ti[0][0]
        # 验证当前调度的任务实例的所有下游任务实例是否都执行完成
        return count == len(task.downstream_task_ids)

    @property
    @provide_session
    def previous_ti(self, session=None):
        """根据当前任务实例获得上一个调度的同一个任务的任务实例
        The task instance for the task that ran before this task instance """

        dag = self.task.dag
        if dag:
            # 获得当前任务实例关联的dag_run，最多有一个
            dr = self.get_dagrun(session=session)

            # LEGACY: most likely running from unit tests
            if not dr:
                # Means that this TI is NOT being run from a DR, but from a catchup
                # 如果是单次调度，返回None
                previous_scheduled_date = dag.previous_schedule(self.execution_date)
                if not previous_scheduled_date:
                    return None
                # 返回上一个任务实例
                return TaskInstance(task=self.task,
                                    execution_date=previous_scheduled_date)

            # Note: 因为dag_run的dag属性默认为None
            dr.dag = dag
            # 获得上一个调度的dag实例
            if dag.catchup:
                # 获得上一个调度的dag实例
                last_dagrun = dr.get_previous_scheduled_dagrun(session=session)
            else:
                # 获得上一个dag实例
                last_dagrun = dr.get_previous_dagrun(session=session)

            if last_dagrun:
                # 获得上一个任务实例
                return last_dagrun.get_task_instance(self.task_id, session=session)

        return None

    @provide_session
    def are_dependencies_met(
            self,
            dep_context=None,
            session=None,
            verbose=False):
        """判断依赖是否满足
        Returns whether or not all the conditions are met for this task instance to be run
        given the context for the dependencies (e.g. a task instance being force run from
        the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that
            should be evaluated.
        :type dep_context: DepContext
        :param session: database session
        :type session: Session
        :param verbose: whether log details on failed dependencies on
            info or debug log level
        :type verbose: boolean
        """
        # 获得DAG上下文
        dep_context = dep_context or DepContext()
        failed = False
        # 根据DAG/TASK上下文中的依赖，返回失败的依赖
        verbose_aware_logger = self.log.info if verbose else self.log.debug
        # 遍历失败的依赖
        for dep_status in self.get_failed_dep_statuses(
                dep_context=dep_context,
                session=session):
            failed = True

            verbose_aware_logger(
                "Dependencies not met for %s, dependency '%s' FAILED: %s",
                self, dep_status.dep_name, dep_status.reason
            )

        if failed:
            return False

        # 依赖全部满足，返回True
        verbose_aware_logger("Dependencies all met for %s", self)
        return True

    @provide_session
    def get_failed_dep_statuses(
            self,
            dep_context=None,
            session=None):
        """获取失败的依赖  ."""
        # 获得DAG上下文
        dep_context = dep_context or DepContext()
        # 默认的依赖self.task.deps 为
        #            # 验证重试时间： 任务实例已经标记为重试，但是还没有到下一次重试时间，如果运行就会失败
        #            NotInRetryPeriodDep(),
        #            # 验证任务实例是否依赖上一个周期的任务实例
        #            PrevDagrunDep(),
        #            # 验证上游依赖任务
        #            TriggerRuleDep(),
        for dep in dep_context.deps | self.task.deps:
            for dep_status in dep.get_dep_statuses(
                    self,
                    session,
                    dep_context):

                self.log.debug(
                    "%s dependency '%s' PASSED: %s, %s",
                    self, dep_status.dep_name, dep_status.passed, dep_status.reason
                )

                if not dep_status.passed:
                    # 返回失败的依赖
                    yield dep_status

    def __repr__(self):
        return (
            "<TaskInstance: {ti.dag_id}.{ti.task_id} "
            "{ti.execution_date} [{ti.state}]>"
        ).format(ti=self)

    def next_retry_datetime(self):
        """获得下一次重试时间
        Get datetime of the next retry if the task instance fails. For exponential
        backoff, retry_delay is used as base and will be converted to seconds.
        """
        delay = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            # 每次间隔的时间递增
            min_backoff = int(delay.total_seconds() * (2 ** (self.try_number - 2)))
            # deterministic per task instance
            # between 0.5 * delay * (2^retry_number) and 1.0 * delay *
            # (2^retry_number)
            hash = int(hashlib.sha1("{}#{}#{}#{}".format(self.dag_id,
                                                         self.task_id,
                                                         self.execution_date,
                                                         self.try_number)
                                    .encode('utf-8')).hexdigest(), 16)
            # between 0.5 * delay * (2^retry_number) and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail.
            delay_backoff_in_seconds = min(
                modded_hash,
                timedelta.max.total_seconds() - 1
            )
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        # 任务失败的时间 + 偏移
        return self.end_date + delay

    def ready_for_retry(self):
        """判断是否可以进行重试操作
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return (self.state == State.UP_FOR_RETRY and
                self.next_retry_datetime() < datetime.now())

    @provide_session
    def pool_full(self, session):
        """判断任务实例的插槽是否已满
        Returns a boolean as to whether the slot pool has room for this
        task to run
        """
        if not self.task.pool:
            return False

        pool = (
            session
            .query(Pool)
            .filter(Pool.pool == self.task.pool)
            .first()
        )
        if not pool:
            return False
        # 判断任务实例可用的插槽数量
        open_slots = pool.open_slots(session=session)

        return open_slots <= 0

    @provide_session
    def get_dagrun(self, session):
        """获得当前任务实例关联的dag_run，最多有一个
        Returns the DagRun for this TaskInstance

        :param session:
        :return: DagRun
        """
        dr = session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == self.execution_date
        ).first()

        return dr

    @provide_session
    def _check_and_change_state_before_execution(
            self,
            verbose=True,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """任务实例执行前的验证
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :type verbose: boolean
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :type ignore_all_deps: boolean
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :type ignore_depends_on_past: boolean
        :param ignore_task_deps: Don't check the dependencies of this TI's task
        :type ignore_task_deps: boolean
        :param ignore_ti_state: Disregards previous task instance state
        :type ignore_ti_state: boolean
        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: boolean
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: boolean
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task
        self.pool = pool or task.pool
        # 是否是测试模式
        self.test_mode = test_mode
        # 将db中任务实例记录更新到orm模型
        self.refresh_from_db(session=session, lock_for_update=True)
        # 任务实例中获得job_id
        self.job_id = job_id
        # 获得任务实例运行所在机器的主机名
        try:
            callable_path = configuration.conf.get('core', 'hostname_callable')
        except XToolConfigException:
            callable_path = None        
        self.hostname = get_hostname(callable_path)
        self.operator = task.__class__.__name__

        if not ignore_all_deps and not ignore_ti_state and self.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        queue_dep_context = DepContext(
            deps=QUEUE_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_ti_state=ignore_ti_state,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps)
        # 判断任务是否在队列中
        if not self.are_dependencies_met(
                dep_context=queue_dep_context,
                session=session,
                verbose=True):
            session.commit()
            return False

        # TODO: Logging needs cleanup, not clear what is being printed
        hr = "\n" + ("-" * 80) + "\n"  # Line break

        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Attempt 1 instead of
        # Attempt 0 for the first attempt).
        msg = "Starting attempt {attempt} of {total}".format(
            attempt=self.try_number,
            total=self.max_tries + 1)
        self.start_date = datetime.now()

        # 验证任务实例是否满足并发限制
        dep_context = DepContext(
            deps=RUN_DEPS - QUEUE_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)
        runnable = self.are_dependencies_met(
            dep_context=dep_context,
            session=session,
            verbose=True)

        # 如果任务超出了并发限制，则调度器需要重新调度这个任务实例
        if not runnable and not mark_success:
            # FIXME: we might have hit concurrency limits, which means we probably
            # have been running prematurely. This should be handled in the
            # scheduling mechanism.
            self.state = State.NONE
            msg = ("FIXME: Rescheduling due to concurrency limits reached at task "
                   "runtime. Attempt {attempt} of {total}. State set to NONE.").format(
                attempt=self.try_number,
                total=self.max_tries + 1)
            self.log.warning(hr + msg + hr)

            self.queued_dttm = datetime.now()
            self.log.info("Queuing into pool %s", self.pool)
            session.merge(self)
            session.commit()
            return False

        # Another worker might have started running this task instance while
        # the current worker process was blocked on refresh_from_db
        if self.state == State.RUNNING:
            msg = "Task Instance already running {}".format(self)
            self.log.warning(msg)
            session.commit()
            return False

        # 如果任务在队列中，且满足并发要求，可以执行

        # print status message
        self.log.info(hr + msg + hr)
        self._try_number += 1

        # 测试模式不记录日志
        if not test_mode:
            session.add(Log(State.RUNNING, self))
        # 修改任务实例状态为执行中，并记录进程ID
        self.state = State.RUNNING
        self.pid = os.getpid()
        self.end_date = None
        # 测试模式不修改任务实例的值
        if not test_mode:
            # 更新任务实例
            session.merge(self)
        session.commit()

        # 关闭DB连接池，因为在后续的LocalTaskJob处理中不再需要DB操作了
        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()
        if verbose:
            if mark_success:
                msg = "Marking success for {} on {}".format(self.task,
                                                            self.execution_date)
                self.log.info(msg)
            else:
                msg = "Executing {} on {}".format(self.task, self.execution_date)
                self.log.info(msg)
        return True

    @provide_session
    def _run_raw_task(
            self,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """不经过调度器而直接执行operator
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: boolean
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: boolean
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session)
        self.job_id = job_id
        try:
            callable_path = conf.get('core', 'hostname_callable')
        except XToolConfigException:
            callable_path = None         
        self.hostname = get_hostname(callable_path)
        self.operator = task.__class__.__name__

        context = {}
        try:
            if not mark_success:
                # 获得运行时上下文
                context = self.get_template_context()
                # 备份任务实例关联的任务
                task_copy = copy.copy(task)
                self.task = task_copy

                # 任务实例收到终止信号后，执行on_kill()，停止子进程
                def signal_handler(signum, frame):
                    self.log.error("Received SIGTERM. Terminating subprocesses.")
                    task_copy.on_kill()
                    raise AirflowException("Task received SIGTERM signal")
                signal.signal(signal.SIGTERM, signal_handler)

                # 删除中间结果
                # Don't clear Xcom until the task is certain to execute
                self.clear_xcom_data()

                # 渲染模板，设置任务属性
                self.render_templates()

                # 任务执行前的预处理操作hook
                task_copy.pre_execute(context=context)

                # If a timeout is specified for the task, make it fail
                # if it goes beyond
                # 判断任务是否执行超时
                result = None
                if task_copy.execution_timeout:
                    try:
                        with timeout(int(
                                task_copy.execution_timeout.total_seconds())):
                            # 执行任务
                            result = task_copy.execute(context=context)
                    except XToolTimeoutError:
                        # 任务超时后执行on_kill()
                        task_copy.on_kill()
                        raise
                else:
                    # 执行任务
                    result = task_copy.execute(context=context)

                # 如果任务实例有返回，则保持结果到中间表
                # If the task returns a result, push an XCom containing it
                if result is not None:
                    self.xcom_push(key=XCOM_RETURN_KEY, value=result)

                # TODO remove deprecated behavior in Airflow 2.0
                # 任务执行完毕后的处理操作hook
                try:
                    task_copy.post_execute(context=context, result=result)
                except TypeError as e:
                    if 'unexpected keyword argument' in str(e):
                        warnings.warn(
                            'BaseOperator.post_execute() now takes two '
                            'arguments, `context` and `result`, but "{}" only '
                            'expected one. This behavior is deprecated and '
                            'will be removed in a future version of '
                            'Airflow.'.format(self.task_id),
                            category=DeprecationWarning)
                        task_copy.post_execute(context=context)
                    else:
                        raise

                Stats.incr('operator_successes_{}'.format(
                    self.task.__class__.__name__), 1, 1)
                Stats.incr('ti_successes')
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSkipException:
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success externally
            # current behavior doesn't hit the success callback
            # 如果任务执行失败，但是用户在页面上强行标记了任务为成功
            if self.state == State.SUCCESS:
                return
            else:
                # 任务执行失败，发送通知，并执行失败回调函数
                self.handle_failure(e, test_mode, context)
                raise
        except (Exception, KeyboardInterrupt) as e:
            # 任务执行失败，发送通知，并执行失败回调函数
            self.handle_failure(e, test_mode, context)
            raise

        # Success callback
        # 执行成功回调函数
        try:
            if task.on_success_callback:
                task.on_success_callback(context)
        except Exception as e3:
            self.log.error("Failed when executing success callback")
            self.log.exception(e3)

        # Recording SUCCESS
        # 记录任务执行完成后的时间
        self.end_date = datetime.now()
        self.set_duration()
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)
        session.commit()

    @provide_session
    def run(
            self,
            verbose=True,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        # 任务执行前需要进行验证
        res = self._check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            # 测试模式
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            session=session)
        # 如果满足可执行条件，则执行任务
        if res:
            self._run_raw_task(
                mark_success=mark_success,
                test_mode=test_mode,
                job_id=job_id,
                pool=pool,
                session=session)

    def dry_run(self):
        """模拟运行任务实例，仅仅渲染模板参数 ."""
        # 拷贝任务
        task = self.task
        task_copy = copy.copy(task)
        self.task = task_copy
        # 渲染任务模版
        self.render_templates()
        task_copy.dry_run()

    @provide_session
    def handle_failure(self, error, test_mode=False, context=None, session=None):
        """任务实例失败处理函数 ."""
        self.log.exception(error)
        task = self.task
        self.end_date = datetime.now()
        self.set_duration()
        Stats.incr('operator_failures_{}'.format(task.__class__.__name__), 1, 1)
        Stats.incr('ti_failures')
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Log failure duration
        # 记录失败的任务实例
        session.add(TaskFail(task, self.execution_date, self.start_date, self.end_date))

        if context is not None:
            context['exception'] = error

        # Let's go deeper
        try:
            # Since this function is called only when the TI state is running,
            # try_number contains the current try_number (not the next). We
            # only mark task instance as FAILED if the next task instance
            # try_number exceeds the max_tries.
            if self.is_eligible_to_retry():
                self.state = State.UP_FOR_RETRY
                self.log.info('Marking task as UP_FOR_RETRY')
                # 重试时发送邮件
                if task.email_on_retry and task.email:
                    self.email_alert(error, is_retry=True)
            else:
                self.state = State.FAILED
                if task.retries:
                    self.log.info('All retries failed; marking task as FAILED')
                else:
                    self.log.info('Marking task as FAILED.')
                # 任务失败时发送邮件
                if task.email_on_failure and task.email:
                    self.email_alert(error, is_retry=False)
        except Exception as e2:
            self.log.error('Failed to send email to: %s', task.email)
            self.log.exception(e2)

        # Handling callbacks pessimistically
        try:
            # 执行重试回调
            if self.state == State.UP_FOR_RETRY and task.on_retry_callback:
                task.on_retry_callback(context)
            # 执行失败回调
            if self.state == State.FAILED and task.on_failure_callback:
                task.on_failure_callback(context)
        except Exception as e3:
            self.log.error("Failed at executing callback")
            self.log.exception(e3)

        if not test_mode:
            session.merge(self)
        session.commit()

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry"""
        return self.task.retries and self.try_number <= self.max_tries

    @provide_session
    def get_template_context(self, session=None):
        task = self.task
        from airflow import macros
        # 从参数中获取tables
        tables = None
        if 'tables' in task.params:
            tables = task.params['tables']

        # 获得调度时间相关变量
        ds = self.execution_date.strftime('%Y-%m-%d')
        ts = self.execution_date.isoformat()
        yesterday_ds = (self.execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (self.execution_date + timedelta(1)).strftime('%Y-%m-%d')

        # 获得上一个调度和下一个调度时间
        prev_execution_date = task.dag.previous_schedule(self.execution_date)
        next_execution_date = task.dag.following_schedule(self.execution_date)

        next_ds = None
        if next_execution_date:
            next_ds = next_execution_date.strftime('%Y-%m-%d')

        prev_ds = None
        if prev_execution_date:
            prev_ds = prev_execution_date.strftime('%Y-%m-%d')

        ds_nodash = ds.replace('-', '')
        ts_nodash = ts.replace('-', '').replace(':', '')
        yesterday_ds_nodash = yesterday_ds.replace('-', '')
        tomorrow_ds_nodash = tomorrow_ds.replace('-', '')

        ti_key_str = "{task.dag_id}__{task.task_id}__{ds_nodash}"
        ti_key_str = ti_key_str.format(**locals())

        params = {}
        run_id = ''
        dag_run = None
        if hasattr(task, 'dag'):
            if task.dag.params:
                params.update(task.dag.params)
            # 获得dag_run的运行run_id
            dag_run = (
                session.query(DagRun)
                .filter_by(
                    dag_id=task.dag.dag_id,
                    execution_date=self.execution_date)
                .first()
            )
            run_id = dag_run.run_id if dag_run else None
            session.expunge_all()
            session.commit()

        if task.params:
            params.update(task.params)

        if configuration.getboolean('core', 'dag_run_conf_overrides_params'):
            self.overwrite_params_with_dag_run_conf(params=params, dag_run=dag_run)

        class VariableAccessor:
            """
            Wrapper around Variable. This way you can get variables in templates by using
            {var.value.your_variable_name}.
            """

            def __init__(self):
                self.var = None

            def __getattr__(self, item):
                self.var = Variable.get(item)
                return self.var

            def __repr__(self):
                return str(self.var)

        class VariableJsonAccessor:
            """
            Wrapper around deserialized Variables. This way you can get variables
            in templates by using {var.json.your_variable_name}.
            """

            def __init__(self):
                self.var = None

            def __getattr__(self, item):
                self.var = Variable.get(item, deserialize_json=True)
                return self.var

            def __repr__(self):
                return str(self.var)

        return {
            'dag': task.dag,
            'ds': ds,
            'next_ds': next_ds,
            'prev_ds': prev_ds,
            'ds_nodash': ds_nodash,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'yesterday_ds': yesterday_ds,
            'yesterday_ds_nodash': yesterday_ds_nodash,
            'tomorrow_ds': tomorrow_ds,
            'tomorrow_ds_nodash': tomorrow_ds_nodash,
            'END_DATE': ds,
            'end_date': ds,
            'dag_run': dag_run,
            'run_id': run_id,
            'execution_date': self.execution_date,
            'prev_execution_date': prev_execution_date,
            'next_execution_date': next_execution_date,
            'latest_date': ds,
            'macros': macros,
            'params': params,
            'tables': tables,
            'task': task,
            'task_instance': self,
            'ti': self,
            'task_instance_key_str': ti_key_str,
            'conf': configuration,
            'test_mode': self.test_mode,
            'var': {
                'value': VariableAccessor(),
                'json': VariableJsonAccessor()
            },
            'inlets': task.inlets,
            'outlets': task.outlets,
        }

    def overwrite_params_with_dag_run_conf(self, params, dag_run):
        if dag_run and dag_run.conf:
            params.update(dag_run.conf)

    def render_templates(self):
        task = self.task
        jinja_context = self.get_template_context()
        if hasattr(self, 'task') and hasattr(self.task, 'dag'):
            if self.task.dag.user_defined_macros:
                jinja_context.update(
                    self.task.dag.user_defined_macros)

        rt = self.task.render_template  # shortcut to method
        for attr in task.__class__.template_fields:
            content = getattr(task, attr)
            if content:
                rendered_content = rt(attr, content, jinja_context)
                setattr(task, attr, rendered_content)

    def email_alert(self, exception, is_retry=False):
        task = self.task
        title = "Airflow alert: {self}".format(**locals())
        exception = str(exception).replace('\n', '<br>')
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        body = (
            "Try {try_number} out of {max_tries}<br>"
            "Exception:<br>{exception}<br>"
            "Log: <a href='{self.log_url}'>Link</a><br>"
            "Host: {self.hostname}<br>"
            "Log file: {self.log_filepath}<br>"
            "Mark success: <a href='{self.mark_success_url}'>Link</a><br>"
        ).format(try_number=self.try_number, max_tries=self.max_tries + 1, **locals())
        send_email(task.email, title, body)

    def set_duration(self):
        """设置任务执行的多久 ."""
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None

    def xcom_push(
            self,
            key,
            value,
            execution_date=None):
        """
        Make an XCom available for tasks to pull.

        :param key: A key for the XCom
        :type key: string
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :type value: any pickleable object
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        :type execution_date: datetime
        """

        if execution_date and execution_date < self.execution_date:
            raise ValueError(
                'execution_date can not be in the past (current '
                'execution_date is {}; received {})'.format(
                    self.execution_date, execution_date))

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            execution_date=execution_date or self.execution_date)

    def xcom_pull(
            self,
            task_ids=None,
            dag_id=None,
            key=XCOM_RETURN_KEY,
            include_prior_dates=False):
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :type key: string
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: string or iterable of strings (representing task_ids)
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: string
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        """

        if dag_id is None:
            dag_id = self.dag_id

        pull_fn = functools.partial(
            XCom.get_one,
            execution_date=self.execution_date,
            key=key,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates)

        if is_container(task_ids):
            return tuple(pull_fn(task_id=t) for t in task_ids)
        else:
            return pull_fn(task_id=task_ids)

    @provide_session
    def get_num_running_task_instances(self, session):
        """获得正在运行的任务实例的数量 ."""
        TI = TaskInstance
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.state == State.RUNNING
        ).count()

    def init_run_context(self, raw=False):
        """设置日志的上下文为任务实例
        Sets the log context.
        """
        self.raw = raw
        self._set_context(self)
