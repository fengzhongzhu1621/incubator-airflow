# -*- coding: utf-8 -*-

import re
import copy
import traceback
import sys
import functools
import warnings
from datetime import datetime, timedelta
from collections import defaultdict

import six
from six import iterkeys, itervalues, iteritems
from sqlalchemy import func, or_

from airflow.models.dagmodel import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.dagstat import DagStat
from airflow.models.taskinstance import TaskInstance
from airflow.models import base
from airflow import settings
from airflow import configuration
from airflow.dag.base_dag import BaseDag

from xTool.decorators.db import provide_session
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.state import State
from xTool.utils.helpers import validate_key
from xTool.utils.dates import cron_presets, date_range as utils_date_range
from xTool.algorithms.graphs.sort import topological_sort as graph_topological_sort
from xTool.exceptions import XToolException


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
        self._old_context_manager_dag = base._CONTEXT_MANAGER_DAG
        base._CONTEXT_MANAGER_DAG = self
        return self

    def __exit__(self, _type, _value, _tb):
        base._CONTEXT_MANAGER_DAG = self._old_context_manager_dag

    # /Context Manager ----------------------------------------------

    def date_range(self, start_date, num=None, end_date=datetime.now()):
        """根据调度配置获取dag实例运行的时间范围."""
        if num:
            end_date = None
        return utils_date_range(
            start_date=start_date, end_date=end_date,
            num=num, delta=self._schedule_interval)

    def is_fixed_time_schedule(self):
        """判断调度时间是否是固定时间
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
            # 获得下一次调度时间
            cron = croniter(self._schedule_interval, dttm)
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
            # 获得上一次调度时间
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
        # 获得下一次调度时间，如果是单次调度，因为self._schedule_interval为None，所以函数默认返回None
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
        # 按调度时间逆序
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
        """获得文件的绝对路径 ."""
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
        """获得dag包含的所有任务列表 ."""
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
        """获得dag包含的所有的有效任务Ids，不包括坐席任务 ."""
        return list(task_id for task_id, task in iteritems(self.task_dict) if not task.adhoc)

    @property
    def active_tasks(self):
        """获得dag包含的所有的有效任务，不包含即席查询 ."""
        return [task for task in itervalues(self.task_dict) if not task.adhoc]

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
        return ", ".join(set([task.owner for task in itervalues(self.task_dict)]))

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
        for task in itervalues(self.task_dict):
            if (isinstance(task, SubDagOperator) or
                    # TODO remove in Airflow 2.0
                    type(task).__name__ == 'SubDagOperator'):
                subdag_lst.append(task.subdag)
                # 递归
                subdag_lst += task.subdag.subdags
        return subdag_lst

    def resolve_template_files(self):
        """删除所有任务的临时文件 ."""
        for task in itervalues(self.task_dict):
            # 从指定的属性中获取设置的模板文件名，渲染模板并将此属性的值设置为渲染后的模板的内容
            task.resolve_template_files()

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
            TI.task_id.in_([task.task_id for task in itervalues(self.task_dict)]),
        )
        if state:
            tis = tis.filter(TI.state == state)
        tis = tis.order_by(TI.execution_date).all()
        return tis

    @property
    def roots(self):
        """获得所有的叶子节点，即没有下游节点 ."""
        return [task for task in itervalues(self.task_dict) if not task.downstream_list]

    def topological_sort(self):
        """拓扑排序
        Sorts tasks in topographical order, such that a task comes after any of its
        upstream dependencies.

        Heavily inspired by:
        http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        :return: list of tasks in topological order
        """
        try
            graph_sorted = graph_topological_sort(self.tasks)
        except XToolException:
            raise AirflowException("A cyclic dependency occurred in dag: {}"
                                    .format(self.dag_id))
        return graph_sorted

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
        from airflow.models.dagbag import DagBag

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
        
        from airflow.models.dagbag import DagBag
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
