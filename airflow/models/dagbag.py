# -*- coding: utf-8 -*-

import hashlib
import sys
import os
import zipfile
import imp
from datetime import datetime, timedelta
from collections import namedtuple, defaultdict

import six
from croniter import (
    croniter, CroniterBadCronError, CroniterBadDateError, CroniterNotAlphaError
)

from sqlalchemy import Column, Integer, String, Float, PickleType, Index
from sqlalchemy import DateTime

from airflow.models.base import Base, ID_LEN, Stats
from airflow.models.dag import DAG
from airflow.dag.base_dag import BaseDag, BaseDagBag
from airflow import configuration
from airflow.executors import GetDefaultExecutor, LocalExecutor
from airflow.exceptions import (
    AirflowDagCycleException, AirflowException, AirflowSkipException
)
from airflow import settings, utils

from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.file import list_py_file_paths
from xTool.utils.timeout import timeout
from xTool.decorators.db import provide_session



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
