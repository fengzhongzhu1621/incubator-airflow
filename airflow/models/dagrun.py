# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import six
from sqlalchemy import (
    Column, Integer, String, Boolean, PickleType,
    Index, UniqueConstraint)
from sqlalchemy import func, or_, and_
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym
from sqlalchemy import DateTime

from airflow.models.base import Base, ID_LEN, Stats
from airflow import configuration
from airflow import settings
from airflow.ti_deps.dep_context import DepContext
from airflow.exceptions import AirflowException

from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.state import State
from xTool.decorators.db import provide_session
from xTool.exceptions import XToolException


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
            # 正在运行的任务实例的消费者job已经启动，则设置为关闭状态
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
                # 增加任务实例的最大重试次数
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

    # 正在运行的任务实例的消费者job的状态改为SHUTDOWN
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


class DagRun(Base, LoggingMixin):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __tablename__ = "dag_run"

    # DAG实例的唯一标识前缀
    ID_PREFIX = 'scheduled__'
    ID_FORMAT_PREFIX = ID_PREFIX + '{0}'

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    execution_date = Column(DateTime, default=func.now())
    start_date = Column(DateTime, default=func.now())
    end_date = Column(DateTime)
    _state = Column('state', String(50), default=State.RUNNING)
    run_id = Column(String(ID_LEN))
    # 是否是由外部接口触发，而不是调度器触发
    external_trigger = Column(Boolean, default=True)
    conf = Column(PickleType)

    dag = None

    # 唯一索引
    __table_args__ = (
        Index('dag_id_state', dag_id, _state),
        UniqueConstraint('dag_id', 'execution_date'),
        UniqueConstraint('dag_id', 'run_id'),
    )

    def __repr__(self):
        return (
            '<DagRun {dag_id} @ {execution_date}: {run_id}, '
            'externally triggered: {external_trigger}>'
        ).format(
            dag_id=self.dag_id,
            execution_date=self.execution_date,
            run_id=self.run_id,
            external_trigger=self.external_trigger)

    def get_state(self):
        return self._state

    def set_state(self, state):
        """设置dagrun的状态 ."""
        if self._state != state:
            self._state = state
            # 如果dagrun为完成状态，则需要修改结束时间
            self.end_date = datetime.now() if self._state in State.finished() else None

            # 设置dirty标记，需要重新计算dag中每种状态的dagrun数量
            if self.dag_id is not None:
                # FIXME: Due to the scoped_session factor we we don't get a clean
                # session here, so something really weird goes on:
                # if you try to close the session dag runs will end up detached
                session = settings.Session()
                from airflow.models.dagstat import DagStat
                DagStat.set_dirty(self.dag_id, session=session)

    @declared_attr
    def state(self):
        return synonym('_state',
                       descriptor=property(self.get_state, self.set_state))

    @classmethod
    def id_for_date(cls, date, prefix=ID_FORMAT_PREFIX):
        """用时间格式化前缀 ."""
        return prefix.format(date.isoformat()[:19])

    @provide_session
    def refresh_from_db(self, session=None):
        """dagrun：从DB中获取状态更新到ORM模型中
        Reloads the current dagrun from the database
        :param session: database session
        """
        DR = DagRun

        dr = session.query(DR).filter(
            DR.dag_id == self.dag_id,
            DR.run_id == self.run_id
        ).one()

        self.id = dr.id
        self.state = dr.state

    @staticmethod
    @provide_session
    def find(dag_id=None, run_id=None, execution_date=None,
             state=None, external_trigger=None, no_backfills=False,
             session=None):
        """
        Returns a set of dag runs for the given search criteria.

        :param dag_id: the dag_id to find dag runs for
        :type dag_id: integer, list
        :param run_id: defines the the run id for this dag run
        :type run_id: string
        :param execution_date: the execution date
        :type execution_date: datetime
        :param state: the state of the dag run
        :type state: State
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param no_backfills: return no backfills (True), return all (False).
        Defaults to False
        :type no_backfills: bool
        :param session: database session
        :type session: Session
        """
        DR = DagRun

        qry = session.query(DR)
        if dag_id:
            qry = qry.filter(DR.dag_id == dag_id)
        if run_id:
            qry = qry.filter(DR.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                qry = qry.filter(DR.execution_date.in_(execution_date))
            else:
                qry = qry.filter(DR.execution_date == execution_date)
        else:
            begin_time = datetime.now() - timedelta(days=configuration.conf.getint('core', 'sql_query_history_days'))
            qry = qry.filter(DR.execution_date > begin_time)
        if state:
            qry = qry.filter(DR.state == state)
        if external_trigger is not None:
            qry = qry.filter(DR.external_trigger == external_trigger)

        # 是否去掉backfill dag实例
        if no_backfills:
            # in order to prevent a circular dependency
            from airflow.jobs import BackfillJob
            # 过滤到run_id包含 （ID_PREFIX = 'backfill_'）的记录
            # 这些记录为 backfill dag实例
            qry = qry.filter(DR.run_id.notlike(BackfillJob.ID_PREFIX + '%'))

        # 按数据时间排序
        dr = qry.order_by(DR.execution_date).all()
        return dr
        
    @provide_session
    def get_task_instances(self, state=None, session=None):
        """获得当前dagrun的多个任务实例
        Returns the task instances for this dag run
        """
        # 获得当前dag实例的所有任务实例
        from airflow.models.taskinstance import TaskInstance
        TI = TaskInstance
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
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
        if self.dag and self.dag.partial:
            tis = tis.filter(TI.task_id.in_(self.dag.task_ids))

        return tis.all()

    @provide_session
    def get_task_instance(self, task_id, session=None):
        """
        Returns the task instance specified by task_id for this dag run

        :param task_id: the task id
        """
        from airflow.models.taskinstance import TaskInstance
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
            TI.task_id == task_id
        ).first()
        return ti

    def get_dag(self):
        """
        Returns the Dag associated with this DagRun.

        :return: DAG
        """
        if not self.dag:
            raise AirflowException("The DAG (.dag) for {} needs to be set"
                                   .format(self))
        return self.dag

    @provide_session
    def get_previous_dagrun(self, session=None):
        """获得上一个dag实例 The previous DagRun, if there is one"""
        begin_time = datetime.now() - timedelta(days=configuration.conf.getint('core', 'sql_query_history_days'))
        
        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date > begin_time,
            DagRun.execution_date < self.execution_date
        ).order_by(
            DagRun.execution_date.desc()
        ).first()

    @provide_session
    def get_previous_scheduled_dagrun(self, session=None):
        """获得上一个调度的dag实例 The previous, SCHEDULED DagRun, if there is one"""
        dag = self.get_dag()

        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == dag.previous_schedule(self.execution_date)
        ).first()

    @provide_session
    def update_state(self, tis=None, session=None):
        """更新dagrun的状态，由所有的任务实例的状态决定
        修改dagrun的状态，并调用dag的成功或失败回调函数
        Determines the overall state of the DagRun based on the state
        of its TaskInstances.

        :return: State
        """
        # 获得dagrun中dag属性
        dag = self.get_dag()

        # 根据dagid和execution_date获得所有任务实例
        if not tis:
            tis = self.get_task_instances(session=session)
            
        self.log.debug("Updating state for %s considering %s task(s)", self, len(tis))

        # 获得任务实例，去掉已删除的任务实例，获得未执行完毕的任务实例
        unfinished_tasks = []
        for ti in tis:
            # skip in db?
            if ti.state == State.REMOVED:
                tis.remove(ti)
            else:
                ti.task = dag.get_task(ti.task_id)
                # 获得未完成的任务实例
                if ti.state in State.unfinished():
                    unfinished_tasks.append(ti)

        # 获得不依赖上个调度的未执行完毕的任务实例
        start_dttm = datetime.now()
        none_depends_on_past = all(not t.task.depends_on_past for t in unfinished_tasks)

        # 获得没有任务并发限制的任务实例
        none_task_concurrency = all(t.task.task_concurrency is None for t in unfinished_tasks)
        
        # 任务没有上游依赖，且没有阈值限制，则有可优化的空间 small speed up
        if unfinished_tasks and none_depends_on_past and none_task_concurrency:
            # todo: this can actually get pretty slow: one task costs between 0.01-015s
            no_dependencies_met = True
            for ut in unfinished_tasks:
                # We need to flag upstream and check for changes because upstream
                # failures can result in deadlock false positives
                old_state = ut.state
                # 判断任务实例的依赖规则是否满足
                # ignore_in_retry_period 忽略重试时间
                # flag_upstream_failed 根据上游任务失败的情况设置当前任务实例的状态
                # 默认的依赖self.task.deps 为
                #            # 验证重试时间： 任务实例已经标记为重试，但是还没有到下一次重试时间，如果运行就会失败
                #            NotInRetryPeriodDep(),
                #            # 验证任务实例是否依赖上一个周期的任务实例
                #            PrevDagrunDep(),
                #            # 验证上游依赖任务
                #            TriggerRuleDep(),
                deps_met = ut.are_dependencies_met(
                    dep_context=DepContext(
                        # 是否根据上游任务失败的情况设置当前任务实例的状态
                        flag_upstream_failed=True,
                        # 是否忽略重试时间
                        ignore_in_retry_period=True),
                    session=session)
                # 如果依赖规则满足，从DB中获取任务实例的当前状态
                if deps_met or old_state != ut.current_state(session=session):
                    # 依赖满足
                    no_dependencies_met = False
                    break

        duration = (datetime.now() - start_dttm).total_seconds() * 1000
        Stats.timing("dagrun.dependency-check.{}".format(self.dag_id), duration)

        # future: remove the check on adhoc tasks (=active_tasks)
        if len(tis) == len(dag.active_tasks):
            # 获得dag的叶子节点
            root_ids = [t.task_id for t in dag.roots]
            # 获得所有的任务实例，都是叶子节点
            roots = [t for t in tis if t.task_id in root_ids]

            # 任务实例完成且根节点任何一个失败，则dagrun也标记为失败
            # if all roots finished and at least one failed, the run failed
            if (not unfinished_tasks and
                    any(r.state in (State.FAILED, State.UPSTREAM_FAILED) for r in roots)):
                self.log.info('Marking run %s failed', self)
                self.set_state(State.FAILED)
                dag.handle_callback(self, success=False, reason='task_failure',
                                    session=session)

            # 任务实例完成且根节点全部成功，则dagrun也标记为成功
            # if all roots succeeded and no unfinished tasks, the run succeeded
            elif not unfinished_tasks and all(r.state in (State.SUCCESS, State.SKIPPED)
                                              for r in roots):
                self.log.info('Marking run %s successful', self)
                self.set_state(State.SUCCESS)
                dag.handle_callback(self, success=True, reason='success', session=session)

            # 如果任务实例没有完成，且任务实例依赖不满足，则dagrun标记为失败，是一种死锁的情况
            # if *all tasks* are deadlocked, the run failed
            elif (unfinished_tasks and none_depends_on_past and
                  none_task_concurrency and no_dependencies_met):
                self.log.info('Deadlock; marking run %s failed', self)
                self.set_state(State.FAILED)
                dag.handle_callback(self, success=False, reason='all_tasks_deadlocked',
                                    session=session)

            # finally, if the roots aren't done, the dag is still running
            else:
                # 将任务状态改为RUNNING
                self.set_state(State.RUNNING)

        # todo: determine we want to use with_for_update to make sure to lock the run
        # 如果dagrun的状态没有变化，则不更新DB
        session.merge(self)
        session.commit()

        # 返回当前dagrun的状态
        return self.state

    @provide_session
    def verify_integrity(self, session=None):
        """根据dag实例，创建任务实例；验证任务实例，因为任务可能被删除或者新的任务没有被添加到DB中
        Verifies the DagRun by checking for removed tasks or tasks that are not in the
        database yet. It will set state to removed or add the task if required.

        处理3中情况
        1. task在dag中不存在，DB中存在              ：设置任务实例的状态为REMOVE
        2. task在dag中存在，  DB中存在且状态为REMOVE ：设置任务实例的状态为None
        3. task在dag中存在，  DB中不存在             ：在DB中新增加一个任务实例
        """
        # 获得dagmodel对象
        dag = self.get_dag()

        # 根据(dag_id,execution_date)获得当前dagrun关联的所有任务实例
        # 注意： 同一个execution_date可能有多个run_id
        # 一般情况下创建一个新的dag_run，tis为[]
        tis = self.get_task_instances(session=session)

        # check for removed or restored tasks
        # 这种情况发生在 execution_date 相同但是run_id不同的情况下
        # 即系统触发的dag_run和用户外部触发的dag_run调度时间刚好一致，但是run_id不一样
        task_ids = []
        for ti in tis:
            # 获得已经存在的任务ID
            task_ids.append(ti.task_id)
            task = None
            try:
                # 从dag中查询任务
                task = dag.get_task(ti.task_id)
                # 如果任务实例的状态为REMOVE, 则需要恢复这个任务，将其状态设置为None
                if ti.state == State.REMOVED:
                    self.log.info("Restoring task '{}' which was previously "
                                "removed from DAG '{}'".format(ti, dag))
                    Stats.incr("task_restored_to_dag.{}".format(dag.dag_id), 1, 1)
                    ti.state = State.NONE                
            except AirflowException:
                # 将不存在的任务标记为删除
                # dag.task_dict中不存在这个任务
                # 用户修改了dag文件，去掉了部分task声明
                # 此时用户手工触发dag_run，它的调度时间刚好和系统正在调度的时间一致
                if ti.state == State.REMOVED:
                    # 任务不存在且实例已经被标记为删除
                    pass  # ti has already been removed, just ignore it
                elif self.state is not State.RUNNING and not dag.partial:
                    # 任务不存在且实例不是运行态
                    # TODO 不明白为什么要对dag.partial进行判断
                    self.log.warning("Failed to get task '{}' for dag '{}'. "
                                     "Marking it as removed.".format(ti, dag))
                    Stats.incr(
                        "task_removed_from_dag.{}".format(dag.dag_id), 1, 1)
                    # 标记任务实例为删除状态
                    ti.state = State.REMOVED

        # 添加缺失的任务实例： 例如dag变更后新增了任务
        # check for missing tasks
        from airflow.models.taskinstance import TaskInstance
        for task in six.itervalues(dag.task_dict):
            # 如果任务类型是即席查询，则不会创建任务实例
            if task.adhoc:
                continue
            # 未到执行时间且非补录的任务是不会创建任务实例的
            if task.start_date > self.execution_date and not self.is_backfill:
                continue
            # 将不存在的任务实例创建到DB中
            if task.task_id not in task_ids:
                ti = TaskInstance(task, self.execution_date)
                session.add(ti)

        session.commit()

    @staticmethod
    def get_run(session, dag_id, execution_date):
        """根据dag_id和调度时间获得dagrun
        :param dag_id: DAG ID
        :type dag_id: unicode
        :param execution_date: execution date
        :type execution_date: datetime
        :return: DagRun corresponding to the given dag_id and execution date
        if one exists. None otherwise.
        :rtype: DagRun
        """
        qry = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.external_trigger == False, # noqa
            DagRun.execution_date == execution_date,
        )
        return qry.first()

    @property
    def is_backfill(self):
        from airflow.jobs import BackfillJob
        return self.run_id.startswith(BackfillJob.ID_PREFIX)

    @classmethod
    @provide_session
    def get_latest_runs(cls, session):
        """采用内联方式，每一行dagrun都对应一个最新的dagrun

        Returns the latest DagRun for each DAG.
        """
        subquery = (
            session
            .query(
                cls.dag_id,
                func.max(cls.execution_date).label('execution_date'))
            .group_by(cls.dag_id)
            .subquery()
        )
        dagruns = (
            session
            .query(cls)
            .join(subquery,
                  and_(cls.dag_id == subquery.c.dag_id,
                       cls.execution_date == subquery.c.execution_date))
            .all()
        )
        return dagruns
