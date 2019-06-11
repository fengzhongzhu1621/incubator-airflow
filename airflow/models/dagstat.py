# -*- coding: utf-8 -*-

import itertools
from datetime import datetime, timedelta

from sqlalchemy import Column, Integer, String, Float, PickleType, Index, Boolean
from sqlalchemy import DateTime

from airflow.models.base import Base, ID_LEN, Stats
from airflow.models.dagrun import DagRun

from xTool.decorators.db import provide_session
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.state import State


class DagStat(Base):
    __tablename__ = "dag_stats"

    dag_id = Column(String(ID_LEN), primary_key=True)
    state = Column(String(50), primary_key=True)
    count = Column(Integer, default=0, nullable=False)
    dirty = Column(Boolean, default=False, nullable=False)

    def __init__(self, dag_id, state, count=0, dirty=False):
        self.dag_id = dag_id
        self.state = state
        self.count = count
        self.dirty = dirty

    @staticmethod
    @provide_session
    def set_dirty(dag_id, session=None):
        """
        :param dag_id: the dag_id to mark dirty
        :param session: database session
        :return:
        """
        # 给dag的每一种状态都创建一条记录
        # 将统计表中不存在的状态插入到db中
        DagStat.create(dag_id=dag_id, session=session)

        try:
            # 给指定的dag所有的状态行加行锁
            stats = session.query(DagStat).filter(
                DagStat.dag_id == dag_id
            ).with_for_update().all()

            # 修改设置dirty标记
            for stat in stats:
                stat.dirty = True
            session.commit()
        except Exception as e:
            session.rollback()
            log = LoggingMixin().log
            log.warning("Could not update dag stats for %s", dag_id)
            log.exception(e)

    @staticmethod
    @provide_session
    def update(dag_ids=None, dirty_only=True, session=None):
        """更新dag每个状态的dag_run的数量，并设置dirty为False
        Updates the stats for dirty/out-of-sync dags

        :param dag_ids: dag_ids to be updated
        :type dag_ids: list
        :param dirty_only: only updated for marked dirty, defaults to True
        :type dirty_only: bool
        :param session: db session to use
        :type session: Session
        """
        try:
            qry = session.query(DagStat)
            if dag_ids:
                qry = qry.filter(DagStat.dag_id.in_(set(dag_ids)))
            # 仅仅获得脏数据
            if dirty_only:
                qry = qry.filter(DagStat.dirty == True) # noqa

            # 添加行级锁
            qry = qry.with_for_update().all()

            # 获得所有的dagId列表
            dag_ids = set([dag_stat.dag_id for dag_stat in qry])

            # avoid querying with an empty IN clause
            if not dag_ids:
                session.commit()
                return

            # 获得dag每个dagrun状态的记录数量
            begin_time = datetime.now() - timedelta(days=configuration.conf.getint('core', 'sql_query_history_days'))
            dagstat_states = set(itertools.product(dag_ids, State.dag_states))
            qry = (
                session.query(DagRun.dag_id, DagRun.state, func.count('*'))
                .filter(DagRun.dag_id.in_(dag_ids))
                .filter(DagRun.execution_date > begin_time)
                .group_by(DagRun.dag_id, DagRun.state)
            )
            counts = {(dag_id, state): count for dag_id, state, count in qry}

            # 计算笛卡尔积
            for dag_id, state in dagstat_states:
                count = 0
                if (dag_id, state) in counts:
                    count = counts[(dag_id, state)]

                session.merge(
                    DagStat(dag_id=dag_id, state=state, count=count, dirty=False)
                )

            session.commit()
        except Exception as e:
            session.rollback()
            log = LoggingMixin().log
            log.warning("Could not update dag stat table")
            log.exception(e)

    @staticmethod
    @provide_session
    def create(dag_id, session=None):
        """将统计表中不存在的状态插入到db中

        Creates the missing states the stats table for the dag specified

        :param dag_id: dag id of the dag to create stats for
        :param session: database session
        :return:
        """
        # unfortunately sqlalchemy does not know upsert
        qry = session.query(DagStat).filter(DagStat.dag_id == dag_id).all()
        states = {dag_stat.state for dag_stat in qry}
        for state in State.dag_states:
            if state not in states:
                try:
                    session.merge(DagStat(dag_id=dag_id, state=state))
                    session.commit()
                except Exception as e:
                    session.rollback()
                    log = LoggingMixin().log
                    log.warning("Could not create stat record")
                    log.exception(e)
