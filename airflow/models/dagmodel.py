# -*- coding: utf-8 -*-

import six
from sqlalchemy import Column, Integer, String, Float, PickleType, Index, Boolean
from sqlalchemy import DateTime
from sqlalchemy import func, or_, and_, true as sqltrue

from airflow.models.base import Base, ID_LEN
from airflow.models.taskinstance import TaskInstance
from airflow import configuration

from xTool.decorators.db import provide_session


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
