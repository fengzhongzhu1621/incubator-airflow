# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

from sqlalchemy import Column, Integer, String, Text

from airflow.models.base import Base
from airflow.models.taskinstance import TaskInstance
from airflow import configuration

from xTool.decorators.db import provide_session
from xTool.utils.state import State


class Pool(Base):
    """任务实例的插槽池管理 ."""
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True)
    pool = Column(String(50), unique=True)  # 插槽的名称
    slots = Column(Integer, default=0)      # 插槽的数量
    description = Column(Text)

    def __repr__(self):
        return self.pool

    def to_json(self):
        return {
            'id': self.id,
            'pool': self.pool,
            'slots': self.slots,
            'description': self.description,
        }

    @provide_session
    def used_slots(self, session):
        """
        Returns the number of slots used at the moment
        """
        begin_time = datetime.now() - timedelta(days=configuration.getint('core', 'sql_query_history_days'))
        running = (
            session
            .query(TaskInstance)
            .filter(TaskInstance.execution_date > begin_time)
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.RUNNING)
            .count()
        )
        return running

    @provide_session
    def queued_slots(self, session):
        """
        Returns the number of slots used at the moment
        """
        begin_time = datetime.now() - timedelta(days=configuration.getint('core', 'sql_query_history_days'))
        return (
            session
            .query(TaskInstance)
            .filter(TaskInstance.execution_date > begin_time)
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )

    @provide_session
    def open_slots(self, session):
        """
        Returns the number of slots open at the moment
        """
        used_slots = self.used_slots(session=session)
        queued_slots = self.queued_slots(session=session)
        return self.slots - used_slots - queued_slots
