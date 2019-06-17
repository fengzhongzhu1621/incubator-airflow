# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String, ForeignKey, Text
from sqlalchemy import DateTime
from sqlalchemy.orm import relationship

from airflow.models.base import Base


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
