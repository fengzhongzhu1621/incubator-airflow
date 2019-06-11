# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String, Float, PickleType, Index
from sqlalchemy import DateTime

from airflow.models.base import Base, ID_LEN


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
