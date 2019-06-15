# -*- coding: utf-8 -*-
import json

from sqlalchemy import Column, Integer, String, Boolean, Text
from sqlalchemy.orm import synonym
from sqlalchemy.ext.declarative import declared_attr

from airflow.models.base import Base, ID_LEN
from airflow import configuration

from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.decorators.db import provide_session
from xTool.crypto.fernet import get_fernet, InvalidFernetToken


class Variable(Base, LoggingMixin):
    """系统常量 ."""
    __tablename__ = "variable"

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    _val = Column('val', Text)
    is_encrypted = Column(Boolean, unique=False, default=False)

    def __repr__(self):
        # Hiding the value
        return '{} : {}'.format(self.key, self._val)

    def get_val(self):
        """获得常量的值 ."""
        log = LoggingMixin().log
        if self._val and self.is_encrypted:
            try:
                # 解密常量值
                fernet_key = configuration.conf.get('core', 'FERNET_KEY')
                fernet = get_fernet(fernet_key)
                return fernet.decrypt(bytes(self._val, 'utf-8')).decode()
            except InvalidFernetToken:
                # 解密失败返回None
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
        """设置常量的值 ."""
        if value:
            # 加密常量值
            fernet_key = configuration.conf.get('core', 'FERNET_KEY')
            fernet = get_fernet(fernet_key)
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
                # 注意：需要commit后才能生效
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
        # 写数据到数据库，但是不commit，其它的事务无法看到这个更新后的结果
        # 作用是防止其它事务获取到正在修改的值
        session.flush()
