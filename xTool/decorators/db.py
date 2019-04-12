# -*- coding: utf-8 -*-

"""
提供DB相关的工具类
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from functools import wraps

import os
import contextlib


from xTool.db import alchemy_orm


@contextlib.contextmanager
def create_session():
    """
    Contextmanager that will create and teardown a session.
    """
    session = alchemy_orm.Session()
    try:
        yield session
        # 清除session实例
        session.expunge_all()
        # 提交事务
        session.commit()
    except:
        # 异常则回滚事务
        session.rollback()
        raise
    finally:
        session.close()


def provide_session(func):
    """如果session变量不再函数参数中，在函数内部也没有声明，则创建一个session变量

    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = 'session'

        # 获得函数中所有的局部变量名
        func_params = func.__code__.co_varnames
        # 判断session变量名是否在函数参数中
        session_in_args = arg_session in func_params and \
            func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            # 如果session变量不再函数参数中，在函数内部也没有声明
            # 则创建一个session变量
            with create_session() as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)

    return wrapper
