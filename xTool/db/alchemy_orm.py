# coding: utf-8

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import atexit
import os
import time
import random

from sqlalchemy import event, exc, select
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

import datetime
from sqlalchemy.types import DateTime, TypeDecorator
import pendulum

from xTool.utils.log.logging_mixin import LoggingMixin


utc = pendulum.timezone('UTC')
log = LoggingMixin().log

engine = None
Session = None


def setup_event_handlers(
        engine,
        reconnect_timeout_seconds,
        initial_backoff_seconds=0.2,
        max_backoff_seconds=120):
    """设置事件处理函数 ."""

    @event.listens_for(engine, "engine_connect")
    def ping_connection(connection, branch):
        """
        Pessimistic SQLAlchemy disconnect handling. Ensures that each
        connection returned from the pool is properly connected to the database.

        http://docs.sqlalchemy.org/en/rel_1_1/core/pooling.html#disconnect-handling-pessimistic

        使用SQLAlchemy时遇到"#2006: MySQL server has gone away"
        原因：
            客户端使用了一个已经在 mysql 中关闭的 session
            mysql 中默认是 8 小时关闭
        解决方案：
            设置SQLAlchemy的连接有效期，在MySQL关闭它之前，我先关闭它。
            在Web框架的层面，每次请求处理完毕时，显式地关闭session。
            在使用session之前，先检查其有效性，无效则创建新的session以供使用。
        """
        if branch:
            # "branch" refers to a sub-connection of a connection,
            # we don't want to bother pinging on these.
            return

        start = time.time()
        backoff = initial_backoff_seconds

        # turn off "close with result".  This flag is only used with
        # "connectionless" execution, otherwise will be False in any case
        save_should_close_with_result = connection.should_close_with_result

        while True:
            connection.should_close_with_result = False

            try:
                connection.scalar(select([1]))
                # If we made it here then the connection appears to be healty
                break
            except exc.DBAPIError as err:
                if time.time() - start >= reconnect_timeout_seconds:
                    log.error(
                        "Failed to re-establish DB connection within %s secs: %s",
                        reconnect_timeout_seconds,
                        err)
                    raise
                if err.connection_invalidated:
                    log.warning("DB connection invalidated. Reconnecting...")

                    # Use a truncated binary exponential backoff. Also includes
                    # a jitter to prevent the thundering herd problem of
                    # simultaneous client reconnects
                    backoff += backoff * random.random()
                    time.sleep(min(backoff, max_backoff_seconds))

                    # run the same SELECT again - the connection will re-validate
                    # itself and establish a new connection.  The disconnect detection
                    # here also causes the whole connection pool to be invalidated
                    # so that all stale connections are discarded.
                    continue
                else:
                    log.error(
                        "Unknown database connection error. Not retrying: %s",
                        err)
                    raise
            finally:
                # restore "close with result"
                connection.should_close_with_result = save_should_close_with_result


    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        """DB连接时获得进程ID ."""
        connection_record.info['pid'] = os.getpid()


    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid {}, "
                "attempting to check out in pid {}".format(connection_record.info['pid'], pid)
            )


def configure_orm(sql_alchemy_conn,
                  pool_enabled=True,
                  pool_size=5,
                  pool_recycle=1800,
                  reconnect_timeout=300,
                  autocommit=False,
                  disable_connection_pool=False,
                  encoding="utf-8",
                  echo=False):
    """配置DB ."""
    log.debug("Setting up DB connection pool (PID %s)" % os.getpid())
    engine_args = {}

    # 不使用DB连接池
    if disable_connection_pool or not pool_enabled:
        engine_args['poolclass'] = NullPool
        log.debug("settings.configure_orm(): Using NullPool")
    elif 'sqlite' not in sql_alchemy_conn:
        # The DB server already has a value for wait_timeout (number of seconds after
        # which an idle sleeping connection should be killed). Since other DBs may
        # co-exist on the same server, SQLAlchemy should set its
        # pool_recycle to an equal or smaller value.
        log.info("setting.configure_orm(): Using pool settings. pool_size={}, "
                 "pool_recycle={}".format(pool_size, pool_recycle))
        engine_args['pool_size'] = pool_size
        engine_args['pool_recycle'] = pool_recycle
        
    # For Python2 we get back a newstr and need a str
    engine_args['encoding'] = encoding.__str__()
    engine_args['echo'] = echo

    # 连接数据库
    engine = create_engine(sql_alchemy_conn, **engine_args)
    # 设置数据库事件处理函数
    setup_event_handlers(engine, reconnect_timeout)

    # 创建全局线程安全session
    Session = scoped_session(
        sessionmaker(autocommit=autocommit,
                     autoflush=False,
                     bind=engine))
    return (engine, Session)


def dispose_orm():
    """ Properly close pooled database connections """
    log.debug("Disposing DB connection pool (PID %s)", os.getpid())
    global engine
    global Session

    if Session:
        Session.remove()
        Session = None
    if engine:
        engine.dispose()
        engine = None


def validate_session(engine, worker_precheck):
    """验证数据库是否可用 ."""
    if not worker_precheck:
        return True
    check_session = sessionmaker(bind=engine)
    session = check_session()
    try:
        session.execute("select 1")
        conn_status = True
    except exc.DBAPIError as err:
        log.error(err)
        conn_status = False
    session.close()
    return conn_status


def configure_adapters():
    from pendulum import Pendulum
    try:
        from sqlite3 import register_adapter
        register_adapter(Pendulum, lambda val: val.isoformat(' '))
    except ImportError:
        pass
    try:
        import MySQLdb.converters
        MySQLdb.converters.conversions[Pendulum] = MySQLdb.converters.DateTime2literal
    except ImportError:
        pass


# configure_adapters()

# Ensure we close DB connections at scheduler and gunicon worker terminations
# atexit.register(dispose_orm)


class UtcDateTime(TypeDecorator):
    """
    Almost equivalent to :class:`~sqlalchemy.types.DateTime` with
    ``timezone=True`` option, but it differs from that by:
    - Never silently take naive :class:`~datetime.datetime`, instead it
      always raise :exc:`ValueError` unless time zone aware value.
    - :class:`~datetime.datetime` value's :attr:`~datetime.datetime.tzinfo`
      is always converted to UTC.
    - Unlike SQLAlchemy's built-in :class:`~sqlalchemy.types.DateTime`,
      it never return naive :class:`~datetime.datetime`, but time zone
      aware value, even with SQLite or MySQL.
    - Always returns DateTime in UTC
    """

    impl = DateTime(timezone=True)

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not isinstance(value, datetime.datetime):
                raise TypeError('expected datetime.datetime, not ' +
                                repr(value))
            elif value.tzinfo is None:
                raise ValueError('naive datetime is disallowed')

            return value.astimezone(utc)

    def process_result_value(self, value, dialect):
        """
        Processes DateTimes from the DB making sure it is always
        returning UTC. Not using timezone.convert_to_utc as that
        converts to configured TIMEZONE while the DB might be
        running with some other setting. We assume UTC datetimes
        in the database.
        """
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=utc)
            else:
                value = value.astimezone(utc)

        return value
