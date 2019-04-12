#coding: utf-8

import atexit
import os
import time
import random

from sqlalchemy import event, exc, select
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from xTool.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log


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
                  disable_connection_pool=False):
    log.debug("Setting up DB connection pool (PID %s)" % os.getpid())
    global engine
    global Session
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

    # 连接数据库
    engine = create_engine(sql_alchemy_conn, **engine_args)
    # 设置数据库事件处理函数
    reconnect_timeout = conf.getint('core', 'SQL_ALCHEMY_RECONNECT_TIMEOUT')
    setup_event_handlers(engine, reconnect_timeout)

    Session = scoped_session(
        sessionmaker(autocommit=autocommit,
                     autoflush=False,
                     bind=engine))


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
