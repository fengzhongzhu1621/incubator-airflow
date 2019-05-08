# coding: utf-8

import logging

import pendulum
from airflow.configuration import conf
from xTool.stats import DummyStatsLogger
from xTool.utils.timezone import set_timezone_var


log = logging.getLogger(__name__)


def create_statsclient():
    """创建数据采集器客户端 ."""
    # 默认的日志收集器
    Stats = DummyStatsLogger

    # 采集到的数据会走 UDP 协议发给 StatsD，由 StatsD 解析、提取、计算处理后，周期性地发送给 Graphite。
    if conf.getboolean('scheduler', 'statsd_on'):
        from statsd import StatsClient

        statsd = StatsClient(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.getint('scheduler', 'statsd_port'),
            prefix=conf.get('scheduler', 'statsd_prefix'))
        Stats = statsd
    else:
        Stats = DummyStatsLogger
    return Stats


def set_default_timezone():
    """设置默认市区 ."""
    timezone = pendulum.timezone('UTC')
    try:
        tz = conf.get("core", "default_timezone")
        if tz == "system":
            timezone = pendulum.local_timezone()
        else:
            timezone = pendulum.timezone(tz)
    except Exception:
        pass
    log.info("Configured default timezone %s" % timezone)
    set_timezone_var(timezone)
    return timezone
