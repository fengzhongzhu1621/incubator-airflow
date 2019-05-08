# coding: utf-8

from airflow.configuration import conf
from xTool.stats import DummyStatsLogger


def create_statsclient():
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