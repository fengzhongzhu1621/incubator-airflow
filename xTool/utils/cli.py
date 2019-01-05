#coding: utf-8

from __future__ import absolute_import


import logging
import sys
from datetime import datetime
from collections import namedtuple
import argparse
import getpass
import socket
import functools
import json
from argparse import Namespace

from six import iteritems


# 自定义命令行配置参数元组
Arg = namedtuple(
    'Arg', ['flags', 'help', 'action', 'default', 'nargs', 'type', 'choices', 'metavar'])
Arg.__new__.__defaults__ = (None, None, None, None, None, None, None)


class BaseCLIFactory(object):
    """支持子命令的命令行工厂函数

    Examples:
        args = {
            # Shared
            'dag_id': Arg(("dag_id",), "The id of the dag"),
            'task_id': Arg(("task_id",), "The id of the task"),
            'execution_date': Arg(
                ("execution_date",), help="The execution date of the DAG",
                type=parse_execution_date),
            'task_regex': Arg(
                ("-t", "--task_regex"),
                "The regex to filter specific task_ids to backfill (optional)"),
            'subdir': Arg(
                ("-sd", "--subdir"),
                "File location or directory from which to look for the dag. "
                "Defaults to '[AIRFLOW_HOME]/dags' where [AIRFLOW_HOME] is the "
                "value you set for 'AIRFLOW_HOME' config you set in 'airflow.cfg' ",
                default=DAGS_FOLDER),
            'start_date': Arg(
                ("-s", "--start_date"), "Override start_date YYYY-MM-DD",
                type=parse_execution_date),


        subparsers = (
            {
                'func': backfill,
                'help': "Run subsections of a DAG for a specified date range. "
                        "If reset_dag_run option is used,"
                        " backfill will first prompt users whether airflow "
                        "should clear all the previous dag_run and task_instances "
                        "within the backfill date range. "
                        "If rerun_failed_tasks is used, backfill "
                        "will auto re-run the previous failed task instances"
                        " within the backfill date range.",
                'args': (
                    'dag_id', 'task_regex', 'start_date', 'end_date',
                    'mark_success', 'local', 'donot_pickle',
                    'bf_ignore_dependencies', 'bf_ignore_first_depends_on_past',
                    'subdir', 'pool', 'delay_on_limit', 'dry_run', 'verbose', 'conf',
                    'reset_dag_run', 'rerun_failed_tasks',
                )
            })
    """
    # 全局命令行参数配置，包含了Arg命名元组
    args = {}

    # 子查询器配置
    subparsers = tuple()

    # 获得子查询器的函数名
    subparsers_dict = {sp['func'].__name__: sp for sp in subparsers}

    @classmethod
    def get_parser(cls):
        # 创建命令行解析器
        parser = argparse.ArgumentParser()
        # 添加子解析器
        subparsers = parser.add_subparsers(
            help='sub-command help', dest='subcommand')
        subparsers.required = True

        # 遍历子解析器配置
        for subparser_name, subparser_conf in iteritems(cls.subparsers_dict):
            # 根据子命令名创建子解析器
            sp = subparsers.add_parser(subparser_name,
                                       help=subparser_conf['help'])
            # 遍历子命令参数
            for arg in subparser_conf['args']:
                # 根据参数名，从全局参数表中获取参数的详细配置
                arg_namedtuple = cls.args[arg]
                # 将命令元组参数转换为字典，去掉flag参数
                kwargs = {
                    f: getattr(arg_namedtuple, f)
                    for f in arg_namedtuple._fields if f != 'flags' and getattr(arg_namedtuple, f)}
                # 子解析器添加参数
                # flags：参数的短写和长写
                sp.add_argument(*arg_namedtuple.flags, **kwargs)

            # 设置参数的动作
            sp.set_defaults(func=subparser_conf['func'])
        return parser


def get_parser():
    return CLIFactory.get_parser()


def action_logging(f):
    """
    Decorates function to execute function at the same time submitting action_logging
    but in CLI context. It will call action logger callbacks twice,
    one for pre-execution and the other one for post-execution.

    :param f: function instance
    :return: wrapped function
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        """
        An wrapper for cli functions. It assumes to have Namespace instance
        at 1st positional argument
        :param args: Positional argument. It assumes to have Namespace instance
        at 1st positional argument
        :param kwargs: A passthrough keyword argument
        """
        # 第一个参数必须是argparse命名空间实例
        assert args
        assert isinstance(args[0], Namespace), \
            "1st positional argument should be argparse.Namespace instance, " \
            "but {}".format(args[0])
        # 创建命令行参数的上下文
        metrics = _build_metrics(f.__name__, args[0])
        # 执行命令行之前的预处理操作，可以通过register_pre_exec_callback注册
        on_pre_execution(**metrics)
        # 执行命令行
        try:
            return f(*args, **kwargs)
        except Exception as e:
            # 如果命令行抛出了异常，记录异常信息
            metrics['error'] = e
            raise
        finally:
            # 记录命令行结束时间
            metrics['end_datetime'] = datetime.now()
            # 执行命令行之后的操作，可以通过register_post_exec_callback注册
            on_post_execution(**metrics)

    return wrapper


def _build_metrics(func_name, namespace):
    """
    Builds metrics dict from function args

    :param func_name: name of function
    :param namespace: Namespace instance from argparse
    :return: dict with metrics
    """

    metrics = {'sub_command': func_name, 'start_datetime': datetime.now(),
               'full_command': '{}'.format(list(sys.argv)), 'user': getpass.getuser()}

    assert isinstance(namespace, Namespace)
    metrics = vars(namespace)
    metrics['host_name'] = socket.gethostname()

    extra = json.dumps(dict((k, metrics[k]) for k in ('host_name', 'full_command')))
    metrics['extra'] = extra

    return metrics


def register_pre_exec_callback(handler):
    """
    Registers more handler function callback for pre-execution.
    This function callback is expected to be called with keyword args.
    For more about the arguments that is being passed to the callback
    
    :param handler
    :return: None
    """
    logging.debug("Adding {} to pre execution callback".format(handler))
    __pre_exec_callbacks.append(handler)


def register_post_exec_callback(handler):
    """
    Registers more handler function callback for post-execution.
    This function callback is expected to be called with keyword args.
    For more about the arguments that is being passed to the callback
    
    :param handler
    :return: None
    """
    logging.debug("Adding {} to post execution callback".format(handler))
    __post_exec_callbacks.append(handler)


def on_pre_execution(**kwargs):
    """
    Calls callbacks before execution.
    Note that any exception from callback will be logged but won't be propagated.
    :param kwargs:
    :return: None
    """
    logging.debug("Calling callbacks: {}".format(__pre_exec_callbacks))
    for cb in __pre_exec_callbacks:
        try:
            cb(**kwargs)
        except Exception:
            logging.exception('Failed on pre-execution callback using {}'.format(cb))


def on_post_execution(**kwargs):
    """
    Calls callbacks after execution.
    As it's being called after execution, it can capture status of execution,
    duration, etc. Note that any exception from callback will be logged but
    won't be propagated.
    :param kwargs:
    :return: None
    """
    logging.debug("Calling callbacks: {}".format(__post_exec_callbacks))
    for cb in __post_exec_callbacks:
        try:
            cb(**kwargs)
        except Exception:
            logging.exception('Failed on post-execution callback using {}'.format(cb))


__pre_exec_callbacks = []
__post_exec_callbacks = []
