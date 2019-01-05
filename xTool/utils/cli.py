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


Arg = namedtuple(
    'Arg', ['flags', 'help', 'action', 'default', 'nargs', 'type', 'choices', 'metavar'])
Arg.__new__.__defaults__ = (None, None, None, None, None, None, None)


class BaseCLIFactory(object):
    args = {}
    subparsers = tuple()

    subparsers_dict = {sp['func'].__name__: sp for sp in subparsers}

    @classmethod
    def get_parser(cls, dag_parser=False):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(
            help='sub-command help', dest='subcommand')
        subparsers.required = True

        subparser_list = cls.dag_subparsers if dag_parser else cls.subparsers_dict.keys()
        for sub in subparser_list:
            sub = cls.subparsers_dict[sub]
            sp = subparsers.add_parser(sub['func'].__name__, help=sub['help'])
            for arg in sub['args']:
                if 'dag_id' in arg and dag_parser:
                    continue
                arg = cls.args[arg]
                kwargs = {
                    f: getattr(arg, f)
                    for f in arg._fields if f != 'flags' and getattr(arg, f)}
                sp.add_argument(*arg.flags, **kwargs)
            sp.set_defaults(func=sub['func'])
        return parser


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
