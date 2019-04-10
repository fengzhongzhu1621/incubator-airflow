# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import sys
import warnings

import six

from builtins import object
from contextlib import contextmanager
from logging import Handler, StreamHandler


class LoggingMixin(object):
    """用于给派生的类添加日志属性.log
    Convenience super-class to have a logger configured with the class name
    """

    def __init__(self, context=None):
        """给处理器增加上下文 ."""
        self._set_context(context)

    # We want to deprecate the logger property in Airflow 2.0
    # The log property is the de facto standard in most programming languages
    @property
    def logger(self):
        warnings.warn(
            'Initializing logger for {} using logger(), which will '
            'be replaced by .log in future version'.format(
                self.__class__.__module__ + '.' + self.__class__.__name__
            ),
            DeprecationWarning
        )
        return self.log

    @property
    def log(self):
        try:
            return self._log
        except AttributeError:
            self._log = logging.root.getChild(
                self.__class__.__module__ + '.' + self.__class__.__name__
            )
            return self._log

    def _set_context(self, context):
        """给处理器增加上下文 ."""
        if context is not None:
            set_context(self.log, context)


class StreamLogWriter(object):
    """添加带有缓冲的日志流，用于重定向标准输入和输出
    Allows to redirect stdout and stderr to logger
    """
    encoding = False

    def __init__(self, logger, level):
        """
        :param log: The log level method to write to, ie. log.debug, log.warning
        :return:
        """
        self.logger = logger
        self.level = level
        self._buffer = str()

    def write(self, message):
        """
        Do whatever it takes to actually log the specified logging record
        :param message: message to log
        """
        if not message.endswith("\n"):
            self._buffer += message
        else:
            # 只有遇到换行符才真正的输入日志，否则缓存到_buffer
            self._buffer += message
            self.logger.log(self.level, self._buffer.rstrip())
            self._buffer = str()

    def flush(self):
        """
        Ensure all logging output has been flushed
        """
        if self._buffer:
            self.logger.log(self.level, self._buffer)
            self._buffer = str()

    def isatty(self):
        """
        Returns False to indicate the fd is not connected to a tty(-like) device.
        For compatibility reasons.
        """
        return False


class RedirectStdHandler(StreamHandler):
    """
    This class is like a StreamHandler using sys.stderr/stdout, but always uses
    whatever sys.stderr/stderr is currently set to rather than the value of
    sys.stderr/stdout at handler construction time.
    """
    def __init__(self, stream):
        # stream是必填的，且必须是字符串，不能使用文件对象
        if not isinstance(stream, six.string_types):
            raise Exception("Cannot use file like objects. Use 'stdout' or 'stderr'"
                            " as a str and without 'ext://'.")

        self._use_stderr = True
        if 'stdout' in stream:
            self._use_stderr = False

        # StreamHandler tries to set self.stream
        Handler.__init__(self)

    @property
    def stream(self):
        if self._use_stderr:
            return sys.stderr

        return sys.stdout


@contextmanager
def redirect_stdout(logger, level):
    """
    重定向stdout

    .. code:: python

        if args.interactive:
            _run(args, dag, ti)
        else:
            with redirect_stdout(ti.log, logging.INFO),\
                    redirect_stderr(ti.log, logging.WARN):
                _run(args, dag, ti)
            logging.shutdown()
    """
    writer = StreamLogWriter(logger, level)
    try:
        sys.stdout = writer
        yield
    finally:
        sys.stdout = sys.__stdout__


@contextmanager
def redirect_stderr(logger, level):
    """
    重定向stderr

    .. code:: python

        if args.interactive:
            _run(args, dag, ti)
        else:
            with redirect_stdout(ti.log, logging.INFO),\
                    redirect_stderr(ti.log, logging.WARN):
                _run(args, dag, ti)
            logging.shutdown()
    """
    writer = StreamLogWriter(logger, level)
    try:
        sys.stderr = writer
        yield
    finally:
        sys.stderr = sys.__stderr__


def set_context(logger, value):
    """给日志处理器添加上下文
    Walks the tree of loggers and tries to set the context for each handler
    :param logger: logger
    :param value: value to set
    """
    _logger = logger
    while _logger:
        for handler in _logger.handlers:
            try:
                handler.set_context(value)
            except AttributeError:
                # Not all handlers need to have context passed in so we ignore
                # the error when handlers do not have set_context defined.
                pass
        if _logger.propagate is True:
            _logger = _logger.parent
        else:
            _logger = None
