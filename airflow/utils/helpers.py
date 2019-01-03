# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import psutil

from builtins import input
from past.builtins import basestring
from datetime import datetime
from functools import reduce
import imp
import os
import re
import signal
import subprocess
import sys
import warnings

from jinja2 import Template

from airflow import configuration
from airflow.exceptions import AirflowException

# When killing processes, time to wait after issuing a SIGTERM before issuing a
# SIGKILL.
DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM = configuration.conf.getint(
    'core', 'KILLED_TASK_CLEANUP_TIME'
)


def validate_key(k, max_length=250):
    """验证key的格式 ."""
    if not isinstance(k, basestring):
        raise TypeError("The key has to be a string")
    elif len(k) > max_length:
        raise AirflowException(
            "The key has to be less than {0} characters".format(max_length))
    elif not re.match(r'^[A-Za-z0-9_\-\.]+$', k):
        raise AirflowException(
            "The key ({k}) has to be made of alphanumeric characters, dashes, "
            "dots and underscores exclusively".format(**locals()))
    else:
        return True


def alchemy_to_dict(obj):
    """ 将SQLAlchemy model instance转化为字典，
    并将时间格式化为iso格式的字符串
    Transforms a SQLAlchemy model instance into a dictionary
    """
    if not obj:
        return None
    d = {}
    for c in obj.__table__.columns:
        value = getattr(obj, c.name)
        # 格式化时间
        if type(value) == datetime:
            value = value.isoformat()
        d[c.name] = value
    return d


def ask_yesno(question):
    yes = set(['yes', 'y'])
    no = set(['no', 'n'])

    done = False
    print(question)
    while not done:
        choice = input().lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            print("Please respond by yes or no.")


def is_in(obj, l):
    """判断obj对象是否在l中，判断相等的逻辑是is，而不是=
    Checks whether an object is one of the item in the list.
    This is different from ``in`` because ``in`` uses __cmp__ when
    present. Here we change based on the object itself
    """
    for item in l:
        if item is obj:
            return True
    return False


def is_container(obj):
    """判断对象是否可以遍历，但不是字符串
    Test if an object is a container (iterable) but not a string
    """
    return hasattr(obj, '__iter__') and not isinstance(obj, basestring)


def as_tuple(obj):
    """将可遍历对象转换为元组，将无法遍历的元素转换为只有一个的元组
    If obj is a container, returns obj as a tuple.
    Otherwise, returns a tuple containing obj.
    """
    if is_container(obj):
        return tuple(obj)
    else:
        return tuple([obj])


def chunks(items, chunk_size):
    """将数组按指定大小分割
    Yield successive chunks of a given size from a list of items
    """
    if (chunk_size <= 0):
        raise ValueError('Chunk size must be a positive integer')
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def reduce_in_chunks(fn, iterable, initializer, chunk_size=0):
    """将数组分组后，并对分片进行fn计算
    Reduce the given list of items by splitting it into chunks
    of the given size and passing each chunk through the reducer
    """
    if len(iterable) == 0:
        return initializer
    if chunk_size == 0:
        chunk_size = len(iterable)
    return reduce(fn, chunks(iterable, chunk_size), initializer)


def as_flattened_list(iterable):
    """ 将二维数组展开为一维数组
    Return an iterable with one level flattened

    >>> as_flattened_list((('blue', 'red'), ('green', 'yellow', 'pink')))
    ['blue', 'red', 'green', 'yellow', 'pink']

    等价于
    list(itertools.chain.from_iterable((('blue', 'red'), ('green', 'yellow', 'pink'))))
    """
    return [e for i in iterable for e in i]


def chain(*tasks):
    """将任务连成线
    Given a number of tasks, builds a dependency chain.

    chain(task_1, task_2, task_3, task_4)

    is equivalent to

    task_1.set_downstream(task_2)
    task_2.set_downstream(task_3)
    task_3.set_downstream(task_4)
    """
    for up_task, down_task in zip(tasks[:-1], tasks[1:]):
        up_task.set_downstream(down_task)


def pprinttable(rows):
    """打印一个漂亮的ascii表格
	Returns a pretty ascii table from tuples

    If namedtuple are used, the table will have headers
    """
    if not rows:
        return

    # 获得表格的头部
    if hasattr(rows[0], '_fields'):  # if namedtuple
        headers = rows[0]._fields
    else:
        headers = ["col{}".format(i) for i in range(len(rows[0]))]

    # 获得表头每一列的长度
    lens = [len(s) for s in headers]
    # 计算所有行中每一列的最大长度
    for row in rows:
        for i in range(len(rows[0])):
            slenght = len("{}".format(row[i]))
            if slenght > lens[i]:
                lens[i] = slenght

    # 获得行列格式
    formats = []
    hformats = []
    for i in range(len(rows[0])):
        if isinstance(rows[0][i], int):
            formats.append("%%%dd" % lens[i])
        else:
            formats.append("%%-%ds" % lens[i])
        hformats.append("%%-%ds" % lens[i])
    pattern = " | ".join(formats)
    hpattern = " | ".join(hformats)
    # 获得每一行的分隔符
    separator = "-+-".join(['-' * n for n in lens])
    s = ""
    s += separator + '\n'
    s += (hpattern % tuple(headers)) + '\n'
    s += separator + '\n'

    def f(t):
        return "{}".format(t) if isinstance(t, basestring) else t

    for line in rows:
        s += pattern % tuple(f(t) for t in line) + '\n'
    s += separator + '\n'
    return s


def reap_process_group(pid, log, sig=signal.SIGTERM,
                       timeout=DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM):
    """
    Tries really hard to terminate all children (including grandchildren). Will send
    sig (SIGTERM) to the process group of pid. If any process is alive after timeout
    a SIGKILL will be send.

    :param log: log handler
    :param pid: pid to kill
    :param sig: signal type 软件终止信号
    :param timeout: how much time a process has to terminate 杀死进程后的等待超时时间
    """

    def on_terminate(p):
        """进程被关闭时的回调，打印进程ID和返回码 ."""
        log.info("Process %s (%s) terminated with exit code %s", p, p.pid, p.returncode)

    # 不允许杀死自己
    if pid == os.getpid():
        raise RuntimeError("I refuse to kill myself")

    # 创建进程对象
    parent = psutil.Process(pid)

    # 根据进程ID，递归获得所有子进程和当前进程
    children = parent.children(recursive=True)
    children.append(parent)

    # 杀掉进程组
    # 程序结束(terminate)信号, 与SIGKILL不同的是该信号可以被阻塞和处理。
    # 通常用来要求程序自己正常退出，shell命令kill缺省产生这个信号。
    # 如果进程终止不了，我们才会尝试SIGKILL。
    log.info("Sending %s to GPID %s", sig, os.getpgid(pid))
    # getpgid 返回pid进程的group id.
    # 如果pid为0,返回当前进程的group id。在unix中有效
    os.killpg(os.getpgid(pid), sig)

    # 等待正在被杀死的进程结束
    gone, alive = psutil.wait_procs(children, timeout=timeout, callback=on_terminate)

    # 如果仍然有进程存活
    if alive:
        # 打印存活的进程
        for p in alive:
            log.warn("process %s (%s) did not respond to SIGTERM. Trying SIGKILL", p, pid)

        # 如果子进程仍然存活，则调用SIGKILL信号重新杀死
        os.killpg(os.getpgid(pid), signal.SIGKILL)

        # 等待子进程结束
        gone, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)

        # 如果子进程仍然存活，则记录错误日志
        if alive:
            for p in alive:
                log.error("Process %s (%s) could not be killed. Giving up.", p, p.pid)


def parse_template_string(template_string):
    if "{{" in template_string:  # jinja mode
        return None, Template(template_string)
    else:
        return template_string, None


class AirflowImporter(object):
    """
    Importer that dynamically loads a class and module from its parent. This
    allows Airflow to support ``from airflow.operators import BashOperator``
    even though BashOperator is actually in
    ``airflow.operators.bash_operator``.

    The importer also takes over for the parent_module by wrapping it. This is
    required to support attribute-based usage:

    .. code:: python

        from airflow import operators
        operators.BashOperator(...)
        _operators = {
            'bash_operator': ['BashOperator'],
        }
        importer = AirflowImporter(sys.modules[__name__], _operators)
    """

    def __init__(self, parent_module, module_attributes):
        """
        :param parent_module: The string package name of the parent module. For
            example, 'airflow.operators'
        :type parent_module: string
        :param module_attributes: The file to class mappings for all importable
            classes.
        :type module_attributes: string
        """
        self._parent_module = parent_module
        self._attribute_modules = self._build_attribute_modules(module_attributes)
        self._loaded_modules = {}

        # Wrap the module so we can take over __getattr__.
        sys.modules[parent_module.__name__] = self

    @staticmethod
    def _build_attribute_modules(module_attributes):
        """遍历模块的属性，返回属性和模块名称的映射关系
        Flips and flattens the module_attributes dictionary from:

            module => [Attribute, ...]

        To:

            Attribute => module

        This is useful so that we can find the module to use, given an
        attribute.
        """
        attribute_modules = {}

        for module, attributes in list(module_attributes.items()):
            for attribute in attributes:
                attribute_modules[attribute] = module

        return attribute_modules

    def _load_attribute(self, attribute):
        """
        Load the class attribute if it hasn't been loaded yet, and return it.
        """
        # 根据属性获得模块名称
        module = self._attribute_modules.get(attribute, False)

        # 如果模块不存在，则抛出导入错误
        if not module:
            # This shouldn't happen. The check happens in find_modules, too.
            raise ImportError(attribute)
        elif module not in self._loaded_modules:
            # 保证模块只加载一次
            # Note that it's very important to only load a given modules once.
            # If they are loaded more than once, the memory reference to the
            # class objects changes, and Python thinks that an object of type
            # Foo that was declared before Foo's module was reloaded is no
            # longer the same type as Foo after it's reloaded.
            # 获得父模块所在的目录
            path = os.path.realpath(self._parent_module.__file__)
            folder = os.path.dirname(path)
            # 在父模块的目录下所有的文件中查找指定的module
            f, filename, description = imp.find_module(module, [folder])
            self._loaded_modules[module] = imp.load_module(module, f, filename, description)

            # This functionality is deprecated, and AirflowImporter should be
            # removed in 2.0.
            #warnings.warn(
            #    "Importing '{i}' directly from '{m}' has been "
            #    "deprecated. Please import from "
            #    "'{m}.[operator_module]' instead. Support for direct "
            #    "imports will be dropped entirely in Airflow 2.0.".format(
            #        i=attribute, m=self._parent_module.__name__),
            #    DeprecationWarning)

        loaded_module = self._loaded_modules[module]
        
        # 从动态加载后的模块中获取属性
        return getattr(loaded_module, attribute)

    def __getattr__(self, attribute):
        """
        Get an attribute from the wrapped module. If the attribute doesn't
        exist, try and import it as a class from a submodule.

        This is a Python trick that allows the class to pretend it's a module,
        so that attribute-based usage works:

            from airflow import operators
            operators.BashOperator(...)

        It also allows normal from imports to work:

            from airflow.operators.bash_operator import BashOperator
        """
        # 如果模块包含指定的属性
        if hasattr(self._parent_module, attribute):
            # Always default to the parent module if the attribute exists.
            return getattr(self._parent_module, attribute)
        # 如果模块不包含指定的属性
        elif attribute in self._attribute_modules:
            # 动态加载模块，并获取属性
            # Try and import the attribute if it's got a module defined.
            loaded_attribute = self._load_attribute(attribute)
            setattr(self, attribute, loaded_attribute)
            return loaded_attribute

        raise AttributeError
