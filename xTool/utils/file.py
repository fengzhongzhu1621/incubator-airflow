# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import unicode_literals

import errno
import os
import shutil
from tempfile import mkdtemp

from contextlib import contextmanager


import os
import json
import re
import time
import zipfile
from abc import ABCMeta, abstractmethod
from collections import defaultdict

import threading


from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.exceptions import XToolConfigException


### incase people are using threading, we lock file reads
lock = threading.Lock()


@contextmanager
def TemporaryDirectory(suffix='', prefix=None, dir=None):
    """创建临时目录 .
    
    with TemporaryDirectory(prefix='xtool_tmp') as tmp_dir:
        pass
    """
    name = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        yield name
    finally:
        try:
            shutil.rmtree(name)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e


def mkdirs(path, mode):
    """创建目录
    Creates the directory specified by path, creating intermediate directories
    as necessary. If directory already exists, this is a no-op.

    :param path: The directory to create
    :type path: str
    :param mode: The mode to give to the directory e.g. 0o755, ignores umask
    :type mode: int
    """
    try:
        o_umask = os.umask(0)
        os.makedirs(path, mode)
    except OSError:
        if not os.path.isdir(path):
            raise
    finally:
        os.umask(o_umask)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise XToolConfigException(
                'Error creating {}: {}'.format(path, exc.strerror))


def rm_f(path):
    """
    Remove the file at the given path with os.remove(), ignoring errors caused by the file's absence.
    """
    try:
        os.remove( path )
    except OSError as e:
        if e.errno == errno.ENOENT:
            pass
        else:
            raise


def list_py_file_paths(directory, followlinks=True, ignore_filename='.ignore', file_ext='.py', safe_mode=False, safe_filters=(b'xTool', b'XTool')):
    """递归遍历目录，返回匹配规则的文件列表
    Traverse a directory and look for Python files.

    :param directory: the directory to traverse
    :type directory: unicode
    :param safe_mode: whether to use a heuristic to determine whether a file
    contains Airflow DAG definitions
    :return: a list of paths to Python files in the specified directory
    :rtype: list[unicode]
    """
    file_paths = []
    if directory is None:
        return []
    elif os.path.isfile(directory):
        return [directory]
    elif os.path.isdir(directory):
        patterns = []
        # 递归遍历目录，包含链接文件
        for root, dirs, files in os.walk(directory, followlinks=followlinks):
            # 获得需要忽略的文件
            ignore_file = [f for f in files if f == ignore_filename]
            if ignore_file:
                f = open(os.path.join(root, ignore_file[0]), 'r')
                patterns += [p.strip() for p in f.read().split('\n') if p]
                f.close()
            for f in files:
                try:
                    # 获得文件的绝对路径
                    file_path = os.path.join(root, f)
                    if not os.path.isfile(file_path):
                        continue
                    # 验证文件后缀
                    mod_name, file_extension = os.path.splitext(
                        os.path.split(file_path)[-1])
                    if file_extension != file_ext and not zipfile.is_zipfile(file_path):
                        continue
                    # 验证忽略规则
                    if any([re.findall(p, file_path) for p in patterns]):
                        continue

                    # 使用启发式方式猜测是否是一个DAG文件，DAG文件需要包含DAG 或 airflow
                    # Heuristic that guesses whether a Python file contains an
                    # Airflow DAG definition.
                    might_contain_dag = True
                    if safe_mode and not zipfile.is_zipfile(file_path):
                        with open(file_path, 'rb') as f:
                            content = f.read()
                            might_contain_dag = all(
                                [s in content for s in safe_filters])

                    if not might_contain_dag:
                        continue

                    file_paths.append(file_path)
                except Exception:
                    log = LoggingMixin().log
                    log.exception("Error while examining %s", f)
    return file_paths


def ensure_file_exists(filename):
    """创建文件
    Given a valid filename, make sure it exists (will create if DNE)."""
    if not os.path.exists(filename):
        head, tail = os.path.split(filename)
        ensure_dir_exists(head)
        with open(filename, 'w') as f:
            pass  # just create the file


def ensure_dir_exists(directory):
    """创建目录
    Given a valid directory path, make sure it exists."""
    if dir:
        if not os.path.isdir(directory):
            os.makedirs(directory)


def load_json_dict(filename, *args):
    """根据参数列表从配置文件中获取值 ."""
    data = {}
    if os.path.exists(filename):
        lock.acquire()
        with open(filename, "r") as f:
            try:
                data = json.load(f)
                if not isinstance(data, dict):
                    data = {}
            except:
                data = {}  # TODO: issue a warning and bubble it up
        lock.release()
        if args:
            return {key: data[key] for key in args if key in data}
    return data


def save_json_dict(filename, json_dict):
    """将json数据保存到配置文件 ."""
    if isinstance(json_dict, dict):
        # this will raise a TypeError if something goes wrong
        json_string = json.dumps(json_dict, indent=4)
        lock.acquire()
        with open(filename, "w") as f:
            f.write(json_string)
        lock.release()
    else:
        raise TypeError("json_dict was not a dictionary. not saving.")
