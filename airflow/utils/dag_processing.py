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

import os
import re
import time
import zipfile
from abc import ABCMeta, abstractmethod
from collections import defaultdict

from six import itervalues, iteritems

from airflow.dag.base_dag import BaseDag, BaseDagBag
from airflow.exceptions import AirflowException
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin


class SimpleDag(BaseDag):
    """
    A simplified representation of a DAG that contains all attributes
    required for instantiating and scheduling its associated tasks.
    """

    def __init__(self, dag, pickle_id=None):
        """
        :param dag: the DAG
        :type dag: DAG
        :param pickle_id: ID associated with the pickled version of this DAG.
        :type pickle_id: unicode
        """
        self._dag_id = dag.dag_id
        self._task_ids = [task.task_id for task in dag.tasks]
        self._full_filepath = dag.full_filepath
        self._is_paused = dag.is_paused
        # 每个dag能并发执行的最大任务数
        self._concurrency = dag.concurrency
        self._pickle_id = pickle_id
        # 获得任务ID和任务特殊参数的映射
        self._task_special_args = {}
        for task in dag.tasks:
            special_args = {}
            if task.task_concurrency is not None:
                special_args['task_concurrency'] = task.task_concurrency
            if len(special_args) > 0:
                self._task_special_args[task.task_id] = special_args

    @property
    def dag_id(self):
        """
        :return: the DAG ID
        :rtype: unicode
        """
        return self._dag_id

    @property
    def task_ids(self):
        """
        :return: A list of task IDs that are in this DAG
        :rtype: list[unicode]
        """
        return self._task_ids

    @property
    def full_filepath(self):
        """
        :return: The absolute path to the file that contains this DAG's definition
        :rtype: unicode
        """
        return self._full_filepath

    @property
    def concurrency(self):
        """
        :return: maximum number of tasks that can run simultaneously from this DAG
        :rtype: int
        """
        return self._concurrency

    @property
    def is_paused(self):
        """
        :return: whether this DAG is paused or not
        :rtype: bool
        """
        return self._is_paused

    @property
    def pickle_id(self):
        """
        :return: The pickle ID for this DAG, if it has one. Otherwise None.
        :rtype: unicode
        """
        return self._pickle_id

    @property
    def task_special_args(self):
        """获得一个dag所有任务的特殊参数 ."""
        return self._task_special_args

    def get_task_special_arg(self, task_id, special_arg_name):
        """根据任务Id获得这个任务的特殊参数 ."""
        if task_id in self._task_special_args and special_arg_name in self._task_special_args[task_id]:
            return self._task_special_args[task_id][special_arg_name]
        else:
            return None


class SimpleDagBag(BaseDagBag):
    """dag容器
    A collection of SimpleDag objects with some convenience methods.
    """

    def __init__(self, simple_dags):
        """
        Constructor.

        :param simple_dags: SimpleDag objects that should be in this
        :type: list(SimpleDag)
        """
        self.simple_dags = simple_dags
        self.dag_id_to_simple_dag = {}

        for simple_dag in simple_dags:
            self.dag_id_to_simple_dag[simple_dag.dag_id] = simple_dag

    @property
    def dag_ids(self):
        """
        :return: IDs of all the DAGs in this
        :rtype: list[unicode]
        """
        return self.dag_id_to_simple_dag.keys()

    def get_dag(self, dag_id):
        """
        :param dag_id: DAG ID
        :type dag_id: unicode
        :return: if the given DAG ID exists in the bag, return the BaseDag
        corresponding to that ID. Otherwise, throw an Exception
        :rtype: SimpleDag
        """
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id]


def list_py_file_paths(directory, safe_mode=True):
    """
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
        patterns_by_dir = {}
        # 递归遍历目录，包含链接文件
        for root, dirs, files in os.walk(directory, followlinks=True):
            patterns = patterns_by_dir.get(root, [])
            # 获得需要忽略的文件
            ignore_file = os.path.join(root, '.airflowignore')
            if os.path.isfile(ignore_file):
                with open(ignore_file, 'r') as f:
                    # If we have new patterns create a copy so we don't change
                    # the previous list (which would affect other subdirs)
                    patterns = patterns + [p for p in f.read().split('\n') if p]

            # If we can ignore any subdirs entirely we should - fewer paths
            # to walk is better. We have to modify the ``dirs`` array in
            # place for this to affect os.walk
            dirs[:] = [
                d
                for d in dirs
                if not any(re.search(p, os.path.join(root, d)) for p in patterns)
            ]

            # We want patterns defined in a parent folder's .airflowignore to
            # apply to subdirs too
            for d in dirs:
                patterns_by_dir[os.path.join(root, d)] = patterns

            for f in files:
                try:
                    # 获得文件的绝对路径
                    file_path = os.path.join(root, f)
                    if not os.path.isfile(file_path):
                        continue
                    # 验证文件后缀
                    mod_name, file_ext = os.path.splitext(
                        os.path.split(file_path)[-1])
                    if file_ext != '.py' and not zipfile.is_zipfile(file_path):
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
                                [s in content for s in (b'DAG', b'airflow')])

                    if not might_contain_dag:
                        continue

                    file_paths.append(file_path)
                except Exception:
                    log = LoggingMixin().log
                    log.exception("Error while examining %s", f)
    return file_paths
