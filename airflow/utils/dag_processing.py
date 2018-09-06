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
            if special_args:
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


class AbstractDagFileProcessor(object):
    """
    Processes a DAG file. See SchedulerJob.process_file() for more details.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self):
        """
        Launch the process to process the file
        """
        raise NotImplementedError()

    @abstractmethod
    def terminate(self, sigkill=False):
        """
        Terminate (and then kill) the process launched to process the file
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def pid(self):
        """
        :return: the PID of the process launched to process the given file
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def exit_code(self):
        """
        After the process is finished, this can be called to get the return code
        :return: the exit code of the process
        :rtype: int
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def done(self):
        """
        Check if the process launched to process this file is done.
        :return: whether the process is finished running
        :rtype: bool
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def result(self):
        """
        :return: result of running SchedulerJob.process_file()
        :rtype: list[SimpleDag]
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def start_time(self):
        """
        :return: When this started to process the file
        :rtype: datetime
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def file_path(self):
        """
        :return: the path to the file that this is processing
        :rtype: unicode
        """
        raise NotImplementedError()


class DagFileProcessorManager(LoggingMixin):
    """
    Given a list of DAG definition files, this kicks off several processors
    in parallel to process them. The parallelism is limited and as the
    processors finish, more are launched. The files are processed over and
    over again, but no more often than the specified interval.

    :type _file_path_queue: list[unicode]
    :type _processors: dict[unicode, AbstractDagFileProcessor]
    :type _last_runtime: dict[unicode, float]
    :type _last_finish_time: dict[unicode, datetime]
    """

    def __init__(self,
                 dag_directory,
                 file_paths,
                 parallelism,
                 process_file_interval,
                 max_runs,
                 processor_factory):
        """
        :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
        :type dag_directory: unicode
        :param file_paths: list of file paths that contain DAG definitions
        :type file_paths: list[unicode]
        :param parallelism: maximum number of simultaneous process to run at once
        :type parallelism: int
        :param process_file_interval: process a file at most once every this
        many seconds
        :type process_file_interval: float
        :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
        :type max_runs: int
        :type process_file_interval: float
        :param processor_factory: function that creates processors for DAG
        definition files. Arguments are (dag_definition_path)
        :type processor_factory: (unicode, unicode) -> (AbstractDagFileProcessor)

        """
        # 需要处理的文件，每个文件启动一个文件处理器进程
        # file_paths 是 file_directory 目录下的有效文件路径
        self._file_paths = file_paths
        # DAG文件队列
        self._file_path_queue = []
        # DAG处理器进程的最大数量，即能够同时处理多少个文件
        self._parallelism = parallelism
        # DAG文件的目录，用于搜索DAG
        self._dag_directory = dag_directory
        # job运行的最大次数，默认是-1
        # 如果是>0，则运行指定次数后，调用进程就会停止
        self._max_runs = max_runs
        # 同一个文件在被处理器处理时的间隔
        self._process_file_interval = process_file_interval
        # 文件处理进程工厂函数，接受一个文件路径参数，返回 AbstractDagFileProcessor 对象
        self._processor_factory = processor_factory
        # Map from file path to the processor
        # 记录正在运行的处理器
        self._processors = {}
        # Map from file path to the last runtime
        # 记录文件处理器执行完成后的执行时长
        self._last_runtime = {}
        # Map from file path to the last finish time
        # 记录文件处理器执行完成后的结束时间
        self._last_finish_time = {}
        # Map from file path to the number of runs
        # 处理器运行次数
        self._run_count = defaultdict(int)
        # Scheduler heartbeat key.
        # 记录心跳的次数
        self._heart_beat_key = 'heart-beat'

    @property
    def file_paths(self):
        """返回需要处理的文件列表 ."""
        return self._file_paths

    def get_pid(self, file_path):
        """获得文件所在处理器的进程ID
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the PID of the process processing the given file or None if
        the specified file is not being processed
        :rtype: int
        """
        if file_path in self._processors:
            return self._processors[file_path].pid
        return None

    def get_all_pids(self):
        """获得所有文件处理器的进程ID列表
        :return: a list of the PIDs for the processors that are running
        :rtype: List[int]
        """
        return [x.pid for x in self._processors.values()]

    def get_runtime(self, file_path):
        """获得文件处理器的运行时长，单位是秒
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the current runtime (in seconds) of the process that's
        processing the specified file or None if the file is not currently
        being processed
        """
        if file_path in self._processors:
            return (datetime.now() - self._processors[file_path].start_time)\
                .total_seconds()
        return None

    def get_last_runtime(self, file_path):
        """文件处理器执行完成后，获得执行的时长，单位是秒
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the runtime (in seconds) of the process of the last run, or
        None if the file was never processed.
        :rtype: float
        """
        return self._last_runtime.get(file_path)

    def get_last_finish_time(self, file_path):
        """文件处理器执行完成后，获得的执行完成时间，单位是秒
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the finish time of the process of the last run, or None if the
        file was never processed.
        :rtype: datetime
        """
        return self._last_finish_time.get(file_path)

    def get_start_time(self, file_path):
        """获得文件处理器的开始时间
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the start time of the process that's processing the
        specified file or None if the file is not currently being processed
        :rtype: datetime
        """
        if file_path in self._processors:
            return self._processors[file_path].start_time
        return None

    def set_file_paths(self, new_file_paths):
        """根据文件处理器需要处理的新的文件列表
        Update this with a new set of paths to DAG definition files.

        :param new_file_paths: list of paths to DAG definition files
        :type new_file_paths: list[unicode]
        :return: None
        """
        # 设置DAG目录下最新的DAG文件路径数组
        self._file_paths = new_file_paths
        # 获得 self._file_path_queue 和  new_file_paths的交集
        # 即从文件队列中删除不存在的文件
        self._file_path_queue = [x for x in self._file_path_queue
                                 if x in new_file_paths]
        # Stop processors that are working on deleted files
        # 已删除的文件关联的处理器停止运行
        filtered_processors = {}
        for file_path, processor in self._processors.items():
            if file_path in new_file_paths:
                filtered_processors[file_path] = processor
            else:
                # 如果正在执行的处理器文件不存在，则停止处理器
                self.log.warning("Stopping processor for %s", file_path)
                # 将被删除的文件关联的文件处理器进程，停止执行
                processor.terminate()
        self._processors = filtered_processors

    def processing_count(self):
        """获得文件处理器的数量
        :return: the number of files currently being processed
        :rtype: int
        """
        return len(self._processors)

    def wait_until_finished(self):
        """阻塞等待所有的文件处理器执行完成
        Sleeps until all the processors are done.
        """
        for file_path, processor in self._processors.items():
            while not processor.done:
                time.sleep(0.1)

    def heartbeat(self):
        """心跳
        
        - 处理执行完毕的处理器
        - 将任务加入队列
        - 执行队列中的进程
        This should be periodically called by the scheduler. This method will
        kick off new processes to process DAG definition files and read the
        results from the finished processors.

        :return: a list of SimpleDags that were produced by processors that
        have finished since the last time this was called
        :rtype: list[SimpleDag]
        """
        # 已完成的文件处理器
        finished_processors = {}
        """:type : dict[unicode, AbstractDagFileProcessor]"""
        # 正在运行的文件处理器		
        running_processors = {}
        """:type : dict[unicode, AbstractDagFileProcessor]"""
        # 遍历所有的文件处理器
        for file_path, processor in self._processors.items():
            if processor.done:
                # 文件处理器运行时间
                self.log.info("Processor for %s finished", file_path)
                now = datetime.now()
                # 获得已完成的文件处理器进程
                finished_processors[file_path] = processor
                # 记录文件处理器的的执行时长
                self._last_runtime[file_path] = (now -
                                                 processor.start_time).total_seconds()
                # 记录文件处理器的结束时间
                self._last_finish_time[file_path] = now
                # 记录文件被处理的次数
                self._run_count[file_path] += 1
            else:
                # 记录正在运行的文件处理器进程
                running_processors[file_path] = processor

        # 每一次心跳，剔除已完成的处理器
        self._processors = running_processors

        self.log.debug("%s/%s scheduler processes running",
                       len(self._processors), self._parallelism)

        self.log.debug("%s file paths queued for processing",
                       len(self._file_path_queue))

        # 收集已完成处理器的执行结果
        # Collect all the DAGs that were found in the processed files
        simple_dags = []
        for file_path, processor in finished_processors.items():
            if processor.result is None:
                self.log.warning(
                    "Processor for %s exited with return code %s.",
                    processor.file_path, processor.exit_code
                )
            else:
                for simple_dag in processor.result:
                    simple_dags.append(simple_dag)

        # 如果队列为空，设置需要入队的文件
        # Generate more file paths to process if we processed all the files
        # already.
        if not self._file_path_queue:
            # If the file path is already being processed, or if a file was
            # processed recently, wait until the next batch
            # 可能存在正在运行的文件处理器进程尚未执行完成
            # 在下一次心跳处理
            file_paths_in_progress = self._processors.keys()

            # 记录下尚未到调度时间的文件
            now = datetime.now()
            file_paths_recently_processed = []

            longest_parse_duration = 0
            # 遍历需要处理的文件列表
            for file_path in self._file_paths:
                # 获得文件处理器上一次执行完成的时间
                last_finish_time = self.get_last_finish_time(file_path)
                # 如果文件曾经处理过
                if (last_finish_time is not None and
                    (now - last_finish_time).total_seconds() <
                        self._process_file_interval):
                    file_paths_recently_processed.append(file_path)

            # 获得已经运行的process文件，且已经达到最大运行次数
            #files_paths_at_run_limit = [file_path
            #                            for file_path, num_runs in self._run_count.items()
            #                            if num_runs == self._max_runs]

            # 获得需要入队的文件，新增的文件在此入库
            # 去掉正在运行的文件
            # 去掉最近需要运行的文件
            # 去掉运行次数已经达到阈值的文件
            files_paths_to_queue = list(set(self._file_paths) -
                                        set(file_paths_in_progress) -
                                        set(file_paths_recently_processed))

            # 打印调试信息：遍历正在运行的处理器进程
            for file_path, processor in self._processors.items():
                self.log.debug(
                    "File path %s is still being processed (started: %s)",
                    processor.file_path, processor.start_time.isoformat()
                )
            self.log.debug(
                "Queuing the following files for processing:\n\t%s",
                "\n\t".join(files_paths_to_queue)
            )

            # 将任务加入队列
            self._file_path_queue.extend(files_paths_to_queue)

        # 处理器并发性最大值验证
        # Start more processors if we have enough slots and files to process
        while (self._parallelism - len(self._processors) > 0 and
               self._file_path_queue):
            # 从队列中出队一个文件
            file_path = self._file_path_queue.pop(0)
            # 创建DAG文件处理器子进程
            processor = self._processor_factory(file_path)
            # 启动DAG文件处理器子进程
            # 执行SchedulerJob(dag_ids=dag_id_white_list, log=log).process_file(file_path, pickle_dags)
            processor.start()
            self.log.info(
                "Started a process (PID: %s) to generate tasks for %s",
                processor.pid, file_path
            )
            # 记录文件子进程
            self._processors[file_path] = processor

        # 记录心跳的数量
        # Update scheduler heartbeat count.
        self._run_count[self._heart_beat_key] += 1

        # 返回已完成处理器的执行结果
        return simple_dags

    def max_runs_reached(self):
        """判断文件处理器是否触发最大阈值
        :return: whether all file paths have been processed max_runs times
        """
        if self._max_runs == -1:  # Unlimited runs.
            return False
        # 如果有任意一个文件都没有达到执行次数，也认为没有到达最大阈值
        #for file_path in self._file_paths:
        #    if self._run_count[file_path] != self._max_runs:
        #        return False
        # 心跳总数大于等于最大运行次数时，也会停止调度
        if self._run_count[self._heart_beat_key] < self._max_runs:
            return False
        return True

    def terminate(self):
        """停止所有处理器
        Stops all running processors
        :return: None
        """
        for processor in self._processors.values():
            processor.terminate()
