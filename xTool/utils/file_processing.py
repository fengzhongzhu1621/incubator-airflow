# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import re
import sys
import time
import zipfile
from abc import ABCMeta, abstractmethod
from collections import defaultdict
import multiprocessing

from six import itervalues, iteritems
from datetime import datetime

from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.log.logging_mixin import set_context
from xTool.utils.log.logging_mixin import StreamLogWriter
from xTool.exceptions import XToolException


class AbstractFileProcessor(object):
    """文件处理器抽象类 ."""
    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self):
        """启动处理器 ."""
        raise NotImplementedError()

    @abstractmethod
    def terminate(self, sigkill=False):
        """终止处理器 ."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def pid(self):
        """获得处理器的进程ID ."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def exit_code(self):
        """获得处理器进程退出码 ."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def done(self):
        """判断处理器进程是否执行完成 ."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def result(self):
        """获得处理器执行结果 ."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def start_time(self):
        """获得处理器进程的开始时间 ."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def file_path(self):
        """获得处理器处理的文件路径 ."""
        raise NotImplementedError()


class BaseMultiprocessFileProcessor(AbstractFileProcessor, LoggingMixin):
    """文件处理器基类 ."""

    # 记录这个类实例的创建数量
    class_creation_counter = 0

    def __init__(self, file_path, *args, **kwargs):
        # 文件的路径
        self._file_path = file_path
        # 创建一个多进程队列
        self._result_queue = multiprocessing.Queue()
        # 文件处理进程对象
        self._process = None
        # 文件处理进程返回的结果
        self._result = None
        # 文件处理进程是否完成的标识
        self._done = False
        # 文件处理进程的开始时间
        self._start_time = None
        # 使用实例的次数作为实例ID
        self._instance_id = BaseMultiprocessFileProcessor.class_creation_counter
        # 实例创建次数自增
        BaseMultiprocessFileProcessor.class_creation_counter += 1
        self.args = args
        self.kwargs = kwargs

    @property
    def file_path(self):
        """返回文件的路径 ."""
        return self._file_path
    
    @staticmethod
    def process_file(self, file_path):
        """子进程文件处理函数 .
        
        Returns:
            返回结果数组
        """
        raise NotImplementedError()

    def _handler(result_queue, file_path, thread_name, args, kwargs):
        log = logging.getLogger("xTool.processor")
        # 设置日志处理器上下文，例如日志handler初始化时创建日志目录
        set_context(log, file_path)

        try:
            # 设置进程名
            start_time = time.time()

            log.info("Started process (PID=%s) to work on %s", os.getpid(), file_path)
            # 执行文件处理
            result = BaseMultiprocessFileProcessor.process_file(file_path)
            # 将执行结果保存到结果队列
            result_queue.put(result)
            end_time = time.time()
            log.info(
                "Processing %s took %.3f seconds", file_path, end_time - start_time
            )
        except:
            log.exception("Got an exception! Propagating...")
            raise

    def start(self):
        """创建一个文件处理子进程，
        并将结果保存到self._result_queue结果队列
        """
        thread_name = "{}_{}".format(self.__class__.__name__, self._instance_id)
        self._process = multiprocessing.Process(
            target=self._handler,
            args=(self._result_queue,
                  self.file_path,
                  thread_name,
                  self.args,
                  self.kwargs),
            name="{}-Process".format(thread_name))
        self._process.start()
        # 记录子进程启动时间
        self._start_time = datetime.now()
        return self._process

    def terminate(self, sigkill=False):
        """终止文件处理子进程 ."""
        if self._process is None:
            raise XToolException("Tried to call stop before starting!")
        # 删除结果队列
        self._result_queue = None
        # 终止进程
        self._process.terminate()
        # 等待进程被杀死
        self._process.join(5)
        # 是否需要强制再次杀死存活的文件处理进程
        if sigkill and self._process.is_alive():
            # 如果进程被终止后依然存活，发送SIGKILL信号杀死进程
            self.log.warning("Killing PID %s", self._process.pid)
            os.kill(self._process.pid, signal.SIGKILL)

    @property
    def pid(self):
        """获得文件处理子进程的PID ."""
        if self._process is None:
            raise XToolException("Tried to get PID before starting!")
        return self._process.pid

    @property
    def exit_code(self):
        """获得文件处理子进程的错误码 ."""
        if not self._done:
            raise XToolException("Tried to call retcode before process was finished!")
        return self._process.exitcode

    @property
    def done(self):
        """判断文件处理子进程是否已经执行完成 ."""
        if self._process is None:
            raise XToolException("Tried to see if it's done before starting!")

        if self._done:
            return True

        # 如果子进程有结果返回
        if not self._result_queue.empty():
            # 获得执行结果
            self._result = self._result_queue.get_nowait()
            self._done = True
            self.log.debug("Waiting for %s", self._process)
            # 等待子进程释放资源并结束
            self._process.join()
            return True

        # 如果子进程已经执行完成
        if not self._process.is_alive():
            # 设置完成标记
            self._done = True
            # 获得子进程执行结果
            if not self._result_queue.empty():
                self._result = self._result_queue.get_nowait()
            # 等待子进程资源释放
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            return True

        return False

    @property
    def result(self):
        """获得文件处理子进程的执行结果 ."""
        if not self.done:
            raise XToolException("Tried to get the result before it's done!")
        return self._result

    @property
    def start_time(self):
        """获得文件处理子进程的启动时间 ."""
        if self._start_time is None:
            raise XToolException("Tried to get start time before it started!")
        return self._start_time


class FileProcessorManager(LoggingMixin):
    """文件处理器进程管理类

    与multiprocess.Pool相比的特点如下
    - 基于文件的并发调度
    - 支持并发控制
    - 支持同一个文件在多次调度之间的间隔设置
    - 支持对进程池输入参数的动态改变
    - 支持以心跳的形式返还进程的部分处理结果

    :type _file_path_queue: list[unicode]
    :type _processors: dict[unicode, AbstractFileProcessor]
    :type _last_runtime: dict[unicode, float]
    :type _last_finish_time: dict[unicode, datetime]
    """
    def __init__(self,
                 file_directory,
                 file_paths,
                 parallelism,
                 process_file_interval,
                 max_runs,
                 processor_factory):
        """
        :param file_directory: All
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
        # 文件的目录
        self._file_directory = file_directory
        # 需要处理的文件，每个文件启动一个文件处理器进程
        # file_paths 是 file_directory 目录下的有效文件路径
        self._file_paths = file_paths
        # 文件队列
        self._file_path_queue = []
        # 文件处理器进程的最大数量，即能够同时处理多少个文件
        self._parallelism = parallelism
        # job运行的最大次数，默认是-1
        # 如果是>0，则运行指定次数后，调用进程就会停止
        self._max_runs = max_runs
        # 同一个文件在被处理器处理时的间隔
        self._process_file_interval = process_file_interval
        # 文件处理进程工厂函数，接受一个文件路径参数，返回 AbstractFileProcessor 对象
        self._processor_factory = processor_factory
        # 记录正在运行的处理器
        self._processors = {}
        # 记录文件处理器执行完成后的执行时长
        self._last_runtime = {}
        # 记录文件处理器执行完成后的结束时间
        self._last_finish_time = {}
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
        """获得文件所在处理器的进程ID ."""
        if file_path in self._processors:
            return self._processors[file_path].pid
        return None

    def get_all_pids(self):
        """获得所有文件处理器的进程ID列表 ."""
        return [x.pid for x in self._processors.values()]

    def get_runtime(self, file_path):
        """获得文件处理器的运行时长，单位是秒 ."""
        if file_path in self._processors:
            return (datetime.now() - self._processors[file_path].start_time)\
                .total_seconds()
        return None

    def get_last_runtime(self, file_path):
        """文件处理器执行完成后，获得执行的时长，单位是秒 ."""
        return self._last_runtime.get(file_path)

    def get_last_finish_time(self, file_path):
        """文件处理器执行完成后，获得的执行完成时间，单位是秒 ."""
        return self._last_finish_time.get(file_path)

    def get_start_time(self, file_path):
        """获得文件处理器的开始时间 ."""
        if file_path in self._processors:
            return self._processors[file_path].start_time
        return None

    def set_file_paths(self, new_file_paths):
        """根据文件处理器需要处理的新的文件列表 ."""
        # 设置目录下最新的文件路径数组
        self._file_paths = list(new_file_paths)
        # 获得 self._file_path_queue 和  new_file_paths的交集
        # 即从文件队列中删除不存在的文件
        self._file_path_queue = [x for x in self._file_path_queue
                                 if x in self._file_paths]
        self.log.info("There are %s files in file_path_queue", len(self._file_path_queue))
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
        self.log.info("There are %s files in processors", len(self._processors))

    def processing_count(self):
        """获得文件处理器的数量 ."""
        return len(self._processors)

    def wait_until_finished(self):
        """阻塞等待所有的文件处理器执行完成 ."""
        for file_path, processor in iteritems(self._processors):
            while not processor.done:
                time.sleep(0.1)

    def heartbeat(self):
        """心跳
        
        - 处理执行完毕的处理器
        - 将任务加入队列
        - 执行队列中的进程
        """
        # 正在运行的文件处理器
        # :type : dict[unicode, AbstractFileProcessor]
        running_processors = {}

        # 遍历所有的文件处理器
        result = []
        for file_path, processor in self._processors.items():
            if processor.done:
                self.log.info("Processor for %s finished", file_path)
                # 文件处理器运行时间
                now = datetime.now()
                # 记录文件处理器的的执行时长
                self._last_runtime[file_path] = (now -
                                                 processor.start_time).total_seconds()
                # 记录文件处理器的结束时间
                self._last_finish_time[file_path] = now
                # 记录文件被处理的次数
                self._run_count[file_path] += 1
                # 收集已完成处理器的执行结果
                if processor.result is None:
                    self.log.warning(
                        "Processor for %s exited with return code %s.",
                        processor.file_path, processor.exit_code
                    )
                else:
                    for value in processor.result:
                        result.append(value)
            else:
                # 记录正在运行的文件处理器进程
                running_processors[file_path] = processor

        # 每一次心跳，剔除已完成的处理器
        self._processors = running_processors

        self.log.debug("%s/%s scheduler processes running",
                       len(self._processors), self._parallelism)

        self.log.debug("%s file paths queued for processing",
                       len(self._file_path_queue))

        # 如果队列为空，设置需要入队的文件
        if not self._file_path_queue:
            # 可能存在正在运行的文件处理器进程尚未执行完成
            # 在下一次心跳处理
            file_paths_in_progress = self._processors.keys()

            # 记录下尚未到调度时间的文件
            # 如果self_process_file_interval为0，则file_paths_recently_processed永远为[]
            now = datetime.now()
            file_paths_recently_processed = []

            # 遍历需要处理的文件列表
            for file_path in self._file_paths:
                # 获得文件处理器上一次执行完成的时间
                last_finish_time = self.get_last_finish_time(file_path)
                # 如果文件曾经处理过
                if last_finish_time is not None:
                    duration = now - last_finish_time
                    # 如果文件尚未到调度时间，则记录在指定数组中
                    if duration.total_seconds() < self._process_file_interval:
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

            file_paths_len = len(self._file_paths)
            file_paths_in_progress_len = len(file_paths_in_progress)
            file_paths_recently_processed_len = len(file_paths_recently_processed)
            files_paths_to_queue_len = len(files_paths_to_queue)
            self.log.info("Queuing %s files for processing: %s = %s - %s - %s",
                          files_paths_to_queue_len,
                          files_paths_to_queue_len,          # 队列的长度
                          file_paths_len,                    # 需要处理的文件的数量
                          file_paths_in_progress_len,        # 正在运行的文件子进程的数量
                          file_paths_recently_processed_len)

        # 处理器并发性最大值验证
        # Start more processors if we have enough slots and files to process
        while (self._parallelism - len(self._processors) > 0 and
               self._file_path_queue):
            # 从队列中出队一个文件
            file_path = self._file_path_queue.pop(0)
            # 创建文件处理器子进程
            processor = self._processor_factory(file_path)
            # 启动文件处理器子进程
            processor.start()
            self.log.info(
                "Started a process (PID: %s) to generate tasks for %s",
                processor.pid, file_path
            )
            # 记录文件子进程
            self._processors[file_path] = processor

        # 记录心跳的次数
        self._run_count[self._heart_beat_key] += 1

        # 返回已完成处理器的执行结果
        return result

    def get_heart_beat_count(self):
        return self._run_count.get(self._heart_beat_key, 0)

    def max_runs_reached(self):
        """判断文件处理器是否触发最大阈值 ."""
        if self._max_runs == -1:  # Unlimited runs.
            return False
        # 如果有任意一个文件都没有达到执行次数，也认为没有到达最大阈值
        #for file_path in self._file_paths:
        #    if self._run_count[file_path] < self._max_runs:
        #        return False
        # 心跳总数大于等于最大运行次数时，也会停止调度
        if self._run_count[self._heart_beat_key] < self._max_runs:
            return False
        return True

    def terminate(self):
        """停止所有处理器 ."""
        for processor in itervalues(self._processors):
            processor.terminate()
