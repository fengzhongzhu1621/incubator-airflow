# -*- coding: utf-8 -*-

"""
LocalExecutor runs tasks by spawning processes in a controlled fashion in different
modes. Given that BaseExecutor has the option to receive a `parallelism` parameter to
limit the number of process spawned, when this parameter is `0` the number of processes
that LocalExecutor can spawn is unlimited.

The following strategies are implemented:
1. Unlimited Parallelism (self.parallelism == 0): In this strategy, LocalExecutor will
spawn a process every time `execute_async` is called, that is, every task submitted to the
LocalExecutor will be executed in its own process. Once the task is executed and the
result stored in the `result_queue`, the process terminates. There is no need for a
`task_queue` in this approach, since as soon as a task is received a new process will be
allocated to the task. Processes used in this strategy are of class LocalWorker.

2. Limited Parallelism (self.parallelism > 0): In this strategy, the LocalExecutor spawns
the number of processes equal to the value of `self.parallelism` at `start` time,
using a `task_queue` to coordinate the ingestion of tasks and the work distribution among
the workers, which will take a task as soon as they are ready. During the lifecycle of
the LocalExecutor, the worker processes are running waiting for tasks, once the
LocalExecutor receives the call to shutdown the executor a poison token is sent to the
workers to terminate them. Processes used in this strategy are of class QueuedLocalWorker.

Arguably, `SequentialExecutor` could be thought as a LocalExecutor with limited
parallelism of just 1 worker, i.e. `self.parallelism = 1`.
This option could lead to the unification of the executor implementations, running
locally, into just one `LocalExecutor` with multiple modes.
"""

import multiprocessing
import subprocess
import time

from builtins import range

from xTool.executors.base_executor import BaseExecutor
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.state import State
from xTool.misc import USE_WINDOWS


class LocalWorker(multiprocessing.Process, LoggingMixin):
    """LocalWorker Process implementation to run airflow commands. Executes the given
    command and puts the result into a result queue when done, terminating execution."""

    def __init__(self, result_queue):
        """
        :param result_queue: the queue to store result states tuples (key, State)
        :type result_queue: multiprocessing.Queue
        """
        super(LocalWorker, self).__init__()
        self.daemon = True
        self.result_queue = result_queue
        self.key = None
        self.command = None

    def execute_work(self, key, command):
        """
        Executes command received and stores result state in queue.
        :param key: the key to identify the TI
        :type key: Tuple(dag_id, task_id, execution_date)
        :param command: the command to execute
        :type command: string
        """
        if key is None:
            return
        self.log.info("%s running %s", self.__class__.__name__, command)
        # shell 的内件命令exec执行命令时，不启用新的shell进程【注： source 和 . 不启用新的shell，在当前shell中执行，设定的局部变量在执行完命令后仍然有效；bash或sh 或shell script执行时，另起一个子shell,其继承父shell的环境变量，其子shelll的变量执行完后不影响父shell，注意三类的区别】exec是用被执行的命令行替换掉当前的shell进程，且exec命令后的其他命令将不再执行。例如在当前shell中执行 exec ls 表示执行ls这条命令来替换当前的shell  即为执行完后会退出当前shell。为了避免这个结果的影响，一般将exec命令放到一个shell脚本中，用主脚本调用这个脚本，调用处可以用bash  xx.sh(xx.sh为存放exec命令的脚本)。这样会为xx.sh建立一个子shell去执行，当执行exec后该子脚本进程就被替换成相应的exec的命令
        # 其中有一个例外：当exec命令对文件描述符操作的时候，就不会替换shell，而是操作完成后还会继续执行后面的命令
        try:
            subprocess.check_call(command, shell=True, close_fds=False if USE_WINDOWS else True)
            state = State.SUCCESS
        except subprocess.CalledProcessError as e:
            state = State.FAILED
            self.log.error("Failed to execute task %s.", str(e))
            # TODO: Why is this commented out?
            # raise e
        self.result_queue.put((key, state))

    def run(self):
        self.execute_work(self.key, self.command)
        time.sleep(1)


class QueuedLocalWorker(LocalWorker):

    """LocalWorker implementation that is waiting for tasks from a queue and will
    continue executing commands as they become available in the queue. It will terminate
    execution once the poison token is found."""

    def __init__(self, task_queue, result_queue):
        super(QueuedLocalWorker, self).__init__(result_queue=result_queue)
        self.task_queue = task_queue

    def run(self):
        while True:
            key, command = self.task_queue.get()
            if key is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break
            self.execute_work(key, command)
            self.task_queue.task_done()
            time.sleep(1)


class LocalExecutor(BaseExecutor):
    """单机并发执行器，采用多进程方式运行job ."""

    class _UnlimitedParallelism(object):
        """Implements LocalExecutor with unlimited parallelism, starting one process
        per each command to execute."""

        def __init__(self, executor):
            """
            :param executor: the executor instance to implement.
            :type executor: LocalExecutor
            """
            self.executor = executor

        def start(self):
            self.executor.workers_used = 0
            self.executor.workers_active = 0

        def execute_async(self, key, command):
            """
            :param key: the key to identify the TI
            :type key: Tuple(dag_id, task_id, execution_date)
            :param command: the command to execute
            :type command: string
            """
            # 每个任务实例，都创建一个子进程
            local_worker = LocalWorker(self.executor.result_queue)
            local_worker.key = key
            local_worker.command = command
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            # 启动子进程执行命令
            local_worker.start()

        def sync(self):
            """每次心跳都需要检查结果队列 . """
            while not self.executor.result_queue.empty():
                # 从队列中获取子进程的执行结果 （key, state）
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)
                self.executor.workers_active -= 1

        def end(self):
            """调度器结束时，需要保证所有的子进程都执行完毕 ."""
            while self.executor.workers_active > 0:
                self.executor.sync()
                time.sleep(0.5)

    class _LimitedParallelism(object):
        """Implements LocalExecutor with limited parallelism using a task queue to
        coordinate work distribution."""

        def __init__(self, executor):
            self.executor = executor

        def start(self):
            # 实现了task_done() 和 join()的队列
            self.executor.queue = multiprocessing.JoinableQueue()
            # 启动指定数量的子进程
            self.executor.workers = [
                QueuedLocalWorker(self.executor.queue, self.executor.result_queue)
                for _ in range(self.executor.parallelism)
            ]
            self.executor.workers_used = len(self.executor.workers)
            for w in self.executor.workers:
                w.start()

        def execute_async(self, key, command):
            """
            :param key: the key to identify the TI
            :type key: Tuple(dag_id, task_id, execution_date)
            :param command: the command to execute
            :type command: string
            """
            self.executor.queue.put((key, command))

        def sync(self):
            while not self.executor.result_queue.empty():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)

        def end(self):
            # Sending poison pill to all worker
            for _ in self.executor.workers:
                self.executor.queue.put((None, None))

            # Wait for commands to finish
            self.executor.queue.join()
            self.executor.sync()

    def start(self):
        # 子进程执行结果队列
        self.result_queue = multiprocessing.Queue()
        self.queue = None
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        # 策略模式
        # parallelism = 0 不限制并发
        # parallelism !=0 同时运行的任务实例的数量
        self.impl = (LocalExecutor._UnlimitedParallelism(self) if self.parallelism == 0
                     else LocalExecutor._LimitedParallelism(self))

        self.impl.start()

    def execute_async(self, key, command, queue=None, executor_config=None):
        """分发任务实例 ."""
        self.impl.execute_async(key=key, command=command)

    def sync(self):
        self.impl.sync()

    def end(self):
        self.impl.end()
