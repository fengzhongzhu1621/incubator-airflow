# -*- coding: utf-8 -*-

import os
from builtins import str
import subprocess

from xTool.executors.base_executor import BaseExecutor
from xTool.utils.state import State
from xTool.misc import USE_WINDOWS


class SequentialExecutor(BaseExecutor):
    """顺序调度器是单进程的，一次只能运行一个任务实例 ."""

    def __init__(self, parallelism):
        super(SequentialExecutor, self).__init__(parallelism)
        self.commands_to_run = []

    def execute_async(self, key, command, queue=None, executor_config=None):
        """分发任务实例 ."""
        self.commands_to_run.append((key, command,))

    def sync(self):
        """顺序执行所有的任务实例 ."""
        for key, command in self.commands_to_run:
            self.log.info("Executing command: %s", command)

            try:
                # 执行命令，等待子进程结束
                subprocess.check_call(command, shell=True, close_fds=False if USE_WINDOWS else True, env=os.environ.copy())
                self.change_state(key, State.SUCCESS)
            except subprocess.CalledProcessError as e:
                self.change_state(key, State.FAILED)
                self.log.error("Failed to execute task %s.", str(e))

        self.commands_to_run = []

    def end(self):
        self.heartbeat()

    def terminate(self):
        pass
