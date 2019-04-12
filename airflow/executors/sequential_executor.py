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

import os
from builtins import str
import subprocess

from airflow.executors.base_executor import BaseExecutor
from xTool.utils.state import State
from xTool.misc import USE_WINDOWS


class SequentialExecutor(BaseExecutor):
    """顺序调度器是单进程的，一次只能运行一个任务实例
    This executor will only run one task instance at a time, can be used
    for debugging. It is also the only executor that can be used with sqlite
    since sqlite doesn't support multiple connections.

    Since we want airflow to work out of the box, it defaults to this
    SequentialExecutor alongside sqlite as you first install it.
    """

    def __init__(self):
        super(SequentialExecutor, self).__init__()
        self.commands_to_run = []

    def execute_async(self, key, command, queue=None, executor_config=None):
        self.commands_to_run.append((key, command,))

    def sync(self):
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
