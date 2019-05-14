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

import time
from celery import states as celery_states
from xTool.executors.base_executor import BaseExecutor


class CeleryExecutor(BaseExecutor):
    """
    CeleryExecutor is recommended for production use of Airflow. It allows
    distributing the execution of task instances to multiple worker nodes.

    Celery is a simple, flexible and reliable distributed system to process
    vast amounts of messages, while providing operations with the tools
    required to maintain such a system.
    """
    def __init__(self, parallelism, execute_command):
        super(CeleryExecutor, self).__init__(parallelism)
        self.execute_command = execute_command

    def start(self):
        self.tasks = {}
        self.last_state = {}

    def execute_async(self, key, command,
                      queue,
                      executor_config=None):
        self.log.info("[celery] queuing {key} through celery, "
                      "queue={queue}".format(**locals()))
        # 向celery发送任务
        self.tasks[key] = self.execute_command.apply_async(
            args=[command], queue=queue)
        # 记录当前任务的状态
        self.last_state[key] = celery_states.PENDING

    def sync(self):
        """非阻塞操作，获取任务执行结果 ."""
        self.log.debug("Inquiring about %s celery task(s)", len(self.tasks))
        for key, task in list(self.tasks.items()):
            try:
                # 获得异步任务的执行状态
                state = task.state
                if self.last_state[key] != state:
                    if state == celery_states.SUCCESS:
                        self.success(key)
                        del self.tasks[key]
                        del self.last_state[key]
                    elif state == celery_states.FAILURE:
                        self.fail(key)
                        del self.tasks[key]
                        del self.last_state[key]
                    elif state == celery_states.REVOKED:
                        self.fail(key)
                        del self.tasks[key]
                        del self.last_state[key]
                    else:
                        self.log.info("Unexpected state: %s", state)
                        self.last_state[key] = state
            except Exception as e:
                self.log.error("Error syncing the celery executor, ignoring it:")
                self.log.exception(e)

    def end(self, synchronous=False):
        if synchronous:
            # 等待所有的任务完成
            while any([
                    task.state not in celery_states.READY_STATES
                    for task in self.tasks.values()]):
                time.sleep(5)
        self.sync()

    def terminate(self):
        pass
