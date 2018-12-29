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

import psutil

from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.helpers import reap_process_group


class BashTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task by invoking through the Bash shell.
    """
    def __init__(self, local_task_job):
        super(BashTaskRunner, self).__init__(local_task_job)

    def start(self):
        """执行bash命令 .

        如果用-c 那么bash 会从第一个非选项参数后面的字符串中读取命令，如果字符串有多个空格，第一个空格前面的字符串是要执行的命令，也就是$0, 后面的是参数，即$1, $2....

        bash -c "cmd string"
        bash -c "./atest hello world"
        """
        self.process = self.run_command(['bash', '-c'], join_args=True)

    def return_code(self):
        return self.process.poll()

    def terminate(self):
        """终止bash进程 ."""
        if self.process and psutil.pid_exists(self.process.pid):
            reap_process_group(self.process.pid, self.log)

    def on_finish(self):
        super(BashTaskRunner, self).on_finish()
