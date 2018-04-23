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

import signal

from airflow.exceptions import AirflowTaskTimeout
from airflow.utils.log.logging_mixin import LoggingMixin


class timeout(LoggingMixin):
    """超时上下文管理器: 给耗时操作增加统一的TimeOut超时处理机制（仅用于单进程）
    To be used in a ``with`` block and timeout its content.

    e.g. 
        try:
            with timeout(int(
                    task_copy.execution_timeout.total_seconds())):
                result = task_copy.execute(context=context)
        except AirflowTaskTimeout:
            task_copy.on_kill()
            raise
    """

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        self.log.error("Process timed out")
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        try:
            # 注册SIGLARM信号，延迟几秒后向自身进程发送信号
            # 进程收到信号后，抛出 AirflowTaskTimeout 异常
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)
        except ValueError as e:
            self.log.warning("timeout can't be used in the current context")
            self.log.exception(e)

    def __exit__(self, type, value, traceback):
        try:
            # 关闭信号
            signal.alarm(0)
        except ValueError as e:
            self.log.warning("timeout can't be used in the current context")
            self.log.exception(e)
