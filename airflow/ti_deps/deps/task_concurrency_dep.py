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

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from xTool.decorators.db import provide_session


class TaskConcurrencyDep(BaseTIDep):
    """每个任务的任务实例有最大限制
    This restricts the number of running task instances for a particular task.
    """
    NAME = "Task Concurrency"

    # dep_context.ignore_all_deps 参数可以为True
    IGNOREABLE = True
    # dep_context.ignore_task_deps 参数可以为True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        # 任务并发数限制，State.RUNNING 状态的任务实例的数量不能超过此阈值，默认为None
        if ti.task.task_concurrency is None:
            yield self._passing_status(reason="Task concurrency is not set.")
            return

        # 获得正在运行的任务实例的数量
        if ti.get_num_running_task_instances(session) >= ti.task.task_concurrency:
            yield self._failing_status(reason="The max task concurrency "
                                              "has been reached.")
            return
        else:
            yield self._passing_status(reason="The max task concurrency "
                                              "has not been reached.")
            return
