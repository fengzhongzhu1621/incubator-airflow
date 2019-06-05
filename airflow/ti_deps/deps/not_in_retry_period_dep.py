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
from datetime import datetime

from xTool.ti_deps.deps.base_ti_dep import BaseTIDep
from xTool.decorators.db import provide_session
from xTool.utils.state import State


class NotInRetryPeriodDep(BaseTIDep):
    NAME = "Not In Retry Period"

    # dep_context.ignore_all_deps 参数可以为True
    IGNOREABLE = True
    # dep_context.ignore_task_deps 参数可以为True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        # 验证是否忽略重试时间，默认为False
        if dep_context.ignore_in_retry_period:
            yield self._passing_status(
                reason="The context specified that being in a retry period was "
                       "permitted.")
            return

        # 如果不忽略重试时间
        # 验证任务实例是否标记为重试，如果状态不为重试，则通过
        if ti.state != State.UP_FOR_RETRY:
            yield self._passing_status(
                reason="The task instance was not marked for retrying.")
            return

        # 判断任务实例为重试状态，且任务重试时间还没有到达
        # 只有当前时间大于等于下一次的重试时间，才会执行任务实例
        # --------------------------------------------------------
        #        ^               ^                         ^
        #        |     {delay}   |                         |
        #     ti.end_date     ti.next_retry_datetime()   datetime.now()
        if ti.is_premature:
            cur_date = datetime.now()
            next_task_retry_date = ti.next_retry_datetime()
            yield self._failing_status(
                reason="Task is not ready for retry yet but will be retried "
                       "automatically. Current date is {0} and task will be retried "
                       "at {1}.".format(cur_date.isoformat(),
                                        next_task_retry_date.isoformat()))
