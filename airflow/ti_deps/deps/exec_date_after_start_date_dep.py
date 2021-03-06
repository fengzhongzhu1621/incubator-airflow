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

from xTool.ti_deps.deps.base_ti_dep import BaseTIDep
from xTool.decorators.db import provide_session


class ExecDateAfterStartDateDep(BaseTIDep):
    """任务的执行时间必须大于等于任务的开始时间 ，大于等于dag的开始时间."""
    NAME = "Execution Date"

    # dep_context.ignore_all_deps 参数可以为True
    IGNOREABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        # 任务实例的调度时间必须大于等于任务的开始时间
        #-------------------------------------------------------------
        #         ^                     ^                      ^
        #         |                     |                      |
        #-------------------------------------------------------------
        #  last_execution_date     ti.execution_date
        #                      ^                                   ^
        #                      |                                   |
        #                ti.task.start_date                       now
        if ti.task.start_date and ti.execution_date < ti.task.start_date:
            yield self._failing_status(
                reason="The execution date is {0} but this is before the task's start "
                "date {1}.".format(
                    ti.execution_date.isoformat(),
                    ti.task.start_date.isoformat()))

        # 任务实例的调度时间必须大于等于dag的开始时间
        #-------------------------------------------------------------
        #         ^                     ^                      ^
        #         |                     |                      |
        #-------------------------------------------------------------
        #  last_execution_date     ti.execution_date
        #                      ^                                   ^
        #                      |                                   |
        #                ti.task.dag.start_date                   now
        if (ti.task.dag and ti.task.dag.start_date and
                ti.execution_date < ti.task.dag.start_date):
            yield self._failing_status(
                reason="The execution date is {0} but this is before the task's "
                "DAG's start date {1}.".format(
                    ti.execution_date.isoformat(),
                    ti.task.dag.start_date.isoformat()))
