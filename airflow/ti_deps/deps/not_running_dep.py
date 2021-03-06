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
from xTool.utils.state import State


class NotRunningDep(BaseTIDep):
    """验证任务实例是否已经运行了  ."""
    NAME = "Task Instance Not Already Running"

    # Task instances must not already be running, as running two copies of the same
    # task instance at the same time (AKA double-trigger) should be avoided at all
    # costs, even if the context specifies that all dependencies should be ignored.

    # dep_context.ignore_all_deps 参数不生效，即此依赖不能被忽略
    IGNOREABLE = False

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        # 正在运行的任务实例不能重复执行
        if ti.state == State.RUNNING:
            yield self._failing_status(
                reason="Task is already running, it started on {0}.".format(
                    ti.start_date))
