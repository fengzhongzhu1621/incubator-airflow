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


class NotSkippedDep(BaseTIDep):
    """验证任务实例的状态是否被标记为跳过 ."""
    NAME = "Task Instance Not Skipped"

    # dep_context.ignore_all_deps 参数可以为True
    IGNOREABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        # 已经被跳过的任务不能执行
        if ti.state == State.SKIPPED:
            yield self._failing_status(reason="The task instance has been skipped.")
