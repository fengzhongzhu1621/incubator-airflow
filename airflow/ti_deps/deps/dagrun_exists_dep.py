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


class DagrunRunningDep(BaseTIDep):
    """验证Dagrun必须是RUNNING的状态 ."""
    NAME = "Dagrun Running"

    # dep_context.ignore_all_deps 参数可以为True
    IGNOREABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        # 获得当前任务实例关联的dag_run，最多有一个
        dag = ti.task.dag
        dagrun = ti.get_dagrun(session)

        if not dagrun:
            # dagrun必须要存在
            yield self._failing_status(
                reason="Task instance's dagrun did not exist")
        else:
            # dagrun的状态必须是RUNNING
            if dagrun.state != State.RUNNING:
                yield self._failing_status(
                    reason="Task instance's dagrun was not in the 'running' state but in "
                           "the state '{}'.".format(dagrun.state))
