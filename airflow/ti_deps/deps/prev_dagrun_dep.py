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
from xTool.utils.state import State


class PrevDagrunDep(BaseTIDep):
    """验证任务实例是否依赖上一个周期的任务实例
    Is the past dagrun in a state that allows this task instance to run, e.g. did this
    task instance's task in the previous dagrun complete if we are depending on past.
    """
    NAME = "Previous Dagrun State"
    IGNOREABLE = True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        if dep_context.ignore_depends_on_past:
            yield self._passing_status(
                reason="The context specified that the state of past DAGs could be "
                       "ignored.")
            return

        # 如果没有配置和上个调度的任务依赖，则直接通过
        # 即 depends_on_past = False，直接通过
        if not ti.task.depends_on_past:
            yield self._passing_status(
                reason="The task did not have depends_on_past set.")
            return

        # 如果任务设置了 depends_on_past = True
        # 如果任务实例需要依赖上一个周期的任务实例
        # Don't depend on the previous task instance if we are the first task
        dag = ti.task.dag
        if dag.catchup:
            # 如果是单次调度，则直接通过，不受depends_on_past配置的影响
            last_execution_date = dag.previous_schedule(ti.execution_date)
            if last_execution_date is None:
                yield self._passing_status(
                    reason="This task does not have a schedule or is @once"
                )
                return
            # 如果上一次调度早于任务的开始时间，则直接通过
            if dag.previous_schedule(ti.execution_date) < ti.task.start_date:
                yield self._passing_status(
                    reason="This task instance was the first task instance for its task.")
                return
        else:
            # 获得任务实例关联的dagrun
            dr = ti.get_dagrun()
            # 获得上一个dagrun，包含内外部调用及补录的记录
            last_dagrun = dr.get_previous_dagrun() if dr else None
            # 如果不存在上个调度周期，即第一次调度，则直接通过
            if not last_dagrun:
                yield self._passing_status(
                    reason="This task instance was the first task instance for its task.")
                return

        # 根据当前任务实例获得上一个调度的同一个任务的任务实例
        previous_ti = ti.previous_ti
        # 注意
        # 1. 如果 depends_on_past = True ， @once，catchup=True; 最好禁止用户这样设置
        # 2. depends_on_past = True，@once, catchup=False, 可以保证once调度的上下依赖
        if not previous_ti:
            yield self._failing_status(
                reason="depends_on_past is true for this task's DAG, but the previous "
                       "task instance has not run yet.")
            return

        # 上一次调度的任务实例的状态必须被标记为跳过或成功
        if previous_ti.state not in {State.SKIPPED, State.SUCCESS}:
            yield self._failing_status(
                reason="depends_on_past is true for this task, but the previous task "
                       "instance {0} is in the state '{1}' which is not a successful "
                       "state.".format(previous_ti, previous_ti.state))

        # 如果任务被标记为wait_for_downstream，
        # 则需要判断上一次任务实例的下游节点是否已经完成
        previous_ti.task = ti.task
        # 如果设置了wait_for_downstream
        # 验证上一个调度的任务实例的所有下游任务实例是否都执行完成
        if (ti.task.wait_for_downstream and
                not previous_ti.are_dependents_done(session=session)):
            yield self._failing_status(
                reason="The tasks downstream of the previous task instance {0} haven't "
                       "completed.".format(previous_ti))
