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

import json

from airflow.exceptions import AirflowException
from airflow.models import DagRun, DagBag
from airflow.utils import timezone
from airflow.utils.state import State


def trigger_dag(dag_id, run_id=None, conf=None, execution_date=None):
    dagbag = DagBag()

    if dag_id not in dagbag.dags:
        raise AirflowException("Dag id {} not found".format(dag_id))

    # 根据dag_id获得的dag对象
    dag = dagbag.get_dag(dag_id)

    # 获得调度时间
    if not execution_date:
        execution_date = timezone.utcnow()

    # 验证调度时间必须存在时区信息
    assert timezone.is_localized(execution_date)
    # 去掉微妙
    execution_date = execution_date.replace(microsecond=0)

    # 获得dag实例运行ID
    if not run_id:
        run_id = "manual__{0}".format(execution_date.isoformat())

    # 判断dag实例是否存在，(dag_id, run_id)可以确认唯一性
    dr = DagRun.find(dag_id=dag_id, run_id=run_id)
    if dr:
        raise AirflowException("Run id {} already exists for dag id {}".format(
            run_id,
            dag_id
        ))

    # 获得dag实例参数配置
    run_conf = None
    if conf:
        run_conf = json.loads(conf)

    # 创建dag_run
    trigger = dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.RUNNING,
        conf=run_conf,
        external_trigger=True
    )

    return trigger
