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

import datetime
import json

from airflow.exceptions import DagRunAlreadyExists, DagNotFound
from airflow.models import DagRun, DagBag
from airflow.utils.state import State


def _trigger_dag(
        dag_id,
        dag_bag,
        dag_run,
        run_id,
        conf,
        execution_date,
        replace_microseconds,
):
    if dag_id not in dag_bag.dags:
        raise DagNotFound("Dag id {} not found".format(dag_id))

    # 根据dag_id获得的dag对象
    dag = dag_bag.get_dag(dag_id)

    # 获得调度时间
    if not execution_date:
        execution_date = datetime.datetime.now()

    # 验证调度时间必须存在时区信息
    assert isinstance(execution_date, datetime.datetime)

    if replace_microseconds:
        execution_date = execution_date.replace(microsecond=0)

    # 获得dag实例运行ID，默认调度时间与run_id关联
    # 还有一种情况是，同一个调度时间有多个run_id
    if not run_id:
        run_id = "manual__{0}".format(execution_date.isoformat())

    # 判断dag实例是否存在，(dag_id, run_id)可以确认唯一性
    dr = dag_run.find(dag_id=dag_id, run_id=run_id)
    if dr:
        raise DagRunAlreadyExists("Run id {} already exists for dag id {}".format(
            run_id,
            dag_id
        ))

    # 获得dag实例参数配置
    run_conf = None
    if conf:
        run_conf = json.loads(conf)

    triggers = list()
    dags_to_trigger = list()
    dags_to_trigger.append(dag)
    while dags_to_trigger:
        dag = dags_to_trigger.pop()
        trigger = dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True,
        )
        triggers.append(trigger)
        if dag.subdags:
            dags_to_trigger.extend(dag.subdags)
    return triggers


def trigger_dag(
        dag_id,
        run_id=None,
        conf=None,
        execution_date=None,
        replace_microseconds=True,
):
    """创建dag_run，并返回此实例 ."""
    dagbag = DagBag()
    dag_run = DagRun()
    triggers = _trigger_dag(
        dag_id=dag_id,
        dag_run=dag_run,
        dag_bag=dagbag,
        run_id=run_id,
        conf=conf,
        execution_date=execution_date,
        replace_microseconds=replace_microseconds,
    )

    # 返回dag实例
    return triggers[0] if triggers else None
