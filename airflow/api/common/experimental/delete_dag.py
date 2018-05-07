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

from airflow import models, settings
from airflow.exceptions import AirflowException
from sqlalchemy import or_


class DagFileExists(AirflowException):
    status = 400


class DagNotFound(AirflowException):
    status = 404


def delete_dag(dag_id):
    """根据dag_id删除dag .
    dag_id可以是dat_id和task_id的组合，中间用.分割
    """
    session = settings.Session()

    # 根据dag_id获得dag，如果不存在抛出异常
    DM = models.DagModel
    dag = session.query(DM).filter(DM.dag_id == dag_id).first()
    if dag is None:
        raise DagNotFound("Dag id {} not found".format(dag_id))

    # 如果dag已经被加载，说明dag文件还存在，抛出异常，不能删除
    dagbag = models.DagBag()
    if dag_id in dagbag.dags:
        raise DagFileExists("Dag id {} is still in DagBag. "
                            "Remove the DAG file first.".format(dag_id))

    count = 0

    # 获得注册的所有模型
    for m in models.Base._decl_class_registry.values():
        # 如果模型中包含dag_id属性，则根据dag_id从指定的模型中删除记录
        if hasattr(m, "dag_id"):
            # 注意：dag子集中的dag_id = dag_id + .
            cond = or_(m.dag_id == dag_id, m.dag_id.like(dag_id + ".%"))
            # TODO 为什么使用fetch模式
            count += session.query(m).filter(
                cond).delete(synchronize_session='fetch')

    # 删除dag的子集
    # TODO 不明白对rsplit的处理逻辑
    if dag.is_subdag:
        p, c = dag_id.rsplit(".", 1)
        for m in models.DagRun, models.TaskFail, models.TaskInstance:
            count += session.query(m).filter(m.dag_id ==
                                             p, m.task_id == c).delete()

    session.commit()

    # 返回删除的记录总数
    return count
