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
#
"""
提供DB相关的工具类
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from functools import wraps

import os
import contextlib

from airflow import settings
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.decorators.db import provide_session

log = LoggingMixin().log


@provide_session
def merge_conn(conn, session=None):
    """添加连接记录 ."""
    from airflow import models
    C = models.Connection
    # 如果连接记录conn不存在连接表中，则将此连接记录新增到表中
    if not session.query(C).filter(C.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()


def initdb(rbac=False):
    session = settings.Session()

    from airflow import models
    upgradedb()

    merge_conn(
        models.Connection(
            conn_id='airflow_db', conn_type='mysql',
            host='localhost', login='root', password='',
            schema='airflow'))
    merge_conn(
        models.Connection(
            conn_id='airflow_ci', conn_type='mysql',
            host='localhost', login='root', extra="{\"local_infile\": true}",
            schema='airflow_ci'))
    merge_conn(
        models.Connection(
            conn_id='beeline_default', conn_type='beeline', port="10000",
            host='localhost', extra="{\"use_beeline\": true, \"auth\": \"\"}",
            schema='default'))
    merge_conn(
        models.Connection(
            conn_id='bigquery_default', conn_type='google_cloud_platform',
            schema='default'))
    merge_conn(
        models.Connection(
            conn_id='local_mysql', conn_type='mysql',
            host='localhost', login='airflow', password='airflow',
            schema='airflow'))
    merge_conn(
        models.Connection(
            conn_id='presto_default', conn_type='presto',
            host='localhost',
            schema='hive', port=3400))
    merge_conn(
        models.Connection(
            conn_id='google_cloud_default', conn_type='google_cloud_platform',
            schema='default',))
    merge_conn(
        models.Connection(
            conn_id='hive_cli_default', conn_type='hive_cli',
            schema='default',))
    merge_conn(
        models.Connection(
            conn_id='hiveserver2_default', conn_type='hiveserver2',
            host='localhost',
            schema='default', port=10000))
    merge_conn(
        models.Connection(
            conn_id='metastore_default', conn_type='hive_metastore',
            host='localhost', extra="{\"authMechanism\": \"PLAIN\"}",
            port=9083))
    merge_conn(
        models.Connection(
            conn_id='mongo_default', conn_type='mongo',
            host='localhost', port=27017))
    merge_conn(
        models.Connection(
            conn_id='mysql_default', conn_type='mysql',
            login='root',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='postgres_default', conn_type='postgres',
            login='postgres',
            schema='airflow',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='sqlite_default', conn_type='sqlite',
            host='/tmp/sqlite_default.db'))
    merge_conn(
        models.Connection(
            conn_id='http_default', conn_type='http',
            host='https://www.google.com/'))
    merge_conn(
        models.Connection(
            conn_id='mssql_default', conn_type='mssql',
            host='localhost', port=1433))
    merge_conn(
        models.Connection(
            conn_id='vertica_default', conn_type='vertica',
            host='localhost', port=5433))
    merge_conn(
        models.Connection(
            conn_id='wasb_default', conn_type='wasb',
            extra='{"sas_token": null}'))
    merge_conn(
        models.Connection(
            conn_id='webhdfs_default', conn_type='hdfs',
            host='localhost', port=50070))
    merge_conn(
        models.Connection(
            conn_id='ssh_default', conn_type='ssh',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='sftp_default', conn_type='sftp',
            host='localhost', port=22, login='travis',
            extra='''
                {"key_file": "~/.ssh/id_rsa", "no_host_key_check": true}
            '''))
    merge_conn(
        models.Connection(
            conn_id='fs_default', conn_type='fs',
            extra='{"path": "/"}'))
    merge_conn(
        models.Connection(
            conn_id='aws_default', conn_type='aws',
            extra='{"region_name": "us-east-1"}'))
    merge_conn(
        models.Connection(
            conn_id='spark_default', conn_type='spark',
            host='yarn', extra='{"queue": "root.default"}'))
    merge_conn(
        models.Connection(
            conn_id='druid_broker_default', conn_type='druid',
            host='druid-broker', port=8082, extra='{"endpoint": "druid/v2/sql"}'))
    merge_conn(
        models.Connection(
            conn_id='druid_ingest_default', conn_type='druid',
            host='druid-overlord', port=8081, extra='{"endpoint": "druid/indexer/v1/task"}'))
    merge_conn(
        models.Connection(
            conn_id='redis_default', conn_type='redis',
            host='localhost', port=6379,
            extra='{"db": 0}'))
    merge_conn(
        models.Connection(
            conn_id='sqoop_default', conn_type='sqoop',
            host='rmdbs', extra=''))
    merge_conn(
        models.Connection(
            conn_id='emr_default', conn_type='emr',
            extra='''
                {   "Name": "default_job_flow_name",
                    "LogUri": "s3://my-emr-log-bucket/default_job_flow_location",
                    "ReleaseLabel": "emr-4.6.0",
                    "Instances": {
                        "Ec2KeyName": "mykey",
                        "Ec2SubnetId": "somesubnet",
                        "InstanceGroups": [
                            {
                                "Name": "Master nodes",
                                "Market": "ON_DEMAND",
                                "InstanceRole": "MASTER",
                                "InstanceType": "r3.2xlarge",
                                "InstanceCount": 1
                            },
                            {
                                "Name": "Slave nodes",
                                "Market": "ON_DEMAND",
                                "InstanceRole": "CORE",
                                "InstanceType": "r3.2xlarge",
                                "InstanceCount": 1
                            }
                        ],
                        "TerminationProtected": false,
                        "KeepJobFlowAliveWhenNoSteps": false
                    },
                    "Applications":[
                        { "Name": "Spark" }
                    ],
                    "VisibleToAllUsers": true,
                    "JobFlowRole": "EMR_EC2_DefaultRole",
                    "ServiceRole": "EMR_DefaultRole",
                    "Tags": [
                        {
                            "Key": "app",
                            "Value": "analytics"
                        },
                        {
                            "Key": "environment",
                            "Value": "development"
                        }
                    ]
                }
            '''))
    merge_conn(
        models.Connection(
            conn_id='databricks_default', conn_type='databricks',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='qubole_default', conn_type='qubole',
            host= 'localhost'))
    merge_conn(
        models.Connection(
            conn_id='segment_default', conn_type='segment',
            extra='{"write_key": "my-segment-write-key"}')),
    merge_conn(
        models.Connection(
            conn_id='azure_data_lake_default', conn_type='azure_data_lake',
            extra='{"tenant": "<TENANT>", "account_name": "<ACCOUNTNAME>" }'))
    merge_conn(
        models.Connection(
            conn_id='cassandra_default', conn_type='cassandra',
            host='localhost', port=9042))

    # 添加 KnownEventType 记录
    # Known event types
    KET = models.KnownEventType
    # 假期
    if not session.query(KET).filter(KET.know_event_type == 'Holiday').first():
        session.add(KET(know_event_type='Holiday'))
    # （尤指电力的）断供期，断供
    if not session.query(KET).filter(KET.know_event_type == 'Outage').first():
        session.add(KET(know_event_type='Outage'))
    # 自然灾害
    if not session.query(KET).filter(
            KET.know_event_type == 'Natural Disaster').first():
        session.add(KET(know_event_type='Natural Disaster'))
    # 营销活动
    if not session.query(KET).filter(
            KET.know_event_type == 'Marketing Campaign').first():
        session.add(KET(know_event_type='Marketing Campaign'))
    session.commit()

    dagbag = models.DagBag()
    # Save individual DAGs in the ORM
    for dag in dagbag.dags.values():
        dag.sync_to_db()
    # Deactivate the unknown ones
    # 将dag表中不再active_dag_ids中的记录标记为禁用
    models.DAG.deactivate_unknown_dags(dagbag.dags.keys())

    Chart = models.Chart
    chart_label = "Airflow task instance by type"
    chart = session.query(Chart).filter(Chart.label == chart_label).first()
    if not chart:
        chart = Chart(
            label=chart_label,
            conn_id='airflow_db',
            chart_type='bar',
            x_is_date=False,
            sql=(
                "SELECT state, COUNT(1) as number "
                "FROM task_instance "
                "WHERE dag_id LIKE 'example%' "
                "GROUP BY state"),
        )
        session.add(chart)
        session.commit()

    if rbac:
        from flask_appbuilder.security.sqla import models
        from flask_appbuilder.models.sqla import Base
        Base.metadata.create_all(settings.engine)


def upgradedb():
    # alembic adds significant import time, so we import it lazily
    from alembic import command
    from alembic.config import Config

    log.info("Creating tables")

    # 获得migrations目录路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    package_dir = os.path.normpath(os.path.join(current_dir, '..'))
    directory = os.path.join(package_dir, 'migrations')

    # 读取配置并设置
    config = Config(os.path.join(package_dir, 'alembic.ini'))
    config.set_main_option('script_location', directory.replace('%', '%%'))
    config.set_main_option('sqlalchemy.url', settings.SQL_ALCHEMY_CONN.replace('%', '%%'))

    # 更新DB
    command.upgrade(config, 'heads')


def resetdb(rbac):
    """
    Clear out the database
    """
    from airflow import models

    # alembic adds significant import time, so we import it lazily
    from alembic.migration import MigrationContext

    log.info("Dropping tables that exist")

    models.Base.metadata.drop_all(settings.engine)
    mc = MigrationContext.configure(settings.engine)
    if mc._version.exists(settings.engine):
        mc._version.drop(settings.engine)

    if rbac:
        # drop rbac security tables
        from flask_appbuilder.security.sqla import models
        from flask_appbuilder.models.sqla import Base
        Base.metadata.drop_all(settings.engine)

    initdb(rbac)
