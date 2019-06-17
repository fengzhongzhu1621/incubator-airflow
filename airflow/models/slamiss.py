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

from sqlalchemy import Boolean, Column, String, Text
from sqlalchemy import DateTime

from airflow.models.base import Base, ID_LEN


class SlaMiss(Base):
    """SLA(正常运行时间保障协议)
    SLA本质是一份服务水平保障协议，是服务供应商和客户之间达成的协议，它定义了前者像后者提供的服务级别和服务水平保证。无论租用共享空间、VPS、云服务器还是香港独立服务器，都需要SLA的保障。它的本质是保证您的网站或应用不应任何原因导致业务中止的比例。未符合您需求的正常运行时间SLA，可能无法保证您的网站或应用的可用性，从而使您丢失客户，并损害您的运营。
    SLA则是服务商与您达成的正常运行时间保证。
    Model that stores a history of the SLA that have been missed.
    It is used to keep track of SLA failures over time and to avoid double
    triggering alert emails.
    """
    __tablename__ = "sla_miss"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    email_sent = Column(Boolean, default=False)
    timestamp = Column(DateTime)
    description = Column(Text)
    notification_sent = Column(Boolean, default=False)

    def __repr__(self):
        return str((
            self.dag_id, self.task_id, self.execution_date.isoformat()))
