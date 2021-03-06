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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime as dt
from flask_admin.form import DateTimePickerWidget
from wtforms import DateTimeField, SelectField
from flask_wtf import FlaskForm


class DateTimeForm(FlaskForm):
    # Date filter form for task views
    execution_date = DateTimeField(
        "Execution date", widget=DateTimePickerWidget())


class DateTimeWithNumRunsForm(FlaskForm):
    # Date time and number of runs form for tree view, task duration
    # and landing times
    base_date = DateTimeField(
        "Anchor date", widget=DateTimePickerWidget(), default=dt.datetime.now())
    num_runs = SelectField("Number of runs", default=25, choices=(
        (5, "5"),
        (25, "25"),
        (50, "50"),
        (100, "100"),
        (365, "365"),
    ))


class DateTimeWithNumRunsWithDagRunsForm(DateTimeWithNumRunsForm):
    # Date time and number of runs and dag runs form for graph and gantt view
    execution_date = SelectField("DAG run")
