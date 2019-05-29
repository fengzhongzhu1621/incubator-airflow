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

from __future__ import absolute_import
from datetime import datetime, timedelta
import dateutil # noqa
from random import random # noqa
import time # noqa
from . import hive # noqa
import uuid # noqa

from xTool.utils.dates import ds_add, ds_format


def _integrate_plugins():
    """Integrate plugins to the context"""
    import sys
    from airflow.plugins_manager import macros_modules
    for macros_module in macros_modules:
        sys.modules[macros_module.__name__] = macros_module
        globals()[macros_module._name] = macros_module

        ##########################################################
        # TODO FIXME Remove in Airflow 2.0

        import os as _os
        if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
            from zope.deprecation import deprecated as _deprecated
            for _macro in macros_module._objects:
                macro_name = _macro.__name__
                globals()[macro_name] = _macro
                _deprecated(
                    macro_name,
                    "Importing plugin macro '{i}' directly from "
                    "'airflow.macros' has been deprecated. Please "
                    "import from 'airflow.macros.[plugin_module]' "
                    "instead. Support for direct imports will be dropped "
                    "entirely in Airflow 2.0.".format(i=macro_name))
