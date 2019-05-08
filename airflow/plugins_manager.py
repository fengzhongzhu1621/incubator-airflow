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

from airflow import configuration
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.module_loading import prepare_classpath
from xTool.utils.module_loading import make_module
from xTool.plugins_manager import XToolPlugin
from xTool.plugins_manager import import_plugins

log = LoggingMixin().log


class AirflowPlugin(XToolPlugin):
    pass


# 获得插件目录
plugins_folder = configuration.conf.get('core', 'plugins_folder')
if not plugins_folder:
    plugins_folder = configuration.conf.get('core', 'airflow_home') + '/plugins'
# 将插件目录加入到系统路径中
prepare_classpath(plugins_folder)

# 导入插件
plugins = import_plugins(plugins_folder)


# Plugin components to integrate as modules
operators_modules = []
sensors_modules = []
hooks_modules = []
executors_modules = []
macros_modules = []

# Plugin components to integrate directly
admin_views = []
flask_blueprints = []
menu_links = []
flask_appbuilder_views = []
flask_appbuilder_menu_links = []

for p in plugins:
    # 创建新模块
    operators_modules.append(
        make_module('airflow.operators.' + p.name, p.operators + p.sensors))
    sensors_modules.append(
        make_module('airflow.sensors.' + p.name, p.sensors)
    )
    hooks_modules.append(make_module('airflow.hooks.' + p.name, p.hooks))
    executors_modules.append(
        make_module('airflow.executors.' + p.name, p.executors))
    macros_modules.append(make_module('airflow.macros.' + p.name, p.macros))

    admin_views.extend(p.admin_views)
    flask_blueprints.extend(p.flask_blueprints)
    menu_links.extend(p.menu_links)
    flask_appbuilder_views.extend(p.appbuilder_views)
    flask_appbuilder_menu_links.extend(p.appbuilder_menu_items)
