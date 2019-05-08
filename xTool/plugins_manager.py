# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import object
import imp
import inspect
import os
import re

from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.exceptions import XToolPluginException

log = LoggingMixin().log


class XToolPlugin(object):
    """插件基类 ."""
    name = None
    operators = []
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []

    @classmethod
    def validate(cls):
        """验证插件必须定义name静态变量 ."""
        if not cls.name:
            raise XToolPluginException("Your plugin needs a name.")


def import_plugins(plugins_folder):
    """从插件目录中导入插件，每个插件都是一个XToolPlugin对象 ."""
    plugins = []
    norm_pattern = re.compile(r'[/|.]')

    for root, dirs, files in os.walk(plugins_folder, followlinks=True):
        for f in files:
            try:
                # 获得插件目录下的所有文件路径
                filepath = os.path.join(root, f)
                if not os.path.isfile(filepath):
                    continue
                # 判断文件的后缀是否是.py
                mod_name, file_ext = os.path.splitext(
                    os.path.split(filepath)[-1])
                if file_ext != '.py':
                    continue

                log.debug('Importing plugin module %s', filepath)
                # 将路径转换为命名空间
                # normalize root path as namespace
                # e.g.:
                # __data__home__user00__application__airflow__plugins_rest_api_plugin
                # mod_name 为文件名（去掉后缀）
                namespace = '_'.join([re.sub(norm_pattern, '__', root), mod_name])

                # 根据文件名加载模块
                m = imp.load_source(namespace, filepath)

                # 将所加载模块中的 XToolPlugin 子类添加到 plugins 列表中
                # 遍历模块的属性值
                for obj in list(m.__dict__.values()):
                    # 判断模块中的类是否为 XToolPlugin 的子类
                    if (
                            inspect.isclass(obj) and
                            issubclass(obj, XToolPlugin) and
                            obj is not XToolPlugin):
                        # 验证子类中是否定义了name静态变量
                        obj.validate()
                        # 将类加入到插件列表中
                        if obj not in plugins:
                            plugins.append(obj)
            except Exception as e:
                log.exception(e)
                log.error('Failed to import plugin %s', filepath)
    return plugins
