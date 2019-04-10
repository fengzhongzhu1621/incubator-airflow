# -*- coding: utf-8 -*-

import importlib
import socket


def get_hostname(callable_path=None):
    """获得主机名 ."""
    if not callable_path:
        return socket.getfqdn()

    # Since we have a callable path, we try to import and run it next.
    # 根据配置文件中的设置的回调函数 module_path:attr_name 获取主机名
    # 加载指定模块，获得模块定义的主机属性名
    module_path, attr_name = callable_path.split(':')
    module = importlib.import_module(module_path)
    callable = getattr(module, attr_name)
	# 执行属性方法返回结果
    return callable()
