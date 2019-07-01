# -*- coding: utf-8 -*-

import importlib
import socket
from urllib.parse import unquote


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


def parse_netloc_to_hostname(uri_parts):
    """获得urlparse解析后的主机名
    Python automatically converts all letters to lowercase in hostname
    See: https://issues.apache.org/jira/browse/AIRFLOW-3615
    """
    hostname = unquote(uri_parts.hostname or '')
    if '/' in hostname:
        hostname = uri_parts.netloc
        if "@" in hostname:
            hostname = hostname.rsplit("@", 1)[1]
        if ":" in hostname:
            hostname = hostname.split(":", 1)[0]
        hostname = unquote(hostname)
    return hostname
 