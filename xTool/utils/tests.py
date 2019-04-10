# -*- coding: utf-8 -*-

import re
import unittest


def skipUnlessImported(module, obj):
    """如果对象不在导入的模块中，跳过被装饰的测试 .
    @skipUnlessImported('airflow.operators.mysql_operator', 'MySqlOperator')
    @skipUnlessImported('xTool.utils.tests', 'skipUnlessImported')
    """
    import importlib
    try:
        m = importlib.import_module(module)
    except ImportError:
        m = None
    return unittest.skipUnless(
        obj in dir(m),
        "Skipping test because {} could not be imported from {}".format(
            obj, module))


def assertEqualIgnoreMultipleSpaces(case, first, second, msg=None):
    """判断两个字符串相等时，忽略多个空白字符 ."""
    def _trim(s):
        """将空白字符替换为单个空格 ."""
        return re.sub("\s+", " ", s.strip())
    return case.assertEqual(_trim(first), _trim(second), msg)
