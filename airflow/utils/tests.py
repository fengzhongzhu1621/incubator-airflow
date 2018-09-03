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

import re
import unittest


def skipUnlessImported(module, obj):
    """如果对象不在导入的模块中，跳过被装饰的测试 .
    
    @skipUnlessImported('airflow.operators.mysql_operator', 'MySqlOperator')
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
    """鍒ゆ柇涓や釜瀛楃涓茬浉绛夋椂锛屽拷鐣ュ涓┖鐧藉瓧绗� ."""
    def _trim(s):
        """灏嗙┖鐧藉瓧绗︽浛鎹负鍗曚釜绌烘牸 ."""
        return re.sub("\s+", " ", s.strip())
    return case.assertEqual(_trim(first), _trim(second), msg)
