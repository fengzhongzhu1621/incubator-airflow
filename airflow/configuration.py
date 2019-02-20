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
# 开启除法浮点运算
from __future__ import division
from __future__ import print_function
# 把你当前模块所有的字符串（string literals）转为unicode
from __future__ import unicode_literals

from builtins import str
from collections import OrderedDict
import copy
import errno
from future import standard_library
import os
import shlex
import six
from six import iteritems
import subprocess
import sys
import warnings

from backports.configparser import ConfigParser
from zope.deprecation import deprecated as _deprecated
from xTool.crypto.fernet import generate_fernet_key
#from xTool.utils.configuration import parameterized_config
from xTool.utils.helpers import expand_env_var, run_command
from xTool.utils.configuration import read_default_config_file
from xTool.utils.configuration import XToolConfigParser
from xTool.utils.file import mkdir_p

from airflow.exceptions import AirflowConfigException
from airflow.utils.log.logging_mixin import LoggingMixin

standard_library.install_aliases()

log = LoggingMixin().log

# 控制警告错误的输出
# show Airflow's deprecation warnings
warnings.filterwarnings(
    action='default', category=DeprecationWarning, module='airflow')
warnings.filterwarnings(
    action='default', category=PendingDeprecationWarning, module='airflow')


def parameterized_config(template):
    """使用全局变量和局部变量渲染模版字符串
    Generates a configuration from the provided template + variables defined in
    current scope
    :param template: a config content templated with {{variables}}
    """
    all_vars = {k: v for d in [globals(), locals()] for k, v in iteritems(d)}
    return template.format(**all_vars)


def _read_default_config_file(file_name):
    """读取默认配置 ."""
    # 获得默认配置路径
    templates_dir = os.path.join(os.path.dirname(__file__), 'config_templates')
    # 获得配置文件路径名
    file_path = os.path.join(templates_dir, file_name)
    return read_default_config_file(file_path)


class AirflowConfigParser(XToolConfigParser):
    env_prefix = "AIRFLOW"
	
    # These configuration elements can be fetched as the stdout of commands
    # following the "{section}__{name}__cmd" pattern, the idea behind this
    # is to not store password on boxes in text files.
    as_command_stdout = {
        ('core', 'sql_alchemy_conn'),
        ('core', 'fernet_key'),
        ('celery', 'broker_url'),
        ('celery', 'result_backend'),
        # Todo: remove this in Airflow 1.11
        ('celery', 'celery_result_backend'),
        ('atlas', 'password'),
        ('smtp', 'smtp_password'),
        ('ldap', 'bind_password'),
        ('kubernetes', 'git_password'),
    }

    # A two-level mapping of (section -> new_name -> old_name). When reading
    # new_name, the old_name will be checked to see if it exists. If it does a
    # DeprecationWarning will be issued and the old name will be used instead
    deprecated_options = {
        'celery': {
            # Remove these keys in Airflow 1.11
            'worker_concurrency': 'celeryd_concurrency',
            'result_backend': 'celery_result_backend',
            'broker_url': 'celery_broker_url',
            'ssl_active': 'celery_ssl_active',
            'ssl_cert': 'celery_ssl_cert',
            'ssl_key': 'celery_ssl_key',
        }
    }

    def __init__(self, default_config=None, *args, **kwargs):
        super(AirflowConfigParser, self).__init__(default_config, *args, **kwargs)
        self.airflow_defaults = self.defaults

    def _validate(self):
        if (
                self.get("core", "executor") != 'SequentialExecutor' and
                "sqlite" in self.get('core', 'sql_alchemy_conn')):
            # sqlite数据库只能使用SequentialExecutor
            raise AirflowConfigException(
                "error: cannot use sqlite with the {}".format(
                    self.get('core', 'executor')))

        elif (
            self.getboolean("webserver", "authenticate") and
            self.get("webserver", "owner_mode") not in ['user', 'ldapgroup']
        ):
            # 如果开启了webserver认证，则dag所有人只能是user, ldapgroup
            raise AirflowConfigException(
                "error: owner_mode option should be either "
                "'user' or 'ldapgroup' when filtering by owner is set")

        elif (
            # 如果开启了webserver认证，启用了ldapgroup，则认证后端必须是ldap_auth
            self.getboolean("webserver", "authenticate") and
            self.get("webserver", "owner_mode").lower() == 'ldapgroup' and
            self.get("webserver", "auth_backend") != (
                'airflow.contrib.auth.backends.ldap_auth')
        ):
            raise AirflowConfigException(
                "error: attempt at using ldapgroup "
                "filtering without using the Ldap backend")

        self.is_validated = True

    def as_dict(
            self, display_source=False, display_sensitive=False, raw=False):
        """
        Returns the current configuration as an OrderedDict of OrderedDicts.
        :param display_source: If False, the option value is returned. If True,
            a tuple of (option_value, source) is returned. Source is either
            'airflow.cfg', 'default', 'env var', or 'cmd'.
        :type display_source: bool
        :param display_sensitive: If True, the values of options set by env
            vars and bash commands will be displayed. If False, those options
            are shown as '< hidden >'
        :type display_sensitive: bool
        :param raw: Should the values be output as interpolated values, or the
            "raw" form that can be fed back in to ConfigParser
        :type raw: bool
        """
        cfg = {}
        configs = [
            ('default', self.airflow_defaults),
            ('airflow.cfg', self),
        ]

        for (source_name, config) in configs:
            for section in config.sections():
                sect = cfg.setdefault(section, OrderedDict())
                for (k, val) in config.items(section=section, raw=raw):
                    if display_source:
                        val = (val, source_name)
                    sect[k] = val

        # add env vars and overwrite because they have priority
        for ev in [ev for ev in os.environ if ev.startswith('AIRFLOW__')]:
            try:
                _, section, key = ev.split('__')
                opt = self._get_env_var_option(section, key)
            except ValueError:
                continue
            if (not display_sensitive and ev != 'AIRFLOW__CORE__UNIT_TEST_MODE'):
                opt = '< hidden >'
            elif raw:
                opt = opt.replace('%', '%%')
            if display_source:
                opt = (opt, 'env var')
            cfg.setdefault(section.lower(), OrderedDict()).update(
                {key.lower(): opt})

        # add bash commands
        for (section, key) in self.as_command_stdout:
            opt = self._get_cmd_option(section, key)
            if opt:
                if not display_sensitive:
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'cmd')
                elif raw:
                    opt = opt.replace('%', '%%')
                cfg.setdefault(section, OrderedDict()).update({key: opt})
                del cfg[section][key + '_cmd']

        return cfg

    def load_test_config(self):
        """
        Load the unit test configuration.

        Note: this is not reversible.
        """
        # override any custom settings with defaults
        self.read_string(parameterized_config(DEFAULT_CONFIG))
        # then read test config
        self.read_string(parameterized_config(TEST_CONFIG))
        # then read any "custom" test settings
        self.read(TEST_CONFIG_FILE)


# Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
# "~/airflow" and "~/airflow/airflow.cfg" respectively as defaults.

# 读取配置文件
DEFAULT_CONFIG = _read_default_config_file('default_airflow.cfg')
TEST_CONFIG = _read_default_config_file('default_test.cfg')

# 创建配置文件目录
if 'AIRFLOW_HOME' not in os.environ:
    AIRFLOW_HOME = expand_env_var('~/airflow')
else:
    AIRFLOW_HOME = expand_env_var(os.environ['AIRFLOW_HOME'])
mkdir_p(AIRFLOW_HOME)

# 获得配置文件路径
if 'AIRFLOW_CONFIG' not in os.environ:
    # 首先从当前用户的home路径下获取配置文件
    # 然后从AIRFLOW_HOME下的获取
    if os.path.isfile(expand_env_var('~/airflow.cfg')):
        AIRFLOW_CONFIG = expand_env_var('~/airflow.cfg')
    else:
        AIRFLOW_CONFIG = AIRFLOW_HOME + '/airflow.cfg'
else:
    # 从环境变量中获取配置文件路径
    AIRFLOW_CONFIG = expand_env_var(os.environ['AIRFLOW_CONFIG'])

# 获得测试目录下dags的目录
# Set up dags folder for unit tests
# this directory won't exist if users install via pip
_TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    'tests',
    'dags')
if os.path.exists(_TEST_DAGS_FOLDER):
    TEST_DAGS_FOLDER = _TEST_DAGS_FOLDER
else:
    TEST_DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')

# 获得测试目录下plugins的目录
# Set up plugins folder for unit tests
_TEST_PLUGINS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    'tests',
    'plugins')
if os.path.exists(_TEST_PLUGINS_FOLDER):
    TEST_PLUGINS_FOLDER = _TEST_PLUGINS_FOLDER
else:
    TEST_PLUGINS_FOLDER = os.path.join(AIRFLOW_HOME, 'plugins')


# 获得单元测试配置文件
TEST_CONFIG_FILE = AIRFLOW_HOME + '/unittests.cfg'

# 如果需要创建一个新的配置文件，则需要产生一个44字节的随机数
# only generate a Fernet key if we need to create a new config file
if not os.path.isfile(TEST_CONFIG_FILE) or not os.path.isfile(AIRFLOW_CONFIG):
    FERNET_KEY = generate_fernet_key()
else:
    FERNET_KEY = ''

# 自动生成单元测试配置文件
TEMPLATE_START = (
    '# ----------------------- TEMPLATE BEGINS HERE -----------------------')
if not os.path.isfile(TEST_CONFIG_FILE):
    log.info(
        'Creating new Airflow config file for unit tests in: %s', TEST_CONFIG_FILE
    )
    with open(TEST_CONFIG_FILE, 'w') as f:
        cfg = parameterized_config(TEST_CONFIG)
        f.write(cfg.split(TEMPLATE_START)[-1].strip())

# 自动生成默认配置文件
if not os.path.isfile(AIRFLOW_CONFIG):
    log.info(
        'Creating new Airflow config file in: %s',
        AIRFLOW_CONFIG
    )
    with open(AIRFLOW_CONFIG, 'w') as f:
        cfg = parameterized_config(DEFAULT_CONFIG)
        cfg = cfg.split(TEMPLATE_START)[-1].strip()
        if six.PY2:
            cfg = cfg.encode('utf8')
        f.write(cfg)

log.info("Reading the config from %s", AIRFLOW_CONFIG)

# 创建配置对象，读取默认配置
conf = AirflowConfigParser(default_config=parameterized_config(DEFAULT_CONFIG))
# 读取正式环境配置文件，覆盖默认配置
conf.read(AIRFLOW_CONFIG)

# 自动生成rbac webserver配置文件
if conf.getboolean('webserver', 'rbac'):
    # 读取默认webserver配置
    DEFAULT_WEBSERVER_CONFIG = _read_default_config_file('default_webserver_config.py')

    WEBSERVER_CONFIG = AIRFLOW_HOME + '/webserver_config.py'

    if not os.path.isfile(WEBSERVER_CONFIG):
        log.info('Creating new FAB webserver config file in: %s', WEBSERVER_CONFIG)
        with open(WEBSERVER_CONFIG, 'w') as f:
            f.write(DEFAULT_WEBSERVER_CONFIG)

# 加载测试配置
if conf.getboolean('core', 'unit_test_mode'):
    conf.load_test_config()

# Historical convenience functions to access config entries

load_test_config = conf.load_test_config
get = conf.get
getboolean = conf.getboolean
getfloat = conf.getfloat
getint = conf.getint
getsection = conf.getsection
has_option = conf.has_option
remove_option = conf.remove_option
as_dict = conf.as_dict
set = conf.set # noqa

for func in [load_test_config, get, getboolean, getfloat, getint, has_option,
             remove_option, as_dict, set]:
    _deprecated(
        func,
        "Accessing configuration method '{f.__name__}' directly from "
        "the configuration module is deprecated. Please access the "
        "configuration from the 'configuration.conf' object via "
        "'conf.{f.__name__}'".format(f=func))
