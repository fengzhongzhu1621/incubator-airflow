# -*- coding: utf-8 -*-

from __future__ import absolute_import
# 开启除法浮点运算
from __future__ import division
from __future__ import print_function
# 把你当前模块所有的字符串（string literals）转为unicode
from __future__ import unicode_literals

import os
import json
from tempfile import mkstemp
from base64 import b64encode
from builtins import str
import copy
from collections import OrderedDict

from future import standard_library
import six
from six import iteritems

from backports.configparser import ConfigParser

from xTool.utils.helpers import expand_env_var
from xTool.utils.helpers import run_command
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.exceptions import XToolConfigException
from xTool.misc import USE_WINDOWS


standard_library.install_aliases()

log = LoggingMixin().log


def read_config_file(file_path):
    """读取默认配置 ."""
    if six.PY2:
        with open(file_path) as f:
            config = f.read()
            return config.decode('utf-8')
    else:
        with open(file_path, encoding='utf-8') as f:
            return f.read()


def parameterized_config(template):
    """使用全局变量和局部变量渲染模版字符串
    Generates a configuration from the provided template + variables defined in
    current scope
    :param template: a config content templated with {{variables}}
    """
    all_vars = {k: v for d in [globals(), locals()] for k, v in iteritems(d)}
    return template.format(**all_vars)


def read_default_config_file(file_path):
    """根据文件路径，读取文件内容 ."""
    # 获得配置文件路径名
    if six.PY2:
        with open(file_path) as f:
            config = f.read()
            return config.decode('utf-8')
    else:
        with open(file_path, encoding='utf-8') as f:
            return f.read()


def tmp_configuration_copy(cfg_dict, chmod=0o600):
    """
    Returns a path for a temporary file including a full copy of the configuration
    settings.
    :return: a path to a temporary file
    """
    cfg_dict = conf.as_dict(display_sensitive=True, raw=True)
    temp_fd, cfg_path = mkstemp()

    with os.fdopen(temp_fd, 'w') as temp_file:
        if chmod is not None:
            if not USE_WINDOWS:
                os.fchmod(temp_fd, chmod)
        json.dump(cfg_dict, temp_file)

    return cfg_path


class XToolConfigParser(ConfigParser):
    """通用配置文件解析器 .
    
    Args:
        default_config: 默认.ini格式配置文件的内容

    examples:
        # 创建配置对象，读取默认配置
        conf = AirflowConfigParser(default_config=parameterized_config(DEFAULT_CONFIG))
        # 读取正式环境配置文件，覆盖默认配置
        conf.read(AIRFLOW_CONFIG)
    """
    env_prefix = "XTOOL"
	
    # These configuration elements can be fetched as the stdout of commands
    # following the "{section}__{name}__cmd" pattern, the idea behind this
    # is to not store password on boxes in text files.
    as_command_stdout = {}

    deprecated_options = {}

    deprecation_format_string = (
        'The {old} option in [{section}] has been renamed to {new} - the old '
        'setting has been used, but please update your config.'
    )

    def __init__(self, default_config=None, *args, **kwargs):
        super(XToolConfigParser, self).__init__(*args, **kwargs)
        # 创建配置文件解析器
        self.defaults = ConfigParser(*args, **kwargs)
        # 读取默认配置
        if default_config is not None:
            self.defaults.read_string(default_config)

        self.is_validated = False

    def _validate(self):
        self.is_validated = True

    def _get_env_var_option(self, section, key):
        """把环境变量的值中包含的”~”和”~user”转换成用户目录，并获取配置结果值 ."""
        # must have format XTOOL__{SECTION}__{KEY} (note double underscore)
        env_var = '{E}__{S}__{K}'.format(E=self.env_prefix, S=section.upper(), K=key.upper())
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])

    def _get_cmd_option(self, section, key):
        """从配置项中获取指令，并执行指令获取指令执行后的返回值

            - 如果key不存在_cmd结尾，则获取key的值
            - 如果key没有配置 且 key以_cmd结尾，则获取key的值，并执行值表示的表达式，返回表达式的结果
        """
        fallback_key = key + '_cmd'
        # if this is a valid command key...
        if (section, key) in self.as_command_stdout:
            if super(XToolConfigParser, self) \
                    .has_option(section, fallback_key):
                command = super(XToolConfigParser, self) \
                    .get(section, fallback_key)
                return run_command(command)

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        # 首先从环境变量中获取配置值，如果环境变量中存在，则不再从配置文件中获取
        option = self._get_env_var_option(section, key)
        if option is not None:
            return option
        # 判断是否是过期配置
        deprecated_name = self.deprecated_options.get(section, {}).get(key, None)
        if deprecated_name:
            option = self._get_env_var_option(section, deprecated_name)
            if option is not None:
                self._warn_deprecate(section, key, deprecated_name)
                return option
        # 然后从最新的配置文件中获取
        if super(XToolConfigParser, self).has_option(section, key):
            # Use the parent's methods to get the actual config here to be able to
            # separate the config from default config.
            return expand_env_var(
                super(XToolConfigParser, self).get(section, key, **kwargs))
        if deprecated_name:
            if super(XToolConfigParser, self).has_option(section, deprecated_name):
                self._warn_deprecate(section, key, deprecated_name)
                return expand_env_var(super(XToolConfigParser, self).get(
                    section,
                    deprecated_name,
                    **kwargs
                ))

        # 获得带有_cmd后缀的配置
        # 执行表达式，获取结果
        option = self._get_cmd_option(section, key)
        if option:
            return option
        if deprecated_name:
            option = self._get_cmd_option(section, deprecated_name)
            if option:
                self._warn_deprecate(section, key, deprecated_name)
                return option

        # 从默认配置文件中获取
        if self.defaults.has_option(section, key):
            return expand_env_var(
                self.defaults.get(section, key, **kwargs))
        else:
            log.warning(
                "section/key [{section}/{key}] not found in config".format(**locals())
            )
            # 配置不存在，抛出异常
            raise XToolConfigException(
                "section/key [{section}/{key}] not found "
                "in config".format(**locals()))

    def getboolean(self, section, key):
        val = str(self.get(section, key)).lower().strip()
        # 去掉结尾的注释
        if '#' in val:
            val = val.split('#')[0].strip()
        if val.lower() in ('t', 'true', '1'):
            return True
        elif val.lower() in ('f', 'false', '0'):
            return False
        else:
            raise XToolConfigException(
                'The value for configuration option "{}:{}" is not a '
                'boolean (received "{}").'.format(section, key, val))

    def getint(self, section, key):
        return int(self.get(section, key))

    def getfloat(self, section, key):
        return float(self.get(section, key))

    def read(self, filenames):
        """读取多个最新的配置文件，进行校验，并覆盖默认配置."""
        super(XToolConfigParser, self).read(filenames)
        self._validate()

    def read_dict(self, *args, **kwargs):
        super(XToolConfigParser, self).read_dict(*args, **kwargs)
        self._validate()

    def has_option(self, section, option):
        try:
            # Using self.get() to avoid reimplementing the priority order
            # of config variables (env, config, cmd, defaults)
            self.get(section, option)
            return True
        except XToolConfigException:
            return False

    def remove_option(self, section, option, remove_default=True):
        """
        Remove an option if it exists in config from a file or
        default config. If both of config have the same option, this removes
        the option in both configs unless remove_default=False.
        """
        if super(XToolConfigParser, self).has_option(section, option):
            super(XToolConfigParser, self).remove_option(section, option)

        if self.defaults.has_option(section, option) and remove_default:
            self.defaults.remove_option(section, option)

    def getsection(self, section):
        """
        Returns the section as a dict. Values are converted to int, float, bool
        as required.
        :param section: section from the config
        :return: dict
        """
        if section not in self._sections and section not in self.defaults._sections:
            return None

        _section = copy.deepcopy(self.defaults._sections[section])

        if section in self._sections:
            _section.update(copy.deepcopy(self._sections[section]))
            # 遍历section下所有的key，对value进行格式化处理
        for key, val in iteritems(_section):
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    if val.lower() in ('t', 'true'):
                        val = True
                    elif val.lower() in ('f', 'false'):
                        val = False
            _section[key] = val
        return _section


    def _warn_deprecate(self, section, key, deprecated_name):
        warnings.warn(
            self.deprecation_format_string.format(
                old=deprecated_name,
                new=key,
                section=section,
            ),
            DeprecationWarning,
            stacklevel=3,
        )
