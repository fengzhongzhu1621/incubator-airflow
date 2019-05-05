# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import getpass
import os
import subprocess
import threading

from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.utils.configuration import tmp_configuration_copy
from xTool.misc import USE_WINDOWS
from xTool.exceptions import AirflowConfigException
from xTool.exceptions import XToolConfigException


PYTHONPATH_VAR = 'PYTHONPATH'


class BaseTaskRunner(LoggingMixin):
    """
    Runs Airflow task instances by invoking the `airflow run` command with raw
    mode enabled in a subprocess.
    """
    def __init__(self, local_task_job, conf):
        """
        :param local_task_job: The local task job associated with running the
        associated task instance. job表中类型为LocalTaskJob的记录，即消费者job.
        :type local_task_job: airflow.jobs.LocalTaskJob
        """
        # 文件配置对象
        self.conf = conf
        # Pass task instance context into log handlers to setup the logger.
        super(BaseTaskRunner, self).__init__(local_task_job.task_instance)
        # 获得job关联的任务实例
        self._task_instance = local_task_job.task_instance

        popen_prepend = []
        # 获得任务设置的执行用户
        if self._task_instance.run_as_user:
            self.run_as_user = self._task_instance.run_as_user
        else:
            try:
                self.run_as_user = self.conf.get('core', 'default_impersonation')
            except XToolConfigException:
                self.run_as_user = None

        # 将配置文件转换为字典
        cfg_dict = self.conf.as_dict(display_sensitive=True, raw=True)
        # 将配置字典复制到临时文件中，并返回临时文件的路径
        self._cfg_path = tmp_configuration_copy(cfg_dict)

        # 修改临时文件的用户
        self.log.debug("Planning to run as the %s user", self.run_as_user)
        if self.run_as_user and (self.run_as_user != getpass.getuser()):
            # Give ownership of file to user; only they can read and write
            subprocess.call(
                ['sudo', 'chown', self.run_as_user, self._cfg_path],
                close_fds=True
            )

            # propagate PYTHONPATH environment variable
            # 用于在导入模块的时候搜索路径，例如 export PYTHONPATH=$PYTHONPATH:`pwd`:'pwd'/slim
            pythonpath_value = os.environ.get(PYTHONPATH_VAR, '')
            popen_prepend = ['sudo', '-E', '-H', '-u', self.run_as_user]

            if pythonpath_value:
                popen_prepend.append('{}={}'.format(PYTHONPATH_VAR, pythonpath_value))

        # 构造worker需要执行的命令
        self._command = popen_prepend + self._task_instance.command_as_list(
            raw=True,
            pickle_id=local_task_job.pickle_id,
            mark_success=local_task_job.mark_success,
            job_id=local_task_job.id,
            pool=local_task_job.pool,
            cfg_path=self._cfg_path,
        )
        self.process = None

    def _read_task_logs(self, stream):
        while True:
            # 阻塞操作
            line = stream.readline()
            # 转换为unicode编码
            if isinstance(line, bytes):
                line = line.decode('utf-8')
            if not line:
                break
            self.log.info('Job %s: Subtask %s %s',
                          self._task_instance.job_id, self._task_instance.task_id,
                          line.rstrip('\n'))

    def run_command(self, run_with, join_args=False):
        """运行任务实例： airflow run --raw
        Run the task command

        :param run_with: list of tokens to run the task command with
        E.g. ['bash', '-c']
        :type run_with: list
        :param join_args: whether to concatenate the list of command tokens
        E.g. ['airflow', 'run'] vs ['airflow run']
        :param join_args: bool
        :return: the process that was run
        :rtype: subprocess.Popen
        """
        # 运行operator
        cmd = [" ".join(self._command)] if join_args else self._command
        full_cmd = run_with + cmd
        self.log.info('Running: %s', full_cmd)
        if USE_WINDOWS:
            preexec_fn = None
            shell = True
        else:
            preexec_fn = os.setsid
            shell = False
        proc = subprocess.Popen(
            full_cmd,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            close_fds=False if USE_WINDOWS else True,
            env=os.environ.copy(),
            preexec_fn=preexec_fn
        )

        # 启动一个线程，在命令执行完成后记录相关日志
        # airflow run --raw 命令执行完成后打印日志
        # Start daemon thread to read subprocess logging output
        log_reader = threading.Thread(
            target=self._read_task_logs,
            args=(proc.stdout,),
        )
        # 主线程结束时，子线程也随之结束
        log_reader.daemon = True
        log_reader.start()
        return proc

    def start(self):
        """
        Start running the task instance in a subprocess.
        """
        raise NotImplementedError()

    def return_code(self):
        """
        :return: The return code associated with running the task instance or
        None if the task is not yet done.
        :rtype int:
        """
        raise NotImplementedError()

    def terminate(self):
        """
        Kill the running task instance.
        """
        raise NotImplementedError()

    def on_finish(self):
        """删除临时配置文件 ."""
        if USE_WINDOWS:
            try:
                os.remove(self._cfg_path)
            except FileNotFoundError:
                pass
        elif self._cfg_path and os.path.isfile(self._cfg_path):
            subprocess.call(['sudo', 'rm', self._cfg_path], close_fds=True)
