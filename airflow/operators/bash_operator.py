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


from builtins import bytes
import os
import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from xTool.utils.file import TemporaryDirectory
from xTool.misc import USE_WINDOWS


class BashOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed. (templated)
    :type bash_command: string
    :param xcom_push: If xcom_push is True, the last line written to stdout
        will also be pushed to an XCom when the bash command completes.
    :type xcom_push: bool
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :type output_encoding: output encoding of bash command
    """
    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command,
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        self.log.info("Tmp dir root location: \n %s", gettempdir())

        self.lineage_data = self.bash_command
        # 创建临时目录
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            # 创建临时文件
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:
                # 将bash命令写入到临时文件中
                f.write(bytes(self.bash_command, 'utf_8'))
                f.flush()
                # 获得临时文件的名称
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info(
                    "Temporary script location: %s",
                    script_location
                )

                if USE_WINDOWS:
                    pre_exec = None
                else:
                    def pre_exec():
                        # Restore default signal disposition and invoke setsid
                        # SIG_DFL: 表示默认信号处理程序
                        for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                            if hasattr(signal, sig):
                                signal.signal(getattr(signal, sig), signal.SIG_DFL)
                        # 创建一个新的会话。新建的会话无控制终端。
                        # 帮助一个进程脱离从父进程继承而来的已打开的终端、隶属进程组和隶属的会话。 
                        os.setsid()

                self.log.info("Running command: %s", self.bash_command)
                # 执行临时bash脚本
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env,
                    preexec_fn=pre_exec)
                # 获取bash进程
                self.sp = sp
                # 获得bash进程的执行结果
                self.log.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).rstrip()
                    self.log.info(line)
                sp.wait()
                # 获得bash进程的错误码
                self.log.info(
                    "Command exited with return code %s",
                    sp.returncode
                )
                # 脚本执行失败抛出异常
                if sp.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line

    def on_kill(self):
        """杀死bash进程 ."""
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
