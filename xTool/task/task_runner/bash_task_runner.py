# -*- coding: utf-8 -*-

import psutil

from xTool.task.task_runner.base_task_runner import BaseTaskRunner
from xTool.utils.helpers import reap_process_group


class BashTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task by invoking through the Bash shell.
    """
    def __init__(self, local_task_job, conf):
        super(BashTaskRunner, self).__init__(local_task_job, conf)

    def start(self):
        """执行bash命令 .

        如果用-c 那么bash 会从第一个非选项参数后面的字符串中读取命令，如果字符串有多个空格，第一个空格前面的字符串是要执行的命令，也就是$0, 后面的是参数，即$1, $2....

        bash -c "cmd string"
        bash -c "./atest hello world"
        """
        self.process = self.run_command(['bash', '-c'], join_args=True)

    def return_code(self):
        """检查子进程状态

            0 正常结束
            1 sleep
            2 子进程不存在
            -15 kill
            None 在运行

            poll() == 0 判断子进程是否正常结束 正确

        """
        return self.process.poll()

    def terminate(self):
        """终止bash进程 ."""
        if self.process and psutil.pid_exists(self.process.pid):
            reap_process_group(self.process.pid, self.log, self.conf.getint(
                'core', 'KILLED_TASK_CLEANUP_TIME'
            ))

    def on_finish(self):
        """删除临时配置文件 ."""
        super(BashTaskRunner, self).on_finish()
