#coding: utf-8

import os
import sys
import threading
import traceback

from six import iteritems


def sigint_handler(sig, frame):
    """符合POSIX平台，信号情报是由它的控制终端，
    当用户希望中断该过程发送到处理的信号。通常ctrl-C，但在某些系统上，“删除”字符或“break”键 - 当进程的控制终端的用户按下中断正在运行的进程的关键SIGINT被发送。 """
    sys.exit(0)


def sigquit_handler(sig, frame):
    """Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\

    在POSIX兼容的平台，SIGQUIT是其控制终端发送到进程，当用户请求的过程中执行核心转储的信号。

    和SIGINT类似, 但由QUIT字符(通常是Ctrl-\)来控制. 进程在因收到SIGQUIT退出时会产生core文件, 在这个意义上类似于一个程序错误信号。
    """
    # 获得主线程
    print("Dumping stack traces for all threads in PID {}".format(os.getpid()))
    # 获得活动子线程的线程ID和名称的映射关系，包括主线程
    id_to_name = {th.ident: th.name for th in threading.enumerate()}
    code = []
    # 获得线程栈
    for thread_id, stack in iteritems(sys._current_frames()):
        code.append("\n# Thread: {}({})"
                    .format(id_to_name.get(thread_id, ""), thread_id))
        for filename, line_number, name, line in traceback.extract_stack(stack):
            code.append('File: "{}", line {}, in {}'
                        .format(filename, line_number, name))
            if line:
                code.append("  {}".format(line.strip()))
    print("\n".join(code))
