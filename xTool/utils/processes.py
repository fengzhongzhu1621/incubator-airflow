#coding: utf-8

import os
try:
    import pwd
    import grp
except ImportError:
    pass
import psutil


# the prefix to append to gunicorn worker processes after init
GUNICORN_WORKER_READY_PREFIX = "[ready] "


def uid_to_name( uid ):
    """用户ID转换为用户名称 ."""
    return pwd.getpwuid(uid).pw_name


def gid_to_name(gid):
    """组ID转换为组名 。"""
    return grp.getgrgid( gid ).gr_name


def name_to_uid( name ):
    """用户名转换为用户ID ."""
    return pwd.getpwnam( name ).pw_uid


def name_to_gid( name ):
    """组名转换为组ID ."""
    return grp.getgrnam( name ).gr_gid


def which(name, path=None):
    """
    从指定路径或环境变量PATH中遍历所有的目录，判断文件名name是否在PATH目录下且可访问
    Look for an executable file of the given name in the given list of directories,
    or the directories listed in the PATH variable of the current environment. Roughly the
    equivalent of the `which` program. Does not work on Windows.

    :type name: str
    :param name: the name of the program

    :type path: Iterable
    :param path: the directory paths to consider or None if the directories referenced in the
    PATH environment variable should be used instead

    :returns: an iterator yielding the full path to every occurrance of an executable file of the
    given name in a directory on the given path or the PATH environment variable if no path was
    passed

    >>> next( which('ls') )
    '/bin/ls'
    >>> list( which('asdalskhvxjvkjhsdasdnbmfiewwewe') )
    []
    >>> list( which('ls', path=()) )
    []
    """
    if path is None:
        path = os.environ.get('PATH')
        if path is None:
            return
        path = path.split(os.pathsep)
    for bin_dir in path:
        executable_path = os.path.join(bin_dir, name)
        if os.access(executable_path, os.X_OK):
            yield executable_path


def get_num_ready_workers_running(gunicorn_master_proc):
    """获得进程状态为ready的gunicorn的子进程的数量 ."""
    # 获得进程的所有子进程
    workers = psutil.Process(gunicorn_master_proc.pid).children()

    def ready_prefix_on_cmdline(proc):
        try:
            # 获得启动进程的命令行
            cmdline = proc.cmdline()
            # 判断是用gunicorn启动的进程，且进程状态为ready
            if cmdline:
                return GUNICORN_WORKER_READY_PREFIX in cmdline[0]
        except psutil.NoSuchProcess:
            pass
        return False

    # 获得状态为ready的gunicorn子进程
    ready_workers = [proc for proc in workers if ready_prefix_on_cmdline(proc)]
    # 返回子进程的数量
    return len(ready_workers)


def get_num_workers_running(gunicorn_master_proc):
    """获得gunicorn的子进程的数量 ."""
    workers = psutil.Process(gunicorn_master_proc.pid).children()
    return len(workers)


def kill_children_processes(pids_to_kill, timeout=5, log=None):
    """杀掉当前进程的所有子进程 ."""
    if pids_to_kill:
        # 获得主进程
        this_process = psutil.Process(os.getpid())
        # Only check child processes to ensure that we don't have a case
        # where we kill the wrong process because a child process died
        # but the PID got reused.
        # 获得存活的子进程
        child_processes = [x for x in this_process.children(recursive=True)
                           if x.is_running() and x.pid in pids_to_kill]
        # 终止多个DAG处理子进程
        # First try SIGTERM
        for child in child_processes:
            log.info("Terminating child PID: %s", child.pid)
            child.terminate()
        # TODO: Remove magic number
        # 等待多个DAG处理子进程结束
        log.info(
            "Waiting up to %s seconds for processes to exit...", timeout)
        try:
            psutil.wait_procs(
                child_processes, timeout=timeout,
                callback=lambda x: log.info('Terminated PID %s', x.pid))
        except psutil.TimeoutExpired:
            log.debug("Ran out of time while waiting for processes to exit")

        # 判断子进程是否结束
        # Then SIGKILL
        child_processes = [x for x in this_process.children(recursive=True)
                           if x.is_running() and x.pid in pids_to_kill]
        if child_processes:
            log.info("SIGKILL processes that did not terminate gracefully")
            for child in child_processes:
                log.info("Killing child PID: %s", child.pid)
                child.kill()
                child.wait()
