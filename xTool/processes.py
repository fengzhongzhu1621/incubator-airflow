#coding: utf-8

import os
try:
    import pwd
    import grp
except ImportError:
    pass


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
