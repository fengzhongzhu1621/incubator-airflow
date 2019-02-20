#coding: utf-8

import platform

# Determine platform being used.
system = platform.system()
USE_MAC = USE_WINDOWS = USE_X11 = False
if system == 'Darwin':
    USE_MAC = True
elif system == 'Windows' or system == 'Microsoft':
    USE_WINDOWS = True
else:  # Default to X11
    USE_X11 = True


def tob(s, enc='utf-8'):
    """将字符串转换为utf8/bytes编码 ."""
    return s.encode(enc) if not isinstance(s, bytes) else s


def tou(s, enc='utf-8'):
    """将字符串转换为unicode编码 ."""
    return s.decode(enc) if isinstance(s, bytes) else s


def get_local_host_ip(ifname=b'eth1'):
    """获得本机的IP地址 ."""
    import platform
    import socket
    if platform.system() == 'Linux':
        import fcntl
        import struct
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        o_ip = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,
            struct.pack('256s', ifname[:15])
        )[20:24])
    else:
        o_ip = socket.gethostbyname(socket.gethostname())
    return o_ip
