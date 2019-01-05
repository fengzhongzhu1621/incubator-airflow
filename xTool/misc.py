#coding: utf-8


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