# encoding: utf-8

"""
基于fernet的加解密算法
"""

import os
import base64

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from xTool.misc import tob
from xTool.utils.log.logging_mixin import LoggingMixin
from xTool.exceptions import XToolException


def generate_fernet_key():
    """产生了一个44字节的随机数，并用base64编码，并解码为unicode编码 ."""
    key = Fernet.generate_key().decode()
    return key


def generate_fernet_pbkdf2hmac_key():
    """密钥用PBKDF2算法处理，参数设置如下，计算后可以保证密钥（特别是弱口令）的安全性 ."""
    salt = os.urandom(16)
    password = os.urandom(64)
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),
                     length=32,
                     salt=salt,
                     iterations=100000,
                     backend=default_backend())
    key = base64.urlsafe_b64encode(kdf.derive(password))
    return key


def encrypt(key, plaintext):
    """基于AES的加密操作 ."""
    f = Fernet(tob(key))
    return f.encrypt(tob(plaintext)).decode('utf-8')


def decrypt(key, ciphertext):
    """基于AES的解密操作 ."""
    f = Fernet(tob(key))
    return f.decrypt(tob(ciphertext)).decode('utf-8')


def double_encrypt(root_key, instance_key_cipher, plaintext):
    # 用根密钥（RootKey）对实例密钥加密串解密
    instance_key = decrypt(root_key, instance_key_cipher)
    # 使用实例密钥（InstanceKey）对口令（Password）加密
    cipher_text = encrypt(instance_key, plaintext)
    return cipher_text


def double_decrypt(root_key, instance_key_cipher, cipher_text):
    # 先用根秘钥解密实例秘钥加密串，获得实例秘钥
    instance_key = decrypt(root_key, instance_key_cipher)
    # 再用实例秘钥解密密文
    return decrypt(instance_key, cipher_text)


def parseDbConfig(dbConfig, root_key=None, instance_key_cipher=None):
    """用于将db配置转换成torndb可识别的参数格式 ."""
    host = dbConfig.get('host')
    user = dbConfig.get('user')
    password = dbConfig.get('password')
    if not password:
        password = dbConfig.get('passwd')
    root_key = root_key if root_key else dbConfig.get('root_key')
    instance_key_cipher = instance_key_cipher if instance_key_cipher else dbConfig.get('instance_key_cipher')
    if password and root_key and instance_key_cipher:
        password = double_decrypt(root_key,
                                  instance_key_cipher,
                                  password)
    charset = dbConfig.get('charset', 'utf8')
    database = dbConfig.get('database')
    if not database:
        database = dbConfig.get('db')
    return (host, database, user, password, 7 * 3600, 0, "+0:00", charset)


InvalidFernetToken = InvalidToken
_fernet = None


def get_fernet(fernet_key):
    """
    Deferred load of Fernet key.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: XToolException if there's a problem trying to load Fernet
    """
    global _fernet
    if _fernet:
        return _fernet

    if not fernet_key:
        log = LoggingMixin().log1
        log.warning(
            "empty cryptography key - values will not be stored encrypted."
        )
        raise XToolException("Could not create Fernet object")
    else:
        _fernet = Fernet(fernet_key.encode('utf-8'))
        _fernet.is_encrypted = True
        return _fernet
