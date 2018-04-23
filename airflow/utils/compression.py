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

from tempfile import NamedTemporaryFile
import shutil
import gzip
import bz2


def uncompress_file(input_file_name, file_extension, dest_dir):
    """解压.gz/.bz2压缩文件到指定目录

    Uncompress gz and bz2 files
    :param input_file_name: 压缩文件名
    :param file_extension: 支持的压缩文件的后缀 .gz / .bz2
    :parma dest_dir: 指定解压的目录
    """
    if file_extension.lower() not in ('.gz', '.bz2'):
        raise NotImplementedError("Received {} format. Only gz and bz2 "
                                  "files can currently be uncompressed."
                                  .format(file_extension))
    if file_extension.lower() == '.gz':
        fmodule = gzip.GzipFile
    elif file_extension.lower() == '.bz2':
        fmodule = bz2.BZ2File

    # 通过delete=False来保证解压后不会删除临时文件
    with fmodule(input_file_name, mode='rb') as f_compressed,\
        NamedTemporaryFile(dir=dest_dir,
                           mode='wb',
                           delete=False) as f_uncompressed:
        shutil.copyfileobj(f_compressed, f_uncompressed)
    return f_uncompressed.name
