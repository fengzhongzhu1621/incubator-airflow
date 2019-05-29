# -*- coding: utf-8 -*-

import distributed
import subprocess
import warnings

from xTool.executors.base_executor import BaseExecutor
from xTool.misc import USE_WINDOWS


class DaskExecutor(BaseExecutor):
    """
    DaskExecutor submits tasks to a Dask Distributed cluster.
    """
    def __init__(self,
                 parallelism,
                 cluster_address,
                 tls_ca,
                 tls_key,
                 tls_cert):
        if not cluster_address:
            raise ValueError(
                'Please provide a Dask cluster address in airflow.cfg')
        self.cluster_address = cluster_address
        # ssl / tls parameters
        self.tls_ca = tls_ca
        self.tls_key = tls_key
        self.tls_cert = tls_cert
        super(DaskExecutor, self).__init__(parallelism=0)

    def start(self):
        if (self.tls_ca) or (self.tls_key) or (self.tls_cert):
            from distributed.security import Security
            security = Security(
                tls_client_key=self.tls_key,
                tls_client_cert=self.tls_cert,
                tls_ca_file=self.tls_ca,
                require_encryption=True,
            )
        else:
            security = None

        # 连接远程dask集群
        self.client = distributed.Client(self.cluster_address, security=security)
        self.futures = {}

    def execute_async(self, key, command, queue=None, executor_config=None):
        if queue is not None:
            warnings.warn(
                'DaskExecutor does not support queues. '
                'All tasks will be run in the same cluster'
            )

        def airflow_run():
            return subprocess.check_call(command, shell=True, close_fds=False if USE_WINDOWS else True)

        # 将命令发送到dask集群中执行
        future = self.client.submit(airflow_run, pure=False)
        self.futures[future] = key

    def _process_future(self, future):
        if future.done():
            key = self.futures[future]
            if future.exception():
                self.log.error("Failed to execute task: %s", repr(future.exception()))
                self.fail(key)
            elif future.cancelled():
                self.log.error("Failed to execute task")
                self.fail(key)
            else:
                self.success(key)
            self.futures.pop(future)

    def sync(self):
        # make a copy so futures can be popped during iteration
        for future in self.futures.copy():
            self._process_future(future)

    def end(self):
        # 按执行完成的顺序返回futures
        for future in distributed.as_completed(self.futures.copy()):
            self._process_future(future)

    def terminate(self):
        self.client.cancel(self.futures.keys())
        self.end()
