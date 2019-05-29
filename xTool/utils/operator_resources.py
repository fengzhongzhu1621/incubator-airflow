# -*- coding: utf-8 -*-

from xTool.exceptions import XToolException

# Constants for resources (megabytes are the base unit)
MB = 1
GB = 1024 * MB
TB = 1024 * GB
PB = 1024 * TB
EB = 1024 * PB


class Resource(object):
    """单个资源
    Represents a resource requirement in an execution environment for an operator.

    :param name: Name of the resource
    :type name: string
    :param units_str: The string representing the units of a resource (e.g. MB for a CPU
        resource) to be used for display purposes
    :type units_str: string
    :param qty: The number of units of the specified resource that are required for
        execution of the operator.
    :type qty: long
    """
    def __init__(self, name, units_str, qty):
        if qty < 0:
            raise XToolException(
                'Received resource quantity {} for resource {} but resource quantity '
                'must be non-negative.'.format(qty, name))
        # 资源名称
        self._name = name
        # 资源单位
        self._units_str = units_str
        # 资源数量
        self._qty = qty

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return str(self.__dict__)

    @property
    def name(self):
        return self._name

    @property
    def units_str(self):
        return self._units_str

    @property
    def qty(self):
        return self._qty


class CpuResource(Resource):
    def __init__(self, qty):
        super(CpuResource, self).__init__('CPU', 'core(s)', qty)


class RamResource(Resource):
    def __init__(self, qty):
        super(RamResource, self).__init__('RAM', 'MB', qty)


class DiskResource(Resource):
    def __init__(self, qty):
        super(DiskResource, self).__init__('Disk', 'MB', qty)


class GpuResource(Resource):
    def __init__(self, qty):
        super(GpuResource, self).__init__('GPU', 'gpu(s)', qty)


class Resources(object):
    """组合资源
    The resources required by an operator. Resources that are not specified will use the
    default values from the airflow config.

    :param cpus: The number of cpu cores that are required
    :type cpus: long
    :param ram: The amount of RAM required
    :type ram: long
    :param disk: The amount of disk space required
    :type disk: long
    :param gpus: The number of gpu units that are required
    :type gpus: long
    """
    def __init__(self, cpus, ram, disk, gpus):
        self.cpus = CpuResource(cpus)
        self.ram = RamResource(ram)
        self.disk = DiskResource(disk)
        self.gpus = GpuResource(gpus)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return str(self.__dict__)
