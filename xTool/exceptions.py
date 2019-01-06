# -*- coding: utf-8 -*-


class XToolException(Exception):
    """
    Base class for all Airflow's errors.
    Each custom exception should be derived from this class
    """
    status_code = 500


class XToolBadRequest(XToolException):
    """Raise when the application or server cannot handle the request"""
    status_code = 400


class XToolNotFoundException(XToolException):
    """Raise when the requested object/resource is not available in the system"""
    status_code = 404


class XToolConfigException(XToolException):
    pass


class XToolSensorTimeout(XToolException):
    pass


class XToolTaskTimeout(XToolException):
    pass


class XToolWebServerTimeout(XToolException):
    pass


class XToolSkipException(XToolException):
    pass


class XToolDagCycleException(XToolException):
    pass


class XToolTimeoutError(AssertionError):

    """Thrown when a timeout occurs in the `timeout` context manager."""

    def __init__(self, value="Timed Out"):
        self.value = value

    def __str__(self):
        return repr(self.value)
