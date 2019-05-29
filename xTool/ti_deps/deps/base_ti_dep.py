# -*- coding: utf-8 -*-


from collections import namedtuple

from xTool.decorators.db import provide_session


class BaseTIDep(object):
    """任务实例依赖抽象基类
    Abstract base class for dependencies that must be satisfied in order for task
    instances to run. For example, a task that can only run if a certain number of its
    upstream tasks succeed. This is an abstract class and must be subclassed to be used.
    """

    # If this dependency can be ignored by a context in which it is added to. Needed
    # because some dependencies should never be ignoreable in their contexts.
    IGNOREABLE = False

    # Whether this dependency is not a global task instance dependency but specific
    # to some tasks (e.g. depends_on_past is not specified by all tasks).
    IS_TASK_DEP = False

    def __init__(self):
        pass

    def __eq__(self, other):
        return type(self) == type(other)

    def __hash__(self):
        return hash(type(self))

    def __repr__(self):
        return "<TIDep({self.name})>".format(self=self)

    @property
    def name(self):
        """获得依赖名 ."""
        return getattr(self, 'NAME', self.__class__.__name__)

    def _get_dep_statuses(self, ti, session, dep_context=None):
        """返回一组依赖状态
        Abstract method that returns an iterable of TIDepStatus objects that describe
        whether the given task instance has this dependency met.

        For example a subclass could return an iterable of TIDepStatus objects, each one
        representing if each of the passed in task's upstream tasks succeeded or not.

        :param ti: the task instance to get the dependency status for
        :type ti: TaskInstance
        :param session: database session
        :type session: Session
        :param dep_context: the context for which this dependency should be evaluated for
        :type dep_context: DepContext
        """
        raise NotImplementedError

    @provide_session
    def get_dep_statuses(self, ti, session, dep_context=None):
        """
        Wrapper around the private _get_dep_statuses method that contains some global
        checks for all dependencies.

        :param ti: the task instance to get the dependency status for
        :type ti: TaskInstance
        :param session: database session
        :type session: Session
        :param dep_context: the context for which this dependency should be evaluated for
        :type dep_context: DepContext
        """
        # 忽略所有依赖的情况, IGNOREABLE开关必须打开
        if self.IGNOREABLE and dep_context.ignore_all_deps:
            yield self._passing_status(
                reason="Context specified all dependencies should be ignored.")
            return

        # 忽略所有任务依赖的情况，IS_TASK_DEP开关必须打开
        if self.IS_TASK_DEP and dep_context.ignore_task_deps:
            yield self._passing_status(
                reason="Context specified all task dependencies should be ignored.")
            return

        # 返回自定义任务依赖
        for dep_status in self._get_dep_statuses(ti, session, dep_context):
            yield dep_status

    @provide_session
    def is_met(self, ti, session, dep_context=None):
        """任务通过的条件是它的所有的依赖全部满足
        Returns whether or not this dependency is met for a given task instance. A
        dependency is considered met if all of the dependency statuses it reports are
        passing.

        :param ti: the task instance to see if this dependency is met for
        :type ti: TaskInstance
        :param session: database session
        :type session: Session
        :param dep_context: The context this dependency is being checked under that stores
            state that can be used by this dependency.
        :type dep_context: BaseDepContext
        """
        return all(status.passed for status in
                   self.get_dep_statuses(ti, session, dep_context))

    @provide_session
    def get_failure_reasons(self, ti, session, dep_context=None):
        """获得任务所有的依赖中，不满足的依赖的原因列表
        Returns an iterable of strings that explain why this dependency wasn't met.

        :param ti: the task instance to see if this dependency is met for
        :type ti: TaskInstance
        :param session: database session
        :type session: Session
        :param dep_context: The context this dependency is being checked under that stores
            state that can be used by this dependency.
        :type dep_context: BaseDepContext
        """
        for dep_status in self.get_dep_statuses(ti, session, dep_context):
            if not dep_status.passed:
                yield dep_status.reason

    def _failing_status(self, reason=''):
        """返回依赖不满足状态."""
        return TIDepStatus(self.name, False, reason)

    def _passing_status(self, reason=''):
        """返回依赖满足状态."""
        return TIDepStatus(self.name, True, reason)


# Dependency status for a specific task instance indicating whether or not the task
# instance passed the dependency.
TIDepStatus = namedtuple('TIDepStatus', ['dep_name', 'passed', 'reason'])
