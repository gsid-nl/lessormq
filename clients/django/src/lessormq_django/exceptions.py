"""What the worker reports when it cannot run a message, and what a task raises to say
the input itself is unusable."""

from __future__ import annotations

from lessormq import PermanentFailure

__all__ = ["LeaseExpired", "MalformedTask", "PermanentFailure", "UnknownTask"]


class MalformedTask(PermanentFailure):
    """The message is not a Django task: no task path, or arguments of the wrong shape.

    Reported as permanent, since running it again cannot help.
    """


class UnknownTask(Exception):
    """The task in the message cannot be imported by this worker.

    Reported as an ordinary failure, not a permanent one: during a deploy a worker with
    the old code can claim a task that only the new code has, and the next attempt may
    land on a worker that knows it.
    """


class LeaseExpired(Exception):
    """Stands for an attempt that ended because its lease ran out (the worker crashed or
    hung), in the errors of a result from ``get_result``. Never raised."""
