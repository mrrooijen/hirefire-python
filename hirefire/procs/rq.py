from __future__ import absolute_import

from rq import Queue, Worker
from rq.registry import StartedJobRegistry, ScheduledJobRegistry
from rq.exceptions import NoSuchJobError

from . import ClientProc


class RQProc(ClientProc):
    """
    A proc class for the `RQ <http://python-rq.org/>`_ library.

    :param name: the name of the proc (required)
    :param queues: list of queue names to check (required)
    :param connection: the connection to use for the queues (optional)
    :type name: str
    :type queues: str or list
    :type connection: :class:`redis.Redis`

    Example::

        from hirefire.procs.rq import RQProc

        class WorkerRQProc(RQProc):
            name = 'worker'
            queues = ['high', 'default', 'low']

    """
    #: The name of the proc (required).
    name = None

    #: The list of queues to check (required).
    queues = ['default']

    #: The connection to use for the queues (optional).
    connection = None

    def __init__(self, connection=None, *args, **kwargs):
        super(RQProc, self).__init__(*args, **kwargs)
        if connection is not None:
            self.connection = connection

    def client(self, queue):
        """
        Given one of the configured queues returns a
        :class:`rq.Queue` instance using the
        :attr:`~hirefire.procs.rq.RQProc.connection`.
        """
        if isinstance(queue, Queue):
            return queue
        return Queue(queue, connection=self.connection)

    def quantity(self, **kwargs):
        """
        Returns the aggregated number of tasks of the proc queues.
        """
        count = 0

        # Total count should be what's queued plus the started jobs plus jobs scheduled to run now.
        for queue in self.clients:
            started_job_registry = StartedJobRegistry(queue.name, queue.connection)
            scheduled_job_registry = ScheduledJobRegistry(queue.name, queue.connection)
            count += (queue.count + len(started_job_registry) + len(scheduled_job_registry.get_jobs_to_schedule()))

        return count
