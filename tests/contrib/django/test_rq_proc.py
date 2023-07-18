from redis import Redis
from rq.queue import Queue
from rq.registry import StartedJobRegistry

from datetime import datetime, timedelta

from hirefire.procs import load_procs, loaded_procs


class TestRQProc:

	# TODO: add proper proc unloading as part of test startup and teardown.

	def test_can_count_queues_properly(self):
		try:
			self._cleanup()
			# Put some jobs on the queue
			self._add_jobs_to_queue('high', 2)
			self._add_jobs_to_queue('bottom', 4)

			# Schedule some jobs to run now
			self._add_jobs_to_queue('high', 2, scheduled_at=datetime.utcnow() - timedelta(minutes=1))
			self._add_jobs_to_queue('bottom', 4, scheduled_at=datetime.utcnow() - timedelta(minutes=1))

			# Schedule some jobs to run in the future
			self._add_jobs_to_queue('high', 2, scheduled_at=datetime.utcnow() + timedelta(minutes=1))
			self._add_jobs_to_queue('bottom', 4, scheduled_at=datetime.utcnow() + timedelta(minutes=1))

			# Now fake a job being active for one of them
			for idx, queue_name in enumerate(['high', 'bottom']):
				queue = Queue(queue_name, connection=Redis())
				registry = StartedJobRegistry(queue_name, queue.connection)
				# Passing in a negative score is important here, otherwise the job will be recognized as expired
				registry.connection.zadd(registry.key, {'job_id_{}'.format(idx): -1})

			# Load the HF procs
			procs = load_procs(*(
				'tests.contrib.django.testapp.rq_test_procs.WorkerProc',
				'tests.contrib.django.testapp.rq_test_procs.AnotherWorkerProc'
			))

            # Total should be all queued (6) + 1 active for each (2) + 1 scheduled for each (6) = 14
            # Ignore jobs scheduled to run in the future (6)
			assert sum([proc.quantity() for proc_name, proc in procs.items()]) == 14
		finally:
			self._cleanup()

	def _add_jobs_to_queue(self, queue_name, num, scheduled_at=None):
		queue = Queue(queue_name, connection=Redis())
		for _ in range(num):
			if scheduled_at:
				queue.enqueue_at(scheduled_at, self._dummy_func)
			else:
				queue.enqueue(self._dummy_func)

	@classmethod
	def _dummy_func(cls):
		pass

	def _cleanup(self):
		loaded_procs.clear()
		for idx, queue_name in enumerate(['high', 'bottom']):
			queue = Queue(queue_name, connection=Redis())
			queue.connection.delete(queue.scheduled_job_registry.key)
			queue.empty()
