import threading
import time

from pyspark import SparkContext

from metis import app

class SparkContextManager(object):
  def __init__(self, wait_seconds=15):
    if app.debug:
      self._max_contexts = 1
    else:
      self._max_contexts = app.config['NUM_WORKERS']
    self._cv = threading.Condition()
    self._contexts_created = 0
    self._queue = []

  def release_context(self, context):
    self._cv.acquire()
    self._queue.append((context, time.time()))
    self._cv.notify()
    self._cv.release()

  def _create_context(self):
    # Also ship the Metis zip file so worker nodes can deserialize Metis
    # internal objects.
    return SparkContext(app.config['SPARK_MASTER'],
                        'Metis-%s' % self._contexts_created,
                        pyFiles=[app.config['METIS_ZIP_FILE']])

  def get_context(self):
    self._cv.acquire()
    while True:
      if not (self._queue and self._contexts_created < self._max_contexts):
        self._cv.wait()
      if self._queue:
        context, last_used_time = self._queue.pop()
      elif self._contexts_created < self._max_contexts:
        context = self._create_context()
        self._contexts_created += 1
      self._cv.release()
      return context

_MANAGER = SparkContextManager()
get_context = _MANAGER.get_context


def release_context_on_collect(rdd):
  def collect(self):
    data = rdd._collect()
    _MANAGER.release_context(rdd.context)
    return data
  rdd._collect = rdd.collect
  rdd.collect = collect
  return rdd
