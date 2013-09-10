import threading
import time

from pyspark import SparkContext
from pyspark.rdd import RDD

from metis import app

def _patch_rdd():
  RDD.release = lambda rdd: _MANAGER.release_context(rdd.context)

_patch_rdd()


class SparkContextManager(object):
  def __init__(self, wait_seconds=15):
    if app.debug:
      self._max_contexts = 1
    else:
      self._max_contexts = app.config['NUM_WORKERS']
    self._cv = threading.Condition()
    self._contexts_created = 0
    self._queue = []    

  def _create_context(self):
    # Also ship the Metis zip file so worker nodes can deserialize Metis
    # internal objects.
    return SparkContext(app.config['SPARK_MASTER'],
                        'Metis-%s' % self._contexts_created,
                        pyFiles=[app.config['METIS_ZIP_FILE']])  

  def release_context(self, context):
    self._cv.acquire()
    self._queue.append((context, time.time()))
    self._cv.notify()
    self._cv.release()

  def get_context(self):
    self._cv.acquire()
    while True:
      if not (self._queue or self._contexts_created < self._max_contexts):
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
