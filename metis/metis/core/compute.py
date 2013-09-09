from metis import app
from metis.core import transform
from metis.core import spark
from metis.lib.kronos.client import KronosClient
from metis.utils.decorators import async

_KRONOS = KronosClient(app.config['KRONOS_SERVER'], blocking=True)


def _get_kronos_rdd(stream, start_time, end_time):
  events = _KRONOS.get(stream, start_time, end_time)
  spark_context = spark.get_context()
  rdd = spark_context.parallelize(events)
  spark.release_context_on_collect(rdd)
  return rdd

def execute_compute_task(stream_in, start_time, end_time, transforms,
                         stream_out):
  x = len(spark._MANAGER._queue)
  rdd = _get_kronos_rdd(stream_in, start_time, end_time)
  for json_transform in transforms:
    metis_transform = transform.parse(json_transform)
    rdd = metis_transform.apply(rdd)
  result = rdd.collect()
  if stream_out is not None:
    min_time = min(event['time'] for event in result)
    max_time = max(event['time'] for event in result)
    # TODO(usmanm): Delete events in `stream_out` from `min_time` to `max_time`.
    _KRONOS.put({stream_out: result})
  result.append(len(spark._MANAGER._queue), x)
  return result

async_execute_compute_task = async(execute_compute_task)
