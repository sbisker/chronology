from metis import app
from metis.conf import constants
from metis.core import transform
from metis.core import spark
from metis.lib.kronos.client import KronosClient
from metis.utils.decorators import async

_KRONOS = KronosClient(app.config['KRONOS_SERVER'], blocking=True)


def _get_kronos_rdd(stream, start_time, end_time):
  def get_events():
    events = _KRONOS.get(stream, start_time, end_time)
    for event in events:
      # Strip the ID field from each event.
      del event[constants.ID_FIELD]
      yield event
  spark_context = spark.get_context()
  rdd = spark_context.parallelize(get_events())
  return rdd


def execute_compute_task(stream_in, start_time, end_time, transforms,
                         stream_out):
  rdd = _get_kronos_rdd(stream_in, start_time, end_time)
  metis_transforms = [transform.parse(json_transform)
                      for json_transform in transforms]
  transform.validate(metis_transforms)
  for metis_transform in metis_transforms:
    rdd = metis_transform.apply(rdd)
  result = rdd.collect()
  rdd.release()
  if stream_out is not None:
    min_time = min(event['time'] for event in result)
    max_time = max(event['time'] for event in result)
    # TODO(usmanm): Delete events in `stream_out` from `min_time` to `max_time`.
    _KRONOS.put({stream_out: result})
  return result

async_execute_compute_task = async(execute_compute_task)
