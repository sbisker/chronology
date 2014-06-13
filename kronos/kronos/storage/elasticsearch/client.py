import bisect

import cjson
import gevent
import time

from collections import defaultdict
from uuid import UUID
from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers

from kronos.conf.constants import ID_FIELD, TIMESTAMP_FIELD
from kronos.conf.constants import ResultOrder
from kronos.storage.base import BaseStorage
from kronos.utils.math import uuid_from_kronos_time, uuid_to_kronos_time
from kronos.utils.math import UUIDType

from kronos.common.cache import InMemoryLRUCache

DOT = u'\uFF0E'

class Event(dict):
  """
  An event is stored in memory as a dictionary.
  We define a comparator because events are sortable by the time in their
  UUIDs
  """
  def __cmp__(self, other):
    self_uuid = UUID(self[ID_FIELD])
    other_uuid = UUID(other[ID_FIELD])

    # If my time is != other's time, return that comparison
    if self_uuid.time < other_uuid.time:
      return -1
    elif self_uuid.time > other_uuid.time:
      return 1

    # If our times are equal, compare our raw bytes
    if self_uuid.bytes < other_uuid.bytes:
      return -1
    elif self_uuid.bytes > other_uuid.bytes:
      return 1

    return 0

class ElasticSearchStorage(BaseStorage):
  """
  The in memory storage backend maintains a sorted list of events per stream
  name.
  The size of this list is capped at `max_items` per stream.
  """
  valid_str = lambda x: len(str(x)) > 0
  pos_int = lambda x: int(x) > 0
  SETTINGS_VALIDATORS = {
    'default_max_items': pos_int,
    'hosts': lambda x: len(x) > 0,
    'keyspace_prefix': valid_str,
    'cas_index': valid_str,
    'event_index_template': valid_str,
    'event_index_prefix': valid_str,
    'rollover_size': pos_int,
    'rollover_check_period_seconds': pos_int,
    'read_size': pos_int,
    'alias_period': pos_int,
    'default_max_items': pos_int,
    'force_refresh': lambda x: isinstance(x, bool),
  }
  
  def __init__(self, name, **settings):
    super(ElasticSearchStorage, self).__init__(name, **settings)
    self.db = defaultdict(lambda: defaultdict(list))
    self.force_refresh = settings['force_refresh']
  
    self.setup_elasticsearch()

  def setup_elasticsearch(self):
    self.es = Elasticsearch(hosts=self.hosts)
 
    # Load index template.
    template = open('%s/index.template' %
                    '/'.join(__file__.split('/')[:-1])).read()
    template = template.replace('{{ id_field }}', ID_FIELD)
    template = template.replace('{{ timestamp_field }}', TIMESTAMP_FIELD)
    template = template.replace('{{ event_index_prefix }}',
                                self.event_index_prefix)

    # Always update template (in case it's missing, or it was updated).
    self.es.indices.put_template(name=self.event_index_template, body=template)

  def is_alive(self):
    return self.es.ping()

  def transform_event(self, event, insert=False):
    '''
    Recursively cleans keys in ``event`` by replacing `.` with its Unicode full
    width equivalent. ElasticSearch uses dot notation for naming nested fields
    and so having dots in field names can potentially lead to issues. (Note that
    Shay Bannon says field names with dots should be avoided even though
    they *will work* :/)
    '''
    for key in event.keys():
      if insert:
        new_key = key.replace('.', DOT)
      else:
        new_key = key.replace(DOT, '.')
      if isinstance(event[key], dict):
        event[new_key] = self.transform_events(event[key])
      else:
        event[new_key] = event[key]
      if new_key != key:
        del event[key]
    return event

  def _insert(self, namespace, stream, events, configuration):
    """
    `namespace` acts as db for different streams
    `stream` is the name of a stream and `events` is a list of events to
    insert.
    """
    #for testing
    self._mem_insert(namespace, stream, events, configuration)

    for event in events:
      event = self.transform_event(event, insert=True)
      event['_index'] = namespace
      event['_type'] = stream
      event['_id'] = event[ID_FIELD]
    
    #print 'insert', events
    es_helpers.bulk(self.es, events, refresh=self.force_refresh)


  def _mem_insert(self, namespace, stream, events, configuration):
    max_items = configuration.get('max_items', self.default_max_items)  
    for event in events:
      while len(self.db[namespace][stream]) >= max_items:
        self.db[namespace][stream].pop(0)
      bisect.insort(self.db[namespace][stream], Event(event))
    
  def _delete(self, namespace, stream, start_id, end_time, configuration):
    self._mem_delete(namespace, stream, start_id,
        end_time, configuration)

  def _mem_delete(self, namespace, stream, start_id, end_time, configuration):
    """
    Delete events with id > `start_id` and end_time <= `end_time`.
    """
    start_id = str(start_id)
    start_id_event = Event({ID_FIELD: start_id})
    end_id_event = Event({ID_FIELD:
                          str(uuid_from_kronos_time(end_time,
                                                    _type=UUIDType.HIGHEST))})
    stream_events = self.db[namespace][stream]

    # Find the interval our events belong to.
    lo = bisect.bisect_left(stream_events, start_id_event)
    if lo + 1 > len(stream_events):
      return 0
    if stream_events[lo][ID_FIELD] == start_id:
      lo += 1
    hi = bisect.bisect_right(stream_events, end_id_event)

    del stream_events[lo:hi]
    return max(0, hi - lo)

  def _retrieve(self, namespace, stream, start_id, 
      end_time, order, limit, configuration):
      
    items =  self._mem_retrieve(namespace, stream, start_id, end_time, 
          order, limit, configuration) 
   
    start_time = uuid_to_kronos_time(start_id)
    body_query = {
          'query': {
            'filtered': {
              'filter': {
                'range': {
                  TIMESTAMP_FIELD: {
                    'gte': start_time,
                    'lte': end_time,
                }
              }
            }  
          }
        }
      }
    sort_query=["%s:%s" % (TIMESTAMP_FIELD, ResultOrder.get_short_name(order)), ID_FIELD] 
    res = self.es.search(index=namespace,
                   doc_type=stream,
                  size=limit,
                  body=body_query,
                  ignore=404,
                  sort=sort_query,
                  ignore_indices=True)
    #TODO pagination
    hits = res.get('hits', {}).get('hits')
   # print 'retrieve',  hits
    if hits is None or not len(hits):
      return
    if hits[0]['_source'][ID_FIELD] == str(start_id):
      hits = hits[1:]

    for hit in hits:
      event = hit['_source']
      yield self.transform_event(event)
  
  def _mem_retrieve(self, namespace, stream, start_id, 
      end_time, order, limit, configuration):
    """
    Yield events from stream starting after the event with id `start_id` until
    and including events with timestamp `end_time`.
    """
    start_id = str(start_id)
    start_id_event = Event({ID_FIELD: start_id})
    end_id_event = Event({ID_FIELD:
                          str(uuid_from_kronos_time(end_time,
                                                    _type=UUIDType.HIGHEST))})
    stream_events = self.db[namespace][stream]

    # Find the interval our events belong to.
    lo = bisect.bisect_left(stream_events, start_id_event)
    if lo + 1 > len(stream_events):
      return
    if stream_events[lo][ID_FIELD] == start_id:
      lo += 1
    hi = bisect.bisect_right(stream_events, end_id_event)
    
    if order == ResultOrder.DESCENDING:
      index_it = xrange(hi-1, lo-1, -1)
    else:
      index_it = xrange(lo, hi)

    for i in index_it:
      if limit <= 0:
        break
      limit -= 1
      yield stream_events[i]

  def _streams(self, namespace):
    return self.db[namespace].iterkeys()

  def _clear(self):
    self.es.indices.delete(index='_all')
