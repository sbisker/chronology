import cjson
import copy
import gevent
import random
import time

from geventhttpclient import HTTPClient

from kronos.core.validators import ID_FIELD, TIMESTAMP_FIELD
from kronos.storage.backends import BaseStorage
from kronos.utils.cache import InMemoryLRUCache

# TODO(usmanm): Fetch node list of cluster periodically and update the total
# set of hosts we have?
class ElasticSearchClusterConnection(object):
  def __init__(self, hosts):
    self.active_nodes = {HTTPClient(server, port=port) for (server, port) in
                         (host.split(':') for host in hosts)}
    self.dead_nodes = set()
    self.retry_greenlet = gevent.spawn(self.retry_loop)

  def retry_loop(self):
    while 1:
      gevent.sleep(5*60)
      self.retry_dead_nodes()

  def retry_dead_nodes(self):
    if not self.dead_nodes:
      return
    # Can concurrently modify self.dead_nodes with retry greenlet, so copy it.
    dead_nodes = copy.copy(self.dead_nodes)
    alive_nodes = []
    for server in dead_nodes:
      if self.ping(server):
        alive_nodes.append(server)
    for server in alive_nodes:
      self.dead_nodes.discard(server)
      self.active_nodes.add(server)
    
  def ping(self, server):
    try:
      server.get('/')
    except:
      return False
    return True

  def request(self, *args, **kwargs):
    retry = 0
    while 1:
      if not self.active_nodes:
        self.retry_dead_nodes()
        retry += 1
        if retry > 3:
          raise Exception('All elasticsearch servers are unreachable!')
        continue
      # O(n). sets don't allow indexing so can't do random.choice.
      server = random.sample(self.active_nodes, 1)[0] 
      try:
        r = server.request(*args, **kwargs)
        r.body = r.read()
        try:
          r.json = cjson.decode(r.body)
        except:
          r.json = None
        return r
      except:
        # TODO(usmanm): Log exception.
        self.active_nodes.discard(server)
        self.dead_nodes.add(server)

class ElasticSearchStorage(BaseStorage):
  def __init__(self, name, **settings):
    super(ElasticSearchStorage, self).__init__(name, **settings)
    self.http = ElasticSearchClusterConnection(settings['hosts'])
    self.cas_index = settings['cas_index']
    self.event_index_template = settings['event_index_template']
    self.event_index_prefix = settings['event_index_prefix']
    self.rollover_size = int(settings['rollover_size'])
    self.rollover_check_period = int(settings['rollover_check_period_seconds'])
    self.read_size = int(settings['read_size'])
    self.alias_period = int(settings['alias_period'])
    self.alias_cache = InMemoryLRUCache() # 1000 entry LRU cache.
    self.event_index = None
    self.event_index_version = 0
    self.setup_elasticsearch()
    self.rollover_greenlet = gevent.spawn(self.rollover_index_if_needed)

  def setup_elasticsearch(self):
    # Load index template.
    template = open('%s/index.template' %
                    '/'.join(__file__.split('/')[:-1])).read()
    template = template.replace('{{ id_field }}', ID_FIELD)
    template = template.replace('{{ timestamp_field }}', TIMESTAMP_FIELD)
    template = template.replace('{{ event_index_prefix }}',
                                self.event_index_prefix)
    _template = cjson.decode(template)
    self.num_shards = (_template['settings'].get('index.number_of_shards') or
                       _template['settings'].get('index', {})
                       .get('index.number_of_shards') or 
                       1)

    # Always update template (in case it's missing, or it was updated).
    self.http.request('PUT', '/_template/%s' % self.event_index_template,
                      body=template)
    
    # Fetch current index.
    r = self.http.request('GET', '/%s/kronos/index' % self.cas_index)
    if r.json.get('exists'):
      self.event_index = r.json['_source']['name']
      self.event_index_version = r.json['_version']
    else:
      # No kronos index present? Create a new one.
      self.rollover_index()

  def rollover_index(self):
    name = '%s%s' % (self.event_index_prefix, int(time.time()))
    r = self.http.request('POST', '/%s/kronos/index?version=%s' %
                          (self.cas_index, self.event_index_version),
                          body=cjson.encode({'name': name}))
    if r.json.get('ok'):
      self.event_index = name
    else:
      # Someone else set a new version before us?
      r = self.http.request('GET', '/%s/kronos/index' % self.cas_index)
      self.event_index = r.json['_source']['name']
    self.event_index_version = r.json['_version']

  def rollover_index_if_needed(self):
    while True:
      try:
        # Check if we need to rollover every `self.rollover_check_period`
        # seconds.
        gevent.sleep(self.rollover_check_period) 
        r = self.http.request('GET', '/%s/_count' % self.event_index)
        if r.json.get('error') or r.json['count'] <= self.rollover_size:
          # Index not yet created or index not big enough to roll over?
          continue
        self.rollover_index()
        self.alias_cache.clear() # Clear alias cache for new index.
      except:
        # The rollover thread is immortal!
        # TODO(usmanm): Log exception here.
        pass

  def is_alive(self):
    try:
      self.http.request('GET', '/')
    except:
      return False
    return True

  def transform_event(self, event, insert=False):
    '''
    Recursively cleans keys in ``event`` by replacing `.` with its Unicode full
    width equivalent. ElasticSearch uses dot notation for naming nested fields
    and so having dots in field names can potentially lead to issues. (Note that
    Shay Bannon says field names with dots should be avoided even though
    they *will work* O.o)
    '''
    for key in event.keys():
      if insert:
        new_key = key.replace('.', u'\uFF0E')
      else:
        new_key = key.replace(u'\uFF0E', '.')
      if isinstance(event[key], dict):
        event[new_key] = self.clean_dots(event[key])
      else:
        event[new_key] = event[key]
      if new_key != key:
        del event[key]
    return event

  def get_alias(self, time):
    return (int(time) / self.alias_period) * self.alias_period
        
  def insert(self, stream, events, configuration):
    bulk = []
    aliases = set()
    for event in events:
      bulk.append('{"index": {"_id":"%s"}}' % event[ID_FIELD])
      bulk.append(cjson.encode(self.transform_event(event, insert=True)))
      alias = self.get_alias(event[TIMESTAMP_FIELD])
      try:
        self.alias_cache.get(alias)
      except KeyError:
        aliases.add('%s%s_alias' % (self.event_index_prefix, alias))
        self.alias_cache.set(alias, None)
    self.http.request('PUT', '/%s/%s/_bulk' % (self.event_index, stream),
                      body='%s\n' % '\n'.join(bulk))
    # Add missing aliases for the the current index.
    self.http.request('POST', '/_aliases', body=cjson.encode({
          'actions': [
            {'add': {'index': self.event_index, 'alias': alias}}
            for alias in aliases
            ]}))
  
  def _retrieve(self, stream, start_id, end_time, configuration):
    start_time = (start_id.time - 0x01b21dd213814000L) * 100 / 1e9
    query = {
      'query': {
        'filtered': {
          'filter': {
            'range': {
              TIMESTAMP_FIELD: {
                'from': start_time,
                'to': end_time,
                'include_lower': True,
                'include_upper': False
                }
              }
            },
          'query': {
            'match_all': {}
            }
          }
        },
      'sort': [
        {
          TIMESTAMP_FIELD: {
            'order': 'asc'
            }
         },
        ID_FIELD
        ],
      'size': self.read_size / self.num_shards
      }

    aliases_to_query = []
    alias = self.get_alias(start_time)
    while 1:
      aliases_to_query.append('%s%s_alias' % (self.event_index_prefix, alias))
      alias += self.alias_period
      if alias > end_time:
        break
      
    events_fetched_so_far = 0
    while 1:
      query['from'] = events_fetched_so_far
      # This may fail for ElasticSearch versions below 2.0 because they don't
      # support the `ignore_indices` parameter and so if any alias is missing,
      # an exception is returned.
      r = self.http.request('POST',
                            ('/%s/%s/_search?search_type=query_and_fetch&'
                             'ignore_indices=missing') %
                            (','.join(aliases_to_query), stream),
                            body=cjson.encode(query))
      hits = r.json.get('hits', {}).get('hits')
      if not hits:
        # No more events left?
        return
      for hit in hits:
        event = hit['_source']
        # Dedup?
        if (event[TIMESTAMP_FIELD], event[ID_FIELD]) <= (start_time, start_id):
          continue
        yield self.transform_event(event, insert=False)
        events_fetched_so_far += 1        
