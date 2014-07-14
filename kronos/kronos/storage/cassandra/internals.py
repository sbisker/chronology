import cassandra
import heapq
import json
import random

from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.query import BatchType
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
from collections import defaultdict
from datetime import timedelta
from timeuuid import TimeUUID
from uuid import UUID

from kronos.common.cache import InMemoryLRUCache
from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import MAX_LIMIT
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.core.exceptions import InvalidTimeUUIDComparison
from kronos.storage.cassandra.errors import CassandraStorageError
from kronos.storage.cassandra.errors import InvalidStreamComparison
from kronos.utils.math import round_down
from kronos.utils.uuid import uuid_to_kronos_time


# Since the Datastax driver doesn't let us pass kwargs to session.prepare
# we'll just go ahead and monkey patch it to work for us.
_prepare = Session.prepare
def patched_prepare(self, query, **kwargs):
  stmt = _prepare(self, query)
  for key, value in kwargs.iteritems():
    setattr(stmt, key, value)
  return stmt
Session.prepare = patched_prepare


class StreamEvent(object):
  def __init__(self, _id, event_json, stream_shard):
    self.stream_shard = stream_shard
    self.json = event_json
    self.id = TimeUUID(_id, descending=stream_shard.descending)
    multiplier = -1 if stream_shard.descending else 1
    self._cmp_value = multiplier * uuid_to_kronos_time(self.id)

  def __cmp__(self, other):
    if not other:
      return 1
    if isinstance(other, TimeUUID):
      return cmp(self.id, other)
    if isinstance(other, StreamEvent):
      return cmp(self.id, other.id)
    raise InvalidTimeUUIDComparison('Compared TimeUUID to type {0}'
                                    .format(type(other)))


class StreamShard(object):
  # CQL commands.
  SELECT_CQL = """SELECT id, blob FROM stream WHERE
    key = ? AND
    id >= ? AND
    id <= ?
    ORDER BY id %s
    LIMIT ?
    """
  
  def __init__(self, session, stream, start_time, width, shard, descending,
               limit, read_size):
    self.session = session
    self.descending = descending
    self.read_size = read_size
    self.limit = limit
    self.key = StreamShard.get_key(stream, start_time, shard)

    # If we want to sort in descending order, compare the end of the
    # interval.
    self._cmp_value = (-1 if descending else 1) * start_time
    if descending:
      self._cmp_value -= width

    self.select_cql = None

  @staticmethod
  def get_key(stream, start_time, shard):
    return '%s:%d:%d' % (stream, start_time, shard)

  def __cmp__(self, other):
    if other is None:
      return 1
    elif isinstance(other, StreamShard):
      return cmp((self._cmp_value, self.key), (other._cmp_value, other.key))
    try:
      return cmp(self._cmp_value, float(other))
    except ValueError:
      raise InvalidStreamComparison('Compared StreamShard to type {0}'
                                    .format(type(other)))

  def iterator(self, start_id, end_id):
    if self.descending:
      order = 'DESC'
    else:
      order = 'ASC'

    if not self.select_cql:
      self.select_cql = self.session.prepare(
        StreamShard.SELECT_CQL % order,
        _routing_key='key',
        fetch_size=self.read_size or 2000,
        consistency_level=ConsistencyLevel.ONE)
    events = self.session.execute(self.select_cql,
                                  (self.key, start_id, end_id, self.limit))
    for event in events:
      try:
        yield StreamEvent(str(event[0]), event[1], self)
      except GeneratorExit:
        break
      except ValueError: # Malformed blob?
        pass

    raise StopIteration


class Stream(object):
  # 6 months.
  MAX_WIDTH = int(timedelta(days=365.25).total_seconds() * 1e7) / 2

  # CQL statements.
  DELETE_CQL = """DELETE FROM stream WHERE
    key = ? AND
    id = ?"""
  INSERT_CQL = """INSERT INTO stream (key, id, blob)
    VALUES (?, ?, ?)"""
  INDEX_INSERT_CQL = """INSERT INTO idx (stream, start_time, width, shard)
    VALUES (?, ?, ?, ?)"""
  INDEX_SCAN_CQL = """SELECT start_time, width, shard FROM idx WHERE
    stream = ? AND
    start_time >= ? AND
    start_time < ?"""

  def __init__(self, session, stream, width, shards, read_size):
    self.session = session
    self.read_size = read_size
    self.stream = stream
    self.shards = shards
    self.width = width

    # Index cache is a write cache: it prevents us from writing to the
    # bucket index if we've already updated it in a previous
    # operation.
    self.index_cache = InMemoryLRUCache(max_items=1000)

    # CQL statements (lazily prepared).
    self.delete_cql = None
    self.insert_cql = None
    self.index_insert_cql = None
    self.index_scan_cql = None

  def get_overlapping_shards(self, start_time, end_time):
    if not self.index_scan_cql:
      self.index_scan_cql = self.session.prepare(Stream.INDEX_SCAN_CQL,
                                                 _routing_key='stream')

    potential_shards = self.session.execute(
      self.index_scan_cql,
      (self.stream, max(start_time - Stream.MAX_WIDTH, 0), end_time))
    shards = defaultdict(lambda: defaultdict(int))
    for (shard_time, width, shard) in potential_shards:
      if shard_time + width < start_time:
        # end_time < shard start_time?
        continue
      shards[shard_time][shard] = max(shards[shard_time][shard], width)
    for shard_time, _ in shards.iteritems():
      for shard, width in _.iteritems():
        yield {'start_time': shard_time,
               'width': width,
               'shard': shard}

  def insert(self, events):
    if not events:
      return
    batch_stmt = BatchStatement(batch_type=BatchType.UNLOGGED,
                                consistency_level=ConsistencyLevel.QUORUM)
    self.insert_to_batch(batch_stmt, events)
    self.session.execute(batch_stmt)

  def insert_to_batch(self, batch_stmt, events):
    if not (self.insert_cql or self.index_insert_cql):
      self.insert_cql = self.session.prepare(
        Stream.INSERT_CQL,
        _routing_key='key',
        consistency_level=ConsistencyLevel.QUORUM)
      self.index_insert_cql = self.session.prepare(
        Stream.INDEX_INSERT_CQL,
        _routing_key='stream',
        consistency_level=ConsistencyLevel.QUORUM)

    shard_idx = {}
    for event in events:
      shard_time = round_down(event[TIMESTAMP_FIELD], self.width)
      shard = shard_idx.get(shard_time,
                            random.randint(0, self.shards - 1))
      
      # Insert to index.
      try:
        self.index_cache.get((shard_time, shard))
      except KeyError:
        bound_stmt = self.index_insert_cql.bind(
          (self.stream, shard_time, self.width, shard))
        batch_stmt.add(bound_stmt)
        self.index_cache.set((shard_time, shard), None)

      # Insert to stream.
      batch_stmt.add(self.insert_cql,
                     (StreamShard.get_key(self.stream, shard_time, shard),
                      UUID(event[ID_FIELD]),
                      json.dumps(event)))
      shard_idx[shard_time] = (shard + 1) % self.shards # Round robin.

  def iterator(self, start_id, end_id, descending, limit):
    start_id.descending = descending
    end_id.descending = descending
    
    shards = self.get_overlapping_shards(uuid_to_kronos_time(start_id),
                                         uuid_to_kronos_time(end_id))
    shards = sorted(map(lambda shard: StreamShard(self.session,
                                                  self.stream,
                                                  shard['start_time'],
                                                  shard['width'],
                                                  shard['shard'],
                                                  descending,
                                                  limit,
                                                  self.read_size),
                        shards))
    iterators = {}
    event_heap = []

    def load_next_shard():
      """
      Pulls the earliest event from the next earliest shard and puts it into the
      event heap.
      """
      if not shards:
        return
      shard = shards.pop(0)
      it = shard.iterator(start_id, end_id)
      try:
        event = it.next()
        heapq.heappush(event_heap, event)
        iterators[shard] = it
      except StopIteration:
        pass

    def load_overlapping_shards():
      """
      Given what the current most recently loaded event is, loads any
      shards that might overlap with that event. Multiple shards
      might overlap because they have overlapping time slices.
      """
      while not event_heap and shards:
        # Try to pull events from unread shards.
        load_next_shard()

      if event_heap:
        # Pull events from all shards that overlap with the next event to be
        # yielded.
        top_event = event_heap[0]
        while shards:
          if top_event._cmp_value < shards[0]._cmp_value:
            break
          load_next_shard()
      elif not iterators:
        # No events in the heap and no active iterators? We're done!
        return
        
      shards_with_events = set(event.stream_shard for event in event_heap)
      for shard in iterators.keys():
        if shard in shards_with_events:
          continue
        try:
          it = iterators[shard]
          event = it.next()
          heapq.heappush(event_heap, event)
        except StopIteration:
          del iterators[shard]
    
    def _iterator(limit):
      load_overlapping_shards() # bootstrap.

      # No events?
      if not event_heap:
        raise StopIteration

      while event_heap or shards:
        if limit <= 0:
          raise StopIteration
        if event_heap:
          # Get the next event to return.
          event = heapq.heappop(event_heap)
          # Note: in descending conditions below, we flip `<` for
          # `>` and `>=` for `<=` UUID comparator logic is flipped.
          if ((not descending and event.id > end_id) or
              (descending and event.id > start_id)):
            raise StopIteration
          elif ((not descending and event.id >= start_id) or
                (descending and event.id >= end_id)):
            limit -= 1
            yield event

        load_overlapping_shards()

    for event in _iterator(limit):
      yield event
    
  def delete(self, start_id, end_id):
    batch_stmt = BatchStatement(batch_type=BatchType.UNLOGGED,
                                consistency_level=ConsistencyLevel.QUORUM)
    num_deleted = self.delete_to_batch(batch_stmt, start_id, end_id)
    self.session.execute(batch_stmt)
    return num_deleted
  
  def delete_to_batch(self, batch_stmt, start_id, end_id):
    if not self.delete_cql:
      self.delete_cql = self.session.prepare(Stream.DELETE_CQL,
                                             _routing_key='key')
    
    shards = self.get_overlapping_shards(uuid_to_kronos_time(start_id),
                                         uuid_to_kronos_time(end_id))
    num_deleted = 0
    for shard in shards:
      shard = StreamShard(self.session, self.stream,
                          shard['start_time'], shard['width'],
                          shard['shard'], False,
                          MAX_LIMIT, read_size=self.read_size)
      for event in shard.iterator(start_id, end_id):
        if event.id == start_id:
          continue
        num_deleted += 1
        batch_stmt.add(self.delete_cql, (shard.key, UUID(str(event.id))))
    return num_deleted


class Namespace(object): 
  # CQL commands.
  CREATE_NAMESPACE_CQL = """CREATE KEYSPACE %s WITH
    REPLICATION = {'class': 'SimpleStrategy',
                   'replication_factor': %d}"""
  DROP_NAMESPACE_CQL = """DROP KEYSPACE %s"""
  # key is of the form: "<stream_name>:<start_time>:<time_width>"
  STREAM_CQL = """CREATE TABLE stream (
    key text,
    id timeuuid,
    blob text,
    PRIMARY KEY (key, id)
  )"""
  INDEX_CQL = """CREATE TABLE idx (
    stream text,
    start_time bigint,
    width bigint,
    shard int,
    PRIMARY KEY (stream, start_time, width, shard)
  )"""
  STREAM_LIST_CQL = """SELECT DISTINCT stream FROM idx"""

  def __init__(self, cluster, name, replication_factor, read_size):
    self.cluster = cluster
    self.name = name
    self.replication_factor = replication_factor
    self.read_size = read_size
    self.session = None

    # Create session.
    self.create_session()

    # Cache for Stream instances.
    self.stream_cache = InMemoryLRUCache(max_items=1000)

  def get_stream(self, stream_name, width, shards):
    # width and shard settings change requires a restart of kronosd, so we can
    # just cache on stream name.
    try:
      return self.stream_cache.get(stream_name)
    except KeyError:
      stream = Stream(self.session, stream_name, width, shards, self.read_size)
      self.stream_cache.set(stream_name, stream)
      return stream

  def list_streams(self):
    for stream in self.session.execute(
      SimpleStatement(Namespace.STREAM_LIST_CQL,
                      consistency_level=ConsistencyLevel.QUORUM)):
      yield stream[0]

  def create_session(self):
    if self.session:
      raise CassandraStorageError

    try:
      session = self.cluster.connect(self.name)
    except cassandra.InvalidRequest, e:
      if "Keyspace '%s' does not exist" % self.name not in e.message:
        raise e
      session = self.cluster.connect()

      # Create keyspace for namespace.
      session.execute(Namespace.CREATE_NAMESPACE_CQL %
                      (self.name, self.replication_factor))
      session.set_keyspace(self.name)

      # Create column families + indices.
      session.execute(Namespace.STREAM_CQL)
      session.execute(Namespace.INDEX_CQL)

    self.session = session

  def destroy_session(self):
    self.session.shutdown()
    self.session = None

  def drop(self):
    self.session.execute(Namespace.DROP_NAMESPACE_CQL % self.name)
    # Session should automatically expire if keyspace dropped.
    self.destroy_session()
