import cjson
import copy
import gevent
import random

from geventhttpclient import HTTPClient

class ElasticSearchConnectionException(Exception):
  pass

# TODO(usmanm): Fetch node list of cluster periodically and update the total
# set of hosts we have?
class ElasticSearchConnection(object):
  def __init__(self, hosts, retry_after=300):
    self._alive_nodes = {HTTPClient(server, port=port) for (server, port) in
                         (host.split(':') for host in hosts)}
    self._dead_nodes = set()
    self._retry_after = retry_after
    self._retry_greenlet = gevent.spawn(self._retry_loop, self._retry_after)

  def _retry_loop(self):
    while True:
      gevent.sleep(self._retry_after)
      self.retry_dead_nodes()

  def _retry_dead_nodes(self):
    if not self._dead_nodes:
      return
    # Can concurrently modify self.dead_nodes with retry greenlet, so copy it.
    dead_nodes = copy.copy(self.dead_nodes)
    alive_nodes = []
    for server in dead_nodes:
      if self.ping(server):
        alive_nodes.append(server)
    for server in alive_nodes:
      self._dead_nodes.discard(server)
      self._alive_nodes.add(server)
    
  def ping(self, server):
    try:
      server.get('/')
    except:
      return False
    return True

  def request(self, *args, **kwargs):
    retry = 0
    while True:
      if not self._alive_nodes:
        self._retry_dead_nodes()
        retry += 1
        if retry > 3:
          raise ElasticSearchConnectionException(
              'All elasticsearch servers are unreachable!')
        continue
      # O(n). sets don't allow indexing so can't do random.choice.
      server = random.sample(self._alive_nodes, 1)[0] 
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
        self._alive_nodes.discard(server)
        self._dead_nodes.add(server)
