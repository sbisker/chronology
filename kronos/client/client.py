import cjson
import requests
import time

from threading import Thread, Lock
from collections import defaultdict

class KronosClientException(Exception):
  pass

class KronosClient(object):
  """
  Initialize a Kronos client that can connect to a server at `http_url`

  Put requests are non-blocking if `blocking`=False
  """
  def __init__(self, http_url, blocking=True):
    self._put_url = '%s/1.0/events/put' % http_url
    self._get_url = '%s/1.0/events/get' % http_url
    self._index_url = '%s/1.0/index' % http_url
    self._blocking = blocking
    if not blocking:
      self._setup_nonblocking()
    self._setup_remote_keys()

  def index(self):
    return requests.get(self._index_url).json()

  """
  Sends a dictionary of `event_dict` of the form {stream_name: [event, ...], ...}
  to the server.

  The `blocking` parameter allows the request to block until the server responds,
  and returns some information on the response.  Here's an example:

  TODO(marcua): example
  """
  def put(self, event_dict):
    if self._blocking:
      return self._put(event_dict)
    else:
      self._put_lock.acquire()
      self._put_queue.append(event_dict)
      self._put_lock.release()

  def flush(self):
    if not self._blocking:
      self._put_lock.acquire()

      self._put_lock.release()
      
  
  def get(self, stream, start_time, end_time):
    stream_params = {
      'stream': stream,
      'start_time': start_time,
      'end_time': end_time
    }

    num_errors = 0
    last_id = None
    done = False
    while not done:
      try:
        response = requests.post(self._get_url,
                                 data=cjson.encode(stream_params),
                                 stream=True)
        if response.status_code != requests.codes.ok:
          raise KronosClientException('Bad server response code %d' % response.status_code)
        for line in response.iter_lines():
          if line:
            event = cjson.decode(line)
            last_id = event[self.id_key]
            yield event
        break
      except Exception, e:
        num_errors += 1
        if num_errors == 10:
          raise KronosClientException(e)
        if last_id != None:
          if 'start_time' in stream_params:
            del stream_params['start_time']
          stream_params['start_id'] = last_id
        time.sleep(num_errors * 0.1)

          
  def _setup_nonblocking(self):
    self._put_queue = []
    self._put_lock = Lock()

    me = self
    class PutThread(Thread):
      def run(self):
        while True:
          me.flush()
          time.sleep(.1)
    PutThread().start()

  def flush(self):
    if self._blocking:
      return
    self._put_lock.acquire()
    old_queue = None
    if self._put_queue:
      old_queue = self._put_queue
      self._put_queue = []
    self._put_lock.release()
    if old_queue:
      mega_events = defaultdict(list)
      for event_dict in old_queue:
        for stream_name, events in event_dict.iteritems():
          mega_events[stream_name].extend(events)
      self._put(mega_events)

  def _setup_remote_keys(self):
    index = self.index()
    self.id_key = index['fields']['id']
    self.time_key = index['fields']['timestamp']

  def _put(self, event_dict):
    response = requests.post(self._put_url, data=cjson.encode(event_dict))
    if response.status_code != requests.codes.ok:
      raise KronosClientException('Received response code %s with errors %s' %
                                  (response.status_code,
                                   response.json()['@errors']))
    response_dict = response.json()
    errors = response_dict.get('@errors')
    if errors:
      raise KronosClientException('Encountered errors %s' % errors)
    return response_dict

