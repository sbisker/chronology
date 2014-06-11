storage = {
    'elasticsearch' : {
      'backend' : 'elasticsearch.ElasticSearchStorage',
      'hosts' : ['localhost'],
      'keyspace_prefix' : 'kronos_test',
      'cas_index' : 'TODO',
      'event_index_prefix' : 'TODO',
      'rollover_size' : 10000, #?
      'rollover_check_period_seconds' : 10, #?
      'read_size' : 5000, #?
      'alias_period' : 5, #?
      'default_max_items' : 1000, #rm
      }
}

default_namespace = 'kronos'

_default_stream_configuration = {
      '': {
            'backends': {
                    'memory': None
                          },
                'read_backend': 'memory'
                    }
        }

namespace_to_streams_configuration = {
  default_namespace: _default_stream_configuration,
  'namespace1': _default_stream_configuration,
  'namespace2': _default_stream_configuration
}
