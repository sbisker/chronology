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
      }
}

