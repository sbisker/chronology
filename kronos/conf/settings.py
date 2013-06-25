import re

from kronos.utils import aws

# Backends.
storage = {
  'cassandra_timewidth': {
    'backend': 'cassandra.TimeWidthCassandraStorage',
    'hosts': ['127.0.0.1:9160'],
    'keyspace': 'kronos_tw_dev',
    'replication_factor': 1,
    'default_width': 86400,
    'default_shards': 3
    },
  'elasticsearch': {
    'backend': 'elasticsearch.ElasticSearchStorage',
    'hosts': ['localhost:9200'],
    'cas_index': 'kronos_cas',
    'event_index_template': 'kronos_template',
    'event_index_prefix': 'kronos_events_',
    'read_size': 1000,
    'alias_period': 2592000,
    'rollover_size': 5000000,
    'rollover_check_period_seconds': 300
    },
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'default_max_items': 100000
    },
  }

# Node related settings. `id` should be a unique name that identifies this
# Kronos instance, while `name` is simply a more human readable name.
node = {
  'id':  aws.get_instance_id(),
  'greenlet_pool_size': 25,
  'log_directory': 'log',
  'cors_whitelist_domains' : map(re.compile, [
    # Domains that match any regex in this list will be allowed to talk to this
    # Kronos instance
    ])
  }

# Stream settings. `fields` maps what keys the ID and timestamp should be
# assigned to in an event JSON.
stream = {
  'fields': {
    'id': '@id',
    'timestamp': '@time'
    }
  }

# TODO(usmanm): Add configuration for logging events for Kronos itself.

