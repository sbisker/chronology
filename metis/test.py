#!/usr/bin/python

"""
Integration test for Metis.  Usage:

  python test.py --kronos-url http://localhost:8151 --metis-url http://localhost:8152|less
"""

from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzutc
from metis import app
from metis.convenience.cohort import cohort_queryplan
from metis.core.query.primitives import agg
from metis.core.query.primitives import aggop
from metis.core.query.primitives import c
from metis.core.query.primitives import f
from metis.core.query.primitives import filt_cond
from metis.core.query.primitives import filt
from metis.core.query.primitives import kstream
from metis.core.query.primitives import p
from metis.core.query.primitives import proj
from metis.core.query.primitives import Comparison
from metis.core.query.primitives import ID
from metis.core.query.primitives import TIME
from pykronos import KronosClient
from pykronos.utils.time import datetime_to_kronos_time
from random import randint
from random import random

import json
import requests

class EndpointTest(object):

  def __init__(self, client, kronos_url, metis_url):
    self._client = client
    self._kronos_url = kronos_url
    self._metis_url = metis_url

  def clear_data(self):
    raise NotImplementedError('Please implement clear_data')

  def generate_date(self):
    raise NotImplementedError('Please implement generate_data')

  def run_test(self):
    raise NotImplementedError('Please implement run_test')

  def description(self):
    raise NotImplementedError('Please implement description')

      
def datetime_to_date_str(dt):
  return str(dt.date())


class GetTest(EndpointTest):
  SECONDS = 1000
  STREAM = 'metis.test.stream'
  
  def clear_data(self):
    self._client.delete(GetTest.STREAM, 0, GetTest.SECONDS)

  def generate_data(self):
    group1_options = ['whale', 'seagull', 'crab', 'seal', 'squid']
    group2_options = ['angela', 'barack']
    group3_options = ['mandela', 'king', 'gandhi']
    value_options = [1.0 * value for value in xrange(10)]

    # Identify expected results by time and group3 in nested
    # dictionaries.
    self._expected = defaultdict(lambda: defaultdict(float))
    for idx in xrange(GetTest.SECONDS):
      blob = {
        '@time': idx,
        'group1': group1_options[idx % len(group1_options)],
        'group2': group2_options[idx % len(group2_options)],
        'group3': group3_options[idx % len(group3_options)],
        'value': value_options[idx % len(value_options)]
      }
      self._client.put({GetTest.STREAM: [blob]})
      if blob['group2'] == 'angela':
        time_group = blob['@time'] - (blob['@time'] % 100)
        self._expected[time_group][blob['group3']] += blob['value']
    self._client.flush()


  def run_test(self):
    plan = [kstream(GetTest.STREAM, 0, GetTest.SECONDS,
                    self._kronos_url, app.config['DEFAULT_READ_NAMESPACE']),
            filt(filt_cond(p('group2'), c('angela'), Comparison.EQUAL)),
            proj([p(ID), p(TIME), p('group1'), p('group2'),
                  p('group3'), p('value')]),
            agg([f('round_time_down', [p('@time'), c('100')], alias='@time'),
                 p('group3')],
                [aggop('sum', [p('value')], alias='sum_of_values')])]
    print plan
    response = requests.post('%s/1.0/query' % self._metis_url,
                             data=json.dumps({'plan': plan}),
                             stream=True)
    results = defaultdict(lambda: defaultdict(float))
    for line in response.iter_lines():
      blob = json.loads(line)
      assert set(blob.keys()) == {'@time', 'sum_of_values', 'group3'}
      results[blob['@time']][blob['group3']] += blob['value']
    assert (json.dumps(self._expected, sort_keys=True) ==
            json.dumps(results, sort_keys=True))
    

  def description(self):
    return 'basic /query'


class CohortTest(EndpointTest):
  EMAIL_STREAM = 'metis.test.emails'
  FRONTPAGE_STREAM = 'metis.test.frontpage'
  START_DATETIME = datetime(2011, 11, 18).replace(tzinfo=tzutc())
  ACTION_REPETITION_DAYS = 14
  EMAIL_WEEKS = [(0, 2), (1, 3), (2, 4), (3, 0), (4, 1)]


  def clear_data(self):
    start = CohortTest.START_DATETIME - timedelta(weeks=2)
    end = (CohortTest.START_DATETIME +
           timedelta(weeks=(2 + len(CohortTest.EMAIL_WEEKS))) +
           timedelta(days=CohortTest.ACTION_REPETITION_DAYS))
    client.delete(CohortTest.EMAIL_STREAM,
                  datetime_to_kronos_time(start),
                  datetime_to_kronos_time(end))
    client.delete(CohortTest.FRONTPAGE_STREAM,
                  datetime_to_kronos_time(start),
                  datetime_to_kronos_time(end))    

  def generate_data(self):
    user_ids = range(700)

    # Email stream: Users in groups 1-5 get an email in weeks 1 and 3,
    # 2 and 4, 3 and 5, 4 and 1, 5 and 2, respectively.
    user_dates = {}
    # Fill in expected_output, which is of
    # the form: {cohort_date: {cohort_size: NN,
    #                          action_dates: {action_date: num_actions}}}
    self._expected = defaultdict(
      lambda: {'cohort_size': 0, 'action_dates': defaultdict(int)})
    for user_id in user_ids:
      weeks1, weeks2 = CohortTest.EMAIL_WEEKS[
        user_id % len(CohortTest.EMAIL_WEEKS)]
      week1 = CohortTest.START_DATETIME + timedelta(weeks=weeks1)
      date1 = week1 + timedelta(hours=randint(0, 72))
      week2 = CohortTest.START_DATETIME + timedelta(weeks=weeks2)
      date2 = week2 + timedelta(hours=randint(0, 72))
      week1 = datetime_to_date_str(week1)
      week2 = datetime_to_date_str(week2)      
      user_dates[user_id] = ({'cohort': week1, 'precise': date1},
                             {'cohort': week2, 'precise': date2})
      self._expected[week1]['cohort_size'] += 1
      self._expected[week2]['cohort_size'] += 1
      client.put({CohortTest.EMAIL_STREAM: [{'user': user_id,
                                  '@time': datetime_to_kronos_time(date1)}]})
      client.put({CohortTest.EMAIL_STREAM: [{'user': user_id,
                                  '@time': datetime_to_kronos_time(date2)}]})      
    
    # Action stream: Users in group 1 hit the front page w/ 1/5
    # percent chance, group 2 with 2/5 chance, etc.  Likelihood for
    # all users on day N is 1/N.
    for user_id in user_ids:
      for email_dates in user_dates[user_id]:
        for day in xrange(CohortTest.ACTION_REPETITION_DAYS):
          group_probability = (
            ((user_id % len(CohortTest.EMAIL_WEEKS)) + 1.0) /
            len(CohortTest.EMAIL_WEEKS))
          day_probability = 1.0 / (day + 1)
          action_probability = group_probability * day_probability
          if random() < action_probability:
            action_date = email_dates['precise'] + timedelta(days=day)
            self._exptected[email_dates['cohort']]['action_dates'][
              datetime_to_date_str(action_date)] += 1
            client.put({CohortTest.FRONTPAGE_STREAM: [{'user_id': user_id,
                                            '@time': action_date}]})

    print json.dumps(self._expected, sort_keys=True, indent=2)

  
  def run_test(self):
    plan = cohort_queryplan({'cohort': {'stream': CohortTest.EMAIL_STREAM,
                                        'transform': [],
                                        'start':
                                          CohortTest.START_DATETIME.date(),
                                        'cohorts': len(CohortTest.EMAIL_WEEKS),
                                        'unit': 'weeks',
                                        'grouping_key': 'user'},
                             'action': {'stream': CohortTest.FRONTPAGE_STREAM,
                                        'transform': [],
                                        'repetitions': CohortTest.ACTION_REPETITION_DAYS,
                                        'unit': 'days',
                                        'grouping_key': 'user_id'}})
    response = requests.post('%s/1.0/query' % self._metis_url,
                             data=json.dumps(plan),
                             stream=True)  
    results = defaultdict(
      lambda: {'cohort_size': 0, 'action_dates': defaultdict(int)})
    for line in response.iter_lines():
      blob = json.loads(line)
      assert set(blob.keys()) == {'@time', 'cohort_size', 'cohort_actions', 'action-date'}      
      results[blob['@time']]['cohort_size'] = blob['cohort_size']
      results[blob['@time']]['action_dates'][blob['action_date']] = (
        blob['cohort_actions'])
    assert (json.dumps(self._expected, sort_keys=True) ==
            json.dumps(results, sort_keys=True))  

  def description(self):
    return 'cohort analysis'

  
if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos test runner.')
  parser.add_argument('--kronos-url', required=True)
  parser.add_argument('--metis-url', required=True)
  parser.add_argument('--test-stream', default='metis.test.raw')
  parser.add_argument('--test-seconds', type=int, default=1000)
  args = parser.parse_args()

  client = KronosClient(args.kronos_url, blocking=False)
  for test in [
    GetTest(client, args.kronos_url, args.metis_url),
#    CohortTest(client, args.kronos_url, args.metis_url)
    ]:
    print 'Testing %s' % (test.description())
    print '...generating data'
#    test.clear_data()
#    test.generate_data()
    print '...running test'
    test.run_test()
#    test.clear_data()  
