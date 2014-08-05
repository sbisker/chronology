import datetime
import json
from jia import app
from metis.core.query.primitives import c, p, f, proj, filt, cond, agg, agg_op
from metis.core.query.primitives import order_by
from metis.core.query.primitives import limit as lim
from metis.core.query.utils import _date_trunc, _date_part
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import kronos_time_to_datetime

def cpf(args):
  if args['cpf_type'] == 'constant':
    return c(args['constant_value'])
  elif args['cpf_type'] == 'property':
    return p(args['property_name'])
  elif args['cpf_type'] == 'function':
    for i in range(len(args['function_args'])):
      args['function_args'][i] = cpf(args['function_args'][i])
    return f(args['function_name'], args['function_args'])
  else:
    raise ValueError("cpf_type must be constant, property, or function")

def transform(query_plan, args):
  fields = {} 
  fields[args[0]] = cpf(args[1])
  print "fff", fields
  return proj(query_plan, fields, merge=True)

def filter(query_plan, args):
  print args[1]
  condition = cond(cpf(args[0]), cpf(args[2]), args[1])
  return filt(query_plan, condition)

def aggregate(query_plan, args):
  aggregates = []
  arg = args[0]

  if arg == 'count':
    aggr = agg_op(arg)
  else:
    aggr = agg_op(arg, []) #TODO(derek): finish this line
  
  group = cpf(args[1])
  
  return agg(query_plan, {'@time':group}, [aggr])

def orderby(query_plan, args):
  return order_by(query_plan, [cpf(args[0])])

def limit(query_plan, args):
  return lim(query_plan, int(args[0]))

def translate_query(query, stream, start_time, end_time):
  namespace = app.config['KRONOS_NAMESPACE']
  host = app.config['KRONOS_URL']

  query_plan = {
    'operator': 'kronos',
    'stream': stream,
    'start_time': start_time,
    'end_time': end_time,
    'namespace': namespace,
    'host': host,
  }

  operators = {
    'transform': transform,
    'filter': filter, #TODO(derek): fix this
    'aggregate': aggregate,
    'orderby': orderby,
    'limit': limit,
  }

  for instruction in query:
    operator = instruction['operator']
    args = instruction['args']
    query_plan = operators[operator](query_plan, args)

  return json.dumps({'plan': query_plan})


# q = translate_query([
#   {
#     'operator': 'aggregate',
#     'args': [
#       {
#         'type': 'count',
#       },
#       {
#         'cpf_type': 'function',
#         'function_name': 'datetrunc',
#         'function_args': [
#           {
#             'cpf_type': 'property',
#             'property_name': '@time'
#           },
#           {
#             'cpf_type': 'constant',
#             'constant_value': 'hour',
#           }
#         ]
#       }
#     ]
#     # 'args': [
#     #   {
#     #     'name': '@time',
#     #     'value': {'cpf_type': 'property', 'name': '@time'}
#     #   },
#     #   {
#     #     'name': 'E-Mail',
#     #     'value': {'cpf_type': 'property', 'name': 'email'}
#     #   }
#     # ]
#   }
# ])

# q = translate_query([{"name":"Filter","operator":"filter","args":[{"cpf_type":"property","function_name":"ceiling","function_args":[],"property_name":"@time"},"gt",{"cpf_type":"function","function_name":"datetrunc","function_args":[{"cpf_type":"property","property_name":"1000000"},{"cpf_type":"constant","constant_value":"days"}]}]}])

# r = requests.post("http://localhost:8155/1.0/query", data=q)
# print r.text

# import pprint
# pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(json.loads(q))