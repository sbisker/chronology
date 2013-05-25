from decorators import templated
from flask import jsonify
from jia import app

@app.route('/')
@templated()
def index():
  return {}

@app.route('/kronos/get', methods=['POST'])
def kronos_get():
  import time
  retval = [
    {'_time': time.time(), 'data': {'money': 35.1}},
    {'_time': time.time()+24*3600, 'data': {'money': 35.3}},
    {'_time': time.time()+48*3600, 'data': {'money': 90}},
    {'_time': time.time()+72*3600, 'data': {'money': 90}},
    {'_time': time.time()+96*3600, 'data': {'money': 90}},           
  ]
  token = app.jinja_env.globals['csrf_token']()
  json_reply = jsonify(data=retval, csrf_token=token)
  return json_reply

@app.route('/charts/timeseries')
@templated()
def timeseries():
  return {}
