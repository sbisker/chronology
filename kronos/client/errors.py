import settings

def global_errors(series_name, req, resp):
  error = False
  if series_name not in settings.SERIES:
    resp['errors'].append("Series configuration must be set in settings.py (edit SERIES)")
    error = True
  return error


def add_param_errors(series_name, req, resp):
  error = global_errors(series_name, req, resp)
  if type(req) != list:
    resp['errors'].append("Submit a request with 'Content-type: "
                          "application/json' and a list of events")
    error = True
  return error


def get_param_errors(series_name, req, resp):
  error = global_errors(series_name, req, resp)
  if type(req) != dict:
    resp['errors'].append("Submit a request with 'Content-type: "
                          "application/json' and a dictionary with "
                          "start/end ranges")
    error = True
  elif ('end_time' not in req or
        ('start_time' not in req and 'start_id' not in req)):
    resp['errors'].append('Must include an end_time one of start_time or start_id')
    error = True
  elif 'start_time' in req and 'start_id' in req:
    resp['errors'].append('Include only one of start_time or start_id')
    error = True
  return error
