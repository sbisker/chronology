"""
  Helper functions to be used for storage client config validation
"""
import types

def is_non_empty_str(x):
  return isinstance(x, types.StringTypes) and len(x) > 0

def is_pos_int(x):
  return is_int(x) and x > 0

def is_int(x):
  return isinstance(x, (int, long))

def is_bool(x):
  return isinstance(x, bool)

def is_list(x):
  return isinstance(x, list)
