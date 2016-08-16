#!/usr/bin/env python2.6
import json
import requests
import traceback

import time
from update_logs import update_logs_main 

from functools import partial
from os.path import abspath

# Simple implementation of http://docs.graylog.org/en/latest/pages/gelf.html
class GrayLogger(object):
  # More info on severity levels can be found at
  # https://en.wikipedia.org/wiki/Syslog#Severity_level
  ALERT = 1
  CRITICAL = 2
  ERROR = 3
  WARNING = 4
  NOTICE = 5
  INFO = 6
  DEBUG = 7

  HEADERS = { 'Content-Type': 'application/json' }

  def __init__(self, api_url, host=None):
    if host is None:
      # Set |host| to be the name of the caller file.
      import inspect
      self._host = abspath(inspect.getouterframes(inspect.currentframe())[1][1])
    else:
      self._host = host
    self._api_url = api_url

  def log(self, level, short_msg, full_msg='', additional_fields={}):
    gelf_str = self._create_gelf(short_msg, full_msg, level, additional_fields)
    r = self._json(gelf_str)
    if r.status_code >= 300:
      print(r.text)
      # Handle errors
      pass

  def __getattr__(self, name):
    levels = dict((k.lower(), v) for k, v in GrayLogger.__dict__.items()
                  if not k.startswith('__') and not callable(k))

    if name in levels:
      return partial(self.log, levels[name])
    raise AttributeError()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type:
      self.error('Uncaught Exception', full_msg=traceback.format_exc())
      self.error('Uncaught Exception', full_msg=traceback.format_exc())
      time.sleep(2)
      update_logs_main()
      return False
    else:
      time.sleep(2)
      update_logs_main()

  def _json(self, data):
    data = json.dumps(data)
    return requests.post(self._api_url, data=data, headers=GrayLogger.HEADERS)

  def _create_gelf(self, short_msg, full_msg, level, additional_fields):
    gelf = {
      'version': '1.1',
      'host': self._host,
      'short_message': short_msg,
      'full_message': full_msg,
      'level': level
    }

    for key, value in additional_fields.items():
      if not key.startswith('_'):
        key = '_%s' % key
      gelf[key] = value
    return gelf
