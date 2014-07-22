from __future__ import absolute_import

import logging

from kronos.conf import settings

def configure():
  logging.basicConfig(
    filename='%s/app.log' % settings.node.log_directory.rstrip('/'),
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s')
