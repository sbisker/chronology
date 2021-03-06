import os
import re
import sys
import zipfile

from metis import app

IGNORE_FILES_RE = re.compile('^.*(pyc|~|zip)$', re.I)


def create_lib_for_spark_workers(): 
  archive_path = '%s/metis.zip' % app.config['PATH']
  app.config['METIS_LIB_FILE'] = archive_path
  
  if not app.debug and zipfile.is_zipfile(archive_path):
    # If zip file is present and we're not running in debug mode, don't
    # regenerate it.
    return
  
  zip_file = zipfile.ZipFile(archive_path, 'w')
  # TODO(usmanm): Zip only the minimum set of files needed.
  for root, dirs, files in os.walk(app.config['PATH'], followlinks=True):
    for file_name in files:
      if IGNORE_FILES_RE.match(file_name):
        continue
      zip_file.write(os.path.join(root, file_name),
                     os.path.join(root.replace(app.config['PATH'], 'metis'),
                                  file_name))
  zip_file.close()


def setup_pyspark():
  # Set SPARK_HOME environment variable.
  os.putenv('SPARK_HOME', app.config['SPARK_HOME'])
  # From Python docs: Calling putenv() directly does not change os.environ, so 
  # it's better to modify os.environ. Also some platforms don't support
  # os.putenv. We'll just perform both.
  os.environ['SPARK_HOME'] = app.config['SPARK_HOME']
  # Add PySpark to path.
  sys.path.append(os.path.join(app.config['SPARK_HOME'], 'python'))
