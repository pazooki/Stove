from operator import add
import time
import os
import sys
import inspect
from functools import partial

try:
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
except KeyError:
    print (sys.path, os.environ)

sys.path.insert(0, '/root/ephemeral-hdfs/bin/')

try:
    from libs.formatting import log
    from libs.filesystem import Source, FileSystem, S3, HDFS, LocalFileSystem
    from libs import finalizer
    from libs.application import Application, SparkConf
except ImportError as ex:
    log(ex.message)
