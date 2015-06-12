from operator import add
import time
import os
import sys
import inspect
from functools import partial

sys.path.insert(0, '/root/ephemeral-hdfs/bin/')

try:
    from libs.formatting import log
    from libs.filesystem import Source, FileSystem, S3, HDFS, LocalFileSystem
    from libs import finalizer
    from libs.application import Application
except ImportError as ex:
    print '#'*20, sys.path, '#'*20, '\n'
    print '#'*20, ex.message, '#'*20, '\n'
    sys.exit(1)
