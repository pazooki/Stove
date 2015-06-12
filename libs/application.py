import os
import sys
import time
import imp
import inspect
import setproctitle

project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_dir)

from libs.formatting import log
from libs.finalizer import Finalizer
from libs.filesystem import FileSystem
from settings.config import sparks_config

try:
    from pyspark import SparkConf
    from pyspark import SparkContext
except ImportError as ex:
    log('Not a Spark application.')



class Application(object):
    """
        You must implement these methods:
            - should_run(self)
            - task(self, rdd, extras)
            - if_failed(self, exception)

        if __name__ == '__main__':
            app = ApplicationChildClass()
            app.filesystem.put(
                Source(source='hdfs:///tmp/incoming/', archive_to='hdfs:///tmp/processed', min_size=1, max_size=10),
                Source(source='s3:///bucket-name/incoming', archive_to='s3:///bucket-name/processed', min_size=1, max_size=1)
            )
            app.finalizer.set_tasks([
                finalizer.WriteToFile(),
                finalizer.Log(),
                finalizer.Archive(app.get_filesystem())
            ])
            app.set_dependencies(['settings/config.py', 'libs/formatting.py', 'libs/transformers.py'])
            app.run(extras={'incoming_files': app.filesystem.ls()})
    """

    def __init__(self, **kwargs):
        self.extras = kwargs.get('extras')
        self.stages = []
        self.stages.append(inspect.currentframe().f_code.co_name)
        self.name = self.__class__.__name__
        self.pid_file = '/var/run/%s.pid' % self.name
        self.pid = str(os.getpid())
        self.spark_context = None
        self.started_at = 0
        self.ended_at = 0
        self.status = 'initiated'
        self.imported = lambda: None
        self.included_libs = []
        self.lock()
        self.filesystem = FileSystem()
        self.finalizer = Finalizer()

    @property
    def spark_config(self):
        self.stages.append(inspect.currentframe().f_code.co_name)
        config = SparkConf()
        # config.setAppName('AbstractApplication')
        # config.set('spark.eventLog.enabled', True)
        # config.set('spark.task.cpus', 2)
        # config.set('spark.speculation', True)
        # config.set('spark.locality.wait', 100)
        # config.set('spark.localExecution.enabled', True)
        # config.set('spark.executor.memory', '6g')
        # config.set('spark.akka.askTimeout', 40)
        # config.set('hadoop.fs.s3.maxRetries', 5)
        # config.set('spark.eventLog.dir', '/var/log/')
        return config

    @property
    def properties(self):
        return {k: v for k, v in self.__dict__.items() if not k.startswith('__') and not callable(k)}

    def get_filesystem(self):
        return self.filesystem

    def set_dependencies(self, libs):
        self.stages.append(inspect.currentframe().f_code.co_name)
        self.included_libs = [os.path.abspath(os.path.join('Sparks/', lib)) for lib in libs]

    def load_dependencies(self):
        self.stages.append(inspect.currentframe().f_code.co_name)
        if not self.included_libs:
            return []

        log('Going to import: %s' % self.included_libs)
        get_module_name = lambda x: x.split('/')[-1].split('.')[0]
        try:
            for lib in self.included_libs:
                log('Importing: %s' % lib)
                setattr(self.imported, get_module_name(lib), imp.load_source(get_module_name(lib), lib))
                self.spark_context.addPyFile(lib)
            log('Successfully Imported: %s' % self.imported.__dict__.keys())
        except Exception as ex:
            self.fail(ex)
        return self.imported

    def set_status(self, status):
        self.timer(status)
        self.status = status

    def get_status(self):
        return self.status

    def timer(self, status):
        if status in ['started']:
            self.started_at = int(time.time())
            log('Application Execution Started.')
        elif status in ['ended', 'failed']:
            self.ended_at = int(time.time())
            ending_msg = ''
            if status in ['ended']:
                ending_msg += 'Application execution ended successfully '
                if sparks_config.get('debug'):
                    ending_msg += 'passed all stages: \n%s' % self.fancy_stages
            elif status in ['failed']:
                ending_msg += 'Application execution failed at: >>>> %s <<<<\n' % self.stages[-1]
                ending_msg += 'Stages: \n%s' % self.fancy_stages
            log(ending_msg)
            log('%d seconds was spent to run this job...' % (self.ended_at - self.started_at))
        elif status in ['shouldnotrun']:
            ending_msg = ''
            ending_msg += 'Application cannot be executed. Please check your implementation of should_run method.'
            log(ending_msg)
        elif status in ['alreadylocked']:
            pass

    def initiate_spark(self):
        self.stages.append(inspect.currentframe().f_code.co_name)
        self.spark_context = SparkContext(conf=self.spark_config)
        log('Initiated Spark.')

    @property
    def root_rdd(self):
        self.stages.append(inspect.currentframe().f_code.co_name)
        try:
            if self.filesystem.sources and self.spark_context:
                sources = self.filesystem.ls('spark')
                log('Going to process %s files: %s\n' % (self.filesystem.count, self.filesystem.ls('fancy')))
                return self.spark_context.textFile(sources)
            else:
                return self.filesystem.sources
        except Exception as ex:
            raise ex

    def run(self):
        self.stages.append(inspect.currentframe().f_code.co_name)
        try:
            if not self.is_locked_by_me:
                self.set_status('alreadylocked')
            elif self.should_run():
                    self.initiate_spark()
                    self.load_dependencies()
                    result = self.task(rdd=self.root_rdd)
                    self.finalize(result)
                    self.set_status('ended')
            else:
                self.set_status('shouldnotrun')
        except Exception as ex:
            self.fail(ex)
        self.exit()

    def finalize(self, result):
        self.stages.append(inspect.currentframe().f_code.co_name)
        return self.finalizer.run(result)

    @property
    def fancy_stages(self):
        return ''.join(['\t%d. %s\n' % (i, s) for i, s in enumerate(self.stages)])

    def fail(self, ex):
        if ex.message:
            log('Exception: %s\n\n' % ex.message)
        self.if_failed(ex)
        self.set_status('failed')

    def exit(self):
        if self.spark_context is not None:
            self.spark_context.stop()
        self.unlock()
        log('Application Execution Stopped.')

    def lock(self):
        try:
            setproctitle.setproctitle(self.name)
            self.stages.append(inspect.currentframe().f_code.co_name)
            self.set_status('started')
            if not self.is_already_locked_by_aliens:
                with open(self.pid_file, 'w+') as lock_file:
                    lock_file.write('%s\n' % self.pid)
                log('Locked %s - PID:%s' % (self.pid_file, self.pid))
            else:
                log('This application is locked by another processes. Lock File: %s' % self.pid_file)
        except Exception as ex:
            self.fail(ex)

    def unlock(self):
        try:
            self.stages.append(inspect.currentframe().f_code.co_name)
            if self.is_locked_by_me:
                os.remove(self.pid_file)
                log('Unlocked %s - PID:%s' % (self.pid_file, self.pid))
            elif self.is_already_locked_by_aliens:
                pass
            elif not os.path.isfile(self.pid_file):
                raise Exception('Trying to remove a lock file that does not exist. Lock File: %s' % self.pid_file)
        except Exception as ex:
            log('Failed to unlock: %s' % ex.message)

    @property
    def is_locked_by_me(self):
        if os.path.isfile(self.pid_file):
            with open(self.pid_file, 'r') as lock_file:
                pid = lock_file.readline().replace('\n', '')
                if pid in [self.pid]:
                    return True
        return False

    @property
    def is_already_locked_by_aliens(self):
        if os.path.isfile(self.pid_file):
            with open(self.pid_file, 'r') as lock_file:
                pid = lock_file.readline().replace('\n', '')
                if pid and pid not in [self.pid]:
                    return True
        return False

    def app_is_valid(self):
        pass

    def should_run(self):
        # Must be implemented for each app
        self.stages.append(inspect.currentframe().f_code.co_name)
        pass

    def task(self, rdd):
        # Must be implemented for each app
        self.stages.append(inspect.currentframe().f_code.co_name)
        return None

    def if_failed(self, ex):
        # Must be implemented for each app
        pass