import tempfile
import socket
import os
import shutil

try:
    from formatting import log, json_encode
    from settings.config import sparks_config
except ImportError:
    try:
        from libs.formatting import log, json_encode
    except ImportError:
        print 'At this point you can go ...'


class Finalizer(object):
    def __init__(self):
        self.tasks = []
        self.stages = []

    def add_task(self, task):
        self.tasks.append(task)

    def set_tasks(self, tasks):
        self.tasks = tasks

    def run(self, original_input, previous_task=None):
        for task in self.tasks:
            try:
                self.stages.append(task)
                previous_task = task.run(original_input, previous_task)
            except Exception as ex:
                log(ex.message)
                self.revert(ex.message)
        return True

    def revert(self, reason):
        log('We are going to revert these stages (in order):\n%s' % ''.join([
            '\t\t%d. %s\n' % (i, stage.__class__.__name__) for i, stage in enumerate(reversed(self.stages))
        ]))
        for task in reversed(self.stages):
            try:
                task.revert()
            except Exception as ex:
                log(ex.message)
                pass
        raise Exception('Finalizer failed: %s' % reason)


class Task(object):
    def __init__(self, extras=None):
        self.status = 'initialized'
        self.error = None
        self.result = None
        self.extras = extras if extras else {}

    def revert(self):
        self.set_status('failed')
        pass

    def run(self, original_input, previous_result):
        self.set_status('running')
        return self

    def set_status(self, status):
        self.status = status
        if self.status in ['failed']:
            log('Reverting %s task.' % self.__class__.__name__)


class WriteToFile(Task):
    def run(self, original_input, previous_task):
        self.set_status('running')
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(json_encode(original_input))
                
            if self.extras.get('destination'):
                self.result = self.extras.get('destination')
                shutil.move(tmp.name, self.extras.get('destination'))
            else:
                self.result = tmp.name
                
            log('Result stored at: %s:%s' % (socket.gethostname(), self.result))
                
        except Exception as ex:
            raise Exception('Writing to temporary file failed: %s' % ex.message)
        return self

    def revert(self):
        self.set_status('failed')
        os.remove(self.result)
        return True


class Log(Task):
    def run(self, original_input, previous_task):
        self.set_status('running')
        try:
            import json
            log(json.dumps(original_input, indent=4, sort_keys=True))
        except Exception as ex:
            raise Exception('Logging failed: %s' % ex.message)
        return self

    def revert(self):
        self.set_status('failed')
        return True


class Archive(Task):
    def run(self, original_input, previous_task):
        self.set_status('running')
        try:
            fs = self.extras
            if fs.archive():
                log('Archived %d files:\n%s' % (len(fs.get_processed_files()), ''.join(['\t\t%s\n' % f for f in fs.get_processed_files()])))
            else:
                raise Exception('not all files were archived.')
        except Exception as ex:
            raise Exception('Archiving failed: %s' % ex.message)
        return self

    def revert(self):
        self.set_status('failed')
        log('You have to move these files back manually:\n%s' % self.extras.get_processed_files())
        return True


class Nuke(Task):
    def run(self, original_input, previous_task):
        self.set_status('running')
        try:
            fs = self.extras
            if fs.nuke():
                if sparks_config.get('debug'):
                    log('Nuked:\n%s' % ''.join(['\t\t%s\n' % f for f in fs.get_processed_files()]))
                else:
                    log('Nuked %d files.' % len(fs.get_processed_files()))
            else:
                raise Exception('not all files were archived.')
        except Exception as ex:
            raise Exception('Archiving failed: %s' % ex.message)
        return self

    def revert(self):
        self.set_status('failed')
        return True