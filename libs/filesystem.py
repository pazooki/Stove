import subprocess
import sys
import os
import hadoopy
from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.key import Key

try:
    from config import cluster
    from formatting import log
except ImportError:
    try:
        from settings.config import cluster
        from libs.formatting import log
    except ImportError as ex:
        print 'Failed to import: %s\n%s' % (ex.message, sys.path)


class LocalFileSystem(object):
    def __init__(self, **kwargs):
        self.blobs = []
        self.uploaded = []
        self.downloaded = []
        self.removed = []
        self.moved = []
        self.files = []

    def ls(self, path, max_size, conditions=None):
        if self.files:
            return self.files
        conditions = conditions or [lambda path: os.path.isfile(path)]
        if not path.endswith('/'):
            if all(condition(path) for condition in conditions):
                self.files.append(path)
                return self.files
        for f in os.listdir(path):
            absolute_path = os.path.join(path, f)
            if all(condition(absolute_path) for condition in conditions):
                self.files.append(absolute_path)
            if len(self.files) == max_size:
                break
        return self.files

    def mv(self, src, dest):
        try:
            src, dest = self.normalize_path(src), self.normalize_path(dest)
            os.rename(src, dest)
            self.moved.append((src, dest))
            return True
        except Exception as ex:
            log('local filesystem mv failed: %s' % ex.message)
            raise ex

    def rm(self, paths):
        for path in paths:
            try:
                os.remove(path)
                self.removed.append(path)
            except Exception as ex:
                log('local filesystem rm failed: %s' % ex.message)
        return self.removed

    def upload(self, src, dest):
        try:
            p = subprocess.Popen(self.build_scp_cmd(src, dest), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if err:
                raise Exception('SCP failed: %s' % err)
            self.uploaded.append(src)
            log('Uploaded file %s to %s - %s' % (src, dest, out))
            return True
        except Exception as ex:
            log('Failed to upload the file to %s - Error: %s' % (dest, ex.message))
            raise ex

    def download(self, src, dest):
        try:
            p = subprocess.Popen(self.build_scp_cmd(src, dest), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if err:
                raise Exception('SCP failed: %s' % err)
            self.downloaded.append((src, dest))
            log('Downloaded file %s to %s - %s' % (src, dest, out))
            return True
        except Exception as ex:
            log('Failed to download the file from %s - Error: %s' % (src, ex.message))
            raise ex

    @staticmethod
    def build_scp_cmd(src, dest):
        command = []
        command.append('scp')
        if src.endswith('/'):
            command.append('-r')
            dest = os.path.join(dest, src.split('/')[-1]) if dest.endswith('/') else dest
        command.append(src)
        command.append(dest)
        return command

    @staticmethod
    def normalize_path(path):
        try:
            return path.replace('local://', '')
        except Exception as ex:
            log('Invalid Path for local file system: %s. Normalization failed: %s' % (path, ex.message))

class S3(object):
    def __init__(self, **kwargs):
        self.key = cluster.get('s3').get('AWS_ACCESS_KEY_ID')
        self.secret = cluster.get('s3').get('AWS_SECRET_ACCESS_KEY')
        self.con = self.connect()
        self.removed = []
        self.moved = []
        self.uploaded = []
        self.files = []
        self.keys = {}
        self.downloaded = []

    def connect(self):
        try:
            return S3Connection(self.key, self.secret)
        except S3ResponseError as ex:
            log(ex.message)

    def ls(self, path, max_size, conditions=None):
        if self.files:
            return self.files
        conditions = conditions or [lambda path: path not in ['', None]]
        try:
            bucket_name, dir_path = self.normalize_path(path)
            bucket = self.con.get_bucket(bucket_name)
            for key in bucket.list(prefix=dir_path):
                spark_s3_path = self.make_spark_friendly_s3path(key)
                if spark_s3_path in ['', None, False]:
                    continue
                if all(condition(spark_s3_path) for condition in conditions):
                    self.files.append(spark_s3_path)
                if len(self.files) == max_size:
                    break
            return self.files
        except Exception as ex:
            log('Invalid Path: %s - %s' % (path, ex.message))

    def mv(self, src, dest):
        src_bucket_name, src_key_path = self.normalize_path(src)
        dest_bucket_name, dest_key_path = self.normalize_path(dest)
        try:
            src_bucket = self.con.get_bucket(src_bucket_name)
            obj = src_bucket.get_key(src_key_path)
            obj.copy(dest_bucket_name, dest_key_path)
            obj.delete()
            self.moved.append((src, dest))
            return True
        except Exception as ex:
            log('s3 mv failed: %s' % ex.message)
            raise ex

    def rm(self, paths=None):
        paths = paths or self.files
        for path in paths:
            src_bucket_name, src_key_path = self.normalize_path(path)
            try:
                src_bucket = self.con.get_bucket(src_bucket_name)
                obj = src_bucket.get_key(src_key_path)
                obj.delete()
                self.removed.append(path)
            except Exception as ex:
                log('s3 rm failed: %s' % ex.message)
                raise ex
        return self.removed

    def upload(self, src, dest):
        s3_bucket, s3_key = self.normalize_path(dest)
        s3_key += os.path.basename(src)
        if s3_key not in self.keys:
            self.keys[s3_key] = {}
            try:
                self.keys[s3_key]['bucket'] = self.con.get_bucket(s3_bucket)
                self.keys[s3_key]['key'] = self.keys.get(s3_key).get('bucket').get_key(s3_key)
                if not self.keys.get(s3_key).get('key'):
                    self.keys[s3_key]['key'] = Key(self.keys.get(s3_key).get('bucket'))
                    self.keys.get(s3_key).get('key').key = s3_key
            except S3ResponseError:
                self.keys[s3_key]['bucket'] = self.con.create_bucket(s3_bucket)
                self.keys[s3_key]['key'] = Key(self.keys.get(s3_key).get('bucket'))
                self.keys.get(s3_key).get('key').key = s3_key
        try:
            self.keys.get(s3_key).get('key').set_contents_from_filename(src, cb=self.pct_report, num_cb=10)
            self.uploaded.append(src)
            log('Uploaded file %s to destination %s' % (src, s3_key))
            return True
        except Exception as ex:
            log('Failed to upload the file: %s - Error: %s' % (s3_key, ex.message))
            raise ex

    def download(self, src, dest):
        bucket_name, key_name = self.normalize_path(src)
        s3_bucket = self.con.get_bucket(bucket_name)
        for key in s3_bucket.list(key_name):
            destination = os.path.join(dest, key.name.split('/')[-1])
            try:
                key.get_contents_to_filename(destination, cb=self.pct_report, num_cb=10)
                self.downloaded.append(src)
                log('Downloaded file %s from S3 to destination %s' % (key.name, destination))
                return True
            except Exception as ex:
                log('Failed to download the file: %s - Error: %s' % (key.name, ex.message))
                raise ex

    @staticmethod
    def pct_report(transfered_bytes, total_bytes):
        pct = (float(transfered_bytes)/float(total_bytes))*100
        log('S3 Transfer Status: total=%d bytes completed=%d bytes - %%%.2f' % (transfered_bytes, total_bytes, pct))

    @staticmethod
    def make_spark_friendly_s3path(key):
        if key.name.endswith('/'):
            return None
        s3n = cluster.get('s3')
        s3n['PATH_TO_FILE'] = '%s/%s' % (key.bucket.name, key.name)
        return 's3n://%(AWS_ACCESS_KEY_ID)s:%(AWS_SECRET_ACCESS_KEY)s@%(PATH_TO_FILE)s' % s3n

    @staticmethod
    def normalize_path(path):
        """
        :param path: '/bucket/incoming'
        :return: ('bucket', 'incoming')
        """
        try:
            path = path.replace('s3n:', '')
            path = path.replace('s3:', '')
            path = ''.join(path.split('@')[1:]) if '@' in path else path
            while path.startswith('/'):
                path = path[1:]
            return tuple(path.split('/', 1))
        except Exception as ex:
            log('Invalid Path for S3: %s. Normalization failed: %s' % (path, ex.message))


class HDFS(object):
    def __init__(self, **kwargs):
        self.uploaded = []
        self.downloaded = []
        self.removed = []
        self.moved = []
        self.files = []

    def ls(self, path, max_size, conditions=None):
        if self.files:
            return self.files
        conditions = conditions or [lambda path: path not in ['', None]]
        for blob in hadoopy.ls(path):
            absolute_path = self._get_absolute_path(blob)
            if all(condition(absolute_path) for condition in conditions):
                self.files.append(absolute_path)
            if len(self.files) == max_size:
                break
        return self.files

    def mv(self, src, dest):
        get_parent_dir = lambda x: '/'.join(x.split('/')[:-1])
        if not hadoopy.isdir(get_parent_dir(dest)):
            hadoopy.mkdir(get_parent_dir(dest))
        hadoopy.mv(src, get_parent_dir(dest))
        self.moved.append((src, dest))
        return True

    def rm(self, paths=None):
        paths = paths or self.files
        for path in paths:
            try:
                hadoopy.rmr(path)
                self.removed.append(path)
            except Exception as ex:
                log('hdfs rm failed: %s' % ex.message)
                raise ex
        return self.removed

    def _get_absolute_path(self, path):
        return 'hdfs://%s' % path

    def upload(self, src, dest):
        destination = os.path.join(dest, src.split('/')[-1])
        try:
            hadoopy.put(src, destination)
            self.uploaded.append(src)
            log('Uploaded file %s to HDFS %s' % (src, destination))
            return True
        except Exception as ex:
            log('Failed to upload the file to HDFS: %s - Error: %s' % (src, ex.message))
            raise ex

    def download(self, src, dest):
        try:
            hadoopy.get(src, dest)
            self.downloaded.append(src)
            log('Downloaded file %s from HDFS to destination %s' % (src, dest))
            return True
        except Exception as ex:
            log('Failed to download the file from HDFS: %s - Error: %s' % (src, ex.message))
            raise ex


class Source(object):
    def __init__(self, **kwargs):
        self.conditions = kwargs.get('conditions')
        self.source = kwargs.get('source')
        self.destination = kwargs.get('destination') or ('%s/processed' % '/'.join(kwargs.get('source').split('/')[:-1]))
        self.protocol = None
        self.files = []
        self.processed = []
        self.min_size = kwargs.get('min_size')
        self.max_size = kwargs.get('max_size')
        self.fs = self.get_filesystem()

    @property
    def get_class_variables(self):
        return {key: value for key, value in self.__dict__.items() if not key.startswith('__') and not callable(key)}

    def get_protocol(self):
        if self.protocol is not None:
            return self.protocol
        self.set_protocol()
        return self.protocol

    def set_protocol(self):
        if '://' in self.source:
            self.protocol, self.source = self.source.split('://')
        else:
            self.protocol, self.source = '', self.source
        self.protocol += '://'

    def get_filesystem(self):
        return {
            's3://': S3(**self.get_class_variables),
            'hdfs://': HDFS(**self.get_class_variables),
            'local://': LocalFileSystem(**self.get_class_variables),
        }.get(self.get_protocol())

    @property
    def count(self):
        return len(self.fs.ls(self.source, max_size=self.max_size, conditions=self.conditions))

    def ls(self):
        if self.count < self.min_size:
            return []
        else:
            self.files = self.fs.ls(self.source, max_size=self.max_size, conditions=self.conditions)
            return self.files

    def rm(self, paths):
        return self.fs.rm(paths)

    def get_processed_files(self):
        return self.processed

    def archive(self, dest=None):
        get_file_name = lambda x: x.split('/')[-1]
        for file in self.files:
            if not dest:
                dest = '%s/%s' % (self.destination, get_file_name(file))
            if self.fs.mv(file, dest):
                self.processed.append(dest)
        return True

    def nuke(self):
        self.fs.rm(self.fs.ls())


class FileSystem(object):
    def __init__(self, **kwargs):
        self.sources = kwargs.get('sources', [])
        self.paths = kwargs.get('paths', [])
        self.processed = kwargs.get('processed', [])

    def get_sources(self):
        return self.sources

    def put(self, *sources):
        for source in sources:
            self.sources.append(source)

    @property
    def count(self):
        return reduce(lambda x, y: x+y, [s.count for s in self.sources], 0)

    def ls(self, options=None):
        if not options:
            if self.paths:
                return self.paths
            try:
                for source in self.sources:
                    for path in source.ls():
                        self.paths.append(path)
                return self.paths
            except Exception as ex:
                log(ex.message)
                return []
        elif options in ['fancy']:
            return self._get_fancy_paths()
        elif options in ['spark']:
            return self._get_spark_friendly_paths()

    def _get_fancy_paths(self):
        if not self.paths:
            return 'There is no path available.'
        import json
        sort_by_protocol = lambda paths: sorted(paths, key=lambda x: x.split(':/', 1)[0])
        return json.dumps(sort_by_protocol(self.paths), indent=4, sort_keys=True)

    def _get_spark_friendly_paths(self):
        new_files = self.ls()
        return ','.join(new_files)

    def get_processed_files(self):
        return self.processed

    def archive(self, dest=None):
        results = []
        for source in self.sources:
            result = source.archive(dest)
            if result:
                results.append((source.files, result))
                for f in source.get_processed_files():
                    self.processed.append(f)
        return all(results)

    def nuke(self, dest=None):
        results = []
        for source in self.sources:
            result = source.rm(source.files)
            if result == source.files:
                results.append(True)
                self.processed += result
            else:
                results.append(False)
        return all(results)