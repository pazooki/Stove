import ujson
import logging
import setproctitle
try:
    from settings.config import sparks_config
except Exception:
    sparks_config = {'debug': True}


if sparks_config.get('debug'):
    fmt = '%(asctime)s - %(process)d - %(pathname)s:%(lineno)d - %(message)s'
    logging.basicConfig(filename='/var/log/sparks.log', level=logging.INFO, format=fmt)
    logger = logging.getLogger('sparks')
    log = logger.info
else:
    fmt = '%(asctime)s - %(name)s - %(process)d - %(message)s'
    logging.basicConfig(filename='/var/log/sparks.log', level=logging.INFO, format=fmt)

    def log(msg):
        logger = logging.getLogger('sparks.%s' % setproctitle.getproctitle())
        logger.info(msg)


def json_to_dict(line):
    try:
        return ujson.loads(line)
    except Exception as ex:
        print (ex.message, line)
        return {}


def json_encode(obj):
    try:
        return ujson.dumps(obj)
    except Exception as ex:
        print (ex.message, obj)


def s3path(path):
    return 's3n://%(AWS_ACCESS_KEY_ID)s:%(AWS_SECRET_ACCESS_KEY)s@' + path