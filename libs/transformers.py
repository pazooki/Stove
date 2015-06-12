try:
    from config import defaults
except ImportError:
    try:
        from settings.config import defaults
    except ImportError:
        print 'At this point you can go ...'

import datetime


def ts2date(ts, format):
    try:
        return datetime.datetime.fromtimestamp(ts).strftime(defaults.get('formats').get(format))
    except Exception as ex:
        print (ex.message, ts)


def row2dict(keys, line):
    try:
        line_keys, value = line
        line_keys += (value,)
        output = {}
        for i, key in enumerate(keys):
            output[key] = line_keys[i]
        return output
    except Exception as ex:
        print (ex.message, line)