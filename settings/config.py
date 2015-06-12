
sparks_config = {
    'debug': False
}

cluster = {
    's3': {
        'AWS_SECRET_ACCESS_KEY': '',
        'AWS_ACCESS_KEY_ID': ''
    },
    'key': 'SSH Key',
    'ssh_key': '~/.ssh/id_prod',
}


defaults = {
    'formats':
        {
            'date_1': '%Y-%m-%d',
            'date_hour': '%Y-%m-%d %H:00:00'
        }
}

try:
    from prod_config import cluster, stove_config
except ImportError:
    pass

