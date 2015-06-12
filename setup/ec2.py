import optparse
import subprocess
import sys
import os

project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_dir)
from settings import config

ENV = 'source /root/stove/setup/env.sh;'

def log(opts, msg):
    if opts.debug:
        print msg

def sync(opts):
    destination = 'root@%s:%s' % (get_master(opts), '/root/')
    rsync_cmd = 'rsync -avL --progress -e ssh -i %s %s %s' % (opts.ssh_key, project_dir, destination)
    log(opts, rsync_cmd)
    subprocess.Popen(rsync_cmd, shell=True).wait()


def provision(opts):
    opts.master_shell = 'chmod -R +x /root/stove'
    master_shell(opts)
    opts.master_shell = 'chmod +x %(provision_script)s; source %(provision_script)s' % {
        'provision_script': '/root/stove/setup/provision.sh'
    }
    master_shell(opts)


def master_shell(opts):
    command = ENV + opts.master_shell
    cmd = 'echo \'%(cmd)s\' | ssh %(destination)s /bin/bash' % {
        'destination': 'root@%s' % get_master(opts),
        'cmd': command,
    }
    log(opts, cmd)
    try:
        subprocess.Popen(cmd, shell=True).wait()
    except KeyboardInterrupt as ex:
        log(opts, ex.message)


def get_master(opts):
    if opts.master:
        return opts.master
    opts.action = 'get-master'
    args = build_args(opts)
    return subprocess.Popen(args, shell=False, stdout=subprocess.PIPE).stdout.readlines()[2].replace('\n', '')


def build_args(opts):
    args = [
        os.path.join(opts.spark_path, 'ec2/spark_ec2.py'),
        '-s', opts.slaves,
        '-k', opts.key,
        '-i', opts.ssh_key,
        opts.action, opts.name,
        '--region=%s' % opts.region,
        '--zone=%s' % opts.zone
    ]
    if opts.extras:
        args += opts.extras.split(' ')
    log(opts, ' '.join(args))
    return args


def normalize_opts(opts):
    if '~' in opts.ssh_key:
        opts.ssh_key = os.path.expanduser(opts.ssh_key)
    if '~' in opts.spark_path:
        opts.spark_path = os.path.expanduser(opts.spark_path)
    return opts


def clone_spark(opts):
    if not os.path.isdir(opts.spark_path):
        print 'You do not have spark source code, let me clone it for you...'
        git('clone', 'git://github.com/apache/spark.git', opts.spark_path)
    return os.path.isdir(opts.spark_path)


def git(*args):
    return subprocess.Popen('git %s' % ' '.join(args), shell=True).wait()


def prep_env(opts):
    clone_spark(opts)


def main(opts):
    prep_env(opts)
    if opts.sync:
        sync(opts)

    if opts.provision:
        provision(opts)
    elif opts.master_shell:
        master_shell(opts)
    else:
        args = build_args(opts)
        subprocess.call(args)


if __name__ == '__main__':
    usage = """
    This is a script for quick cluster setup on AWS
    **NOTES:
        If you are setting a placement-group you must specify a zone in that region as well
        to get the name of zones run this command: $aws ec2 describe-availability-zones --region us-east-1

    # Now cd to the root directory of the project
    python setup/ec2.py

    # That will start a cluster and it will take care of other configurations for you
    # If it fails in the middle for whatever reason, you can restart the configuration without creating new instances by:
    python setup/ec2.py -x --resume

    # Now if you want to run the segment_stats job after the cluster is up and running you could do it by syncing this project with to /root/Sparks on the master
    python setup/ec2.py --sync

    # Now you could ssh to the master by running this command:
    python setup/ec2.py -a login

    # Now we want to provision all slaves
    python setup/ec2.py --provision

    # To run a command remotely on the master (you have to put it inside single quotes, e.g. 'tail -100 /var/log/messages')
    python setup/ec2.py --master-shell '<cmd>'

    # Lets run a job, the provision script will put spark in your path so you could just submit the application like this:
    python setup/ec2.py --sync --master-shell 'spark-submit /root/apps/example/hello_world.py'

    # You could watch the logs on a separate terminal
    python setup/ec2.py --master-shell 'tail -f /var/log/stove.log'
"""
    parser = optparse.OptionParser(usage)
    parser.add_option('-b', '--spark-base-path', dest='spark_path', default='~/spark', help='Spark Base Directory')
    parser.add_option('-a', '--action', dest='action', default='launch', help='launch, destroy, login, stop, start, get-master, reboot-slaves')
    parser.add_option('-s', '--slaves', dest='slaves', default='2', help='Number of slaves')
    parser.add_option('-r', '--region', dest='region', default='us-east-1', help='AWS Region')
    parser.add_option('-z', '--zone', dest='zone', default='us-east-1b', help='AWS Zone')
    parser.add_option('-k', '--key-pair', dest='key', default=config.cluster.get('key'), help='AWS Key for the region')
    parser.add_option('-i', '--identity-file', dest='ssh_key', default=config.cluster.get('ssh_key'), help='SSH private key for the region')
    parser.add_option('-n', '--name', dest='name', default='sparkcluster', help='Cluster Name')
    parser.add_option('-x', '--extras', dest='extras', default=None, help='Extras')
    parser.add_option('--sync', dest='sync', action="store_true", default=False, help='Only sync the project directory')
    parser.add_option('--provision', dest='provision', action="store_true", default=False, help='Provision the cluster')
    parser.add_option('--master', dest='master', default=None, help='Public DNS of master')
    parser.add_option('--master-shell', dest='master_shell', default=None, help='Run a command remotely on master')
    parser.add_option('--debug', dest='debug', action="store_true", default=False, help='Print out debug messages')
    parser.add_option('--tests', dest='tests', action="store_true", default=False, help='Running tests.')

    options, remainder = parser.parse_args()
    opts = normalize_opts(options)
    main(opts=opts)
