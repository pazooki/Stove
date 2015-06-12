<pre>
   _____ _                 
  / ____| |                
 | (___ | |_ _____   _____ 
  \___ \| __/ _ \ \ / / _ \
  ____) | || (_) \ V /  __/
 |_____/ \__\___/ \_/ \___|
                           
                           
Version 0.1
</pre>

A cute minimalistic job scheduler written in Python for running Spark applications.

# What was Stove built for?

Spark is not exactly made for batch jobs but Stove uses Spark for processing log files in batch currently.
You could think of Stove as a place to organize your Spark applications that you run routinely or on demand
Stove job is to take care of pluming tasks such as:
- Writing Spark applications in a consistent format that goes through pre defined stages
- Making triggers for running them
- Doing X with the result
- Using the generalizing common functions for spark applications

# What values does Stove provide for you?

- Provides a template for writing a Spark Application
- Provides a generalized interface for filesystems that you may want to use: S3, HDFS, Local
- Stove is not going to limit you in writing an application in any way

# Example:
Lets launch a cluster on AWS and light on the Stove!

Create a config file for boto with read permission for your user if you don't already have it on /etc/boto.cfg
http://boto.cloudhackers.com/en/latest/boto_config_tut.html

First go to settings/config.py and add your keys to it.

<pre>
cluster = {
    's3': {
        'AWS_SECRET_ACCESS_KEY': '',
        'AWS_ACCESS_KEY_ID': ''
    },
    'key': 'SSH Key',
    'ssh_key': '~/.ssh/id_prod',
}
</pre>


Now cd to the root directory of the project
<pre>
python setup/ec2.py
</pre>

That will start a cluster and it will take care of other configurations for you
If it fails in the middle for whatever reason, you can restart the configuration without creating new instances by:
<pre>
python setup/ec2.py -x --resume
</pre>

Now if you want to run the segment_stats job after the cluster is up and running you could do it by syncing this project with to /root/Sparks on the master
<pre>
python setup/ec2.py --sync
</pre>

Now you could ssh to the master by running this command:
<pre>
python setup/ec2.py -a login
</pre>

Now we want to provision all slaves
<pre>
python setup/ec2.py --provision
</pre>

To run a command remotely on the master (you have to put it inside single quotes, e.g. 'tail -100 /var/log/messages')
<pre>
python setup/ec2.py --master-shell '<cmd>'
</pre>

Lets run a job, the provision script will put spark in your path so you could just submit the application like this:
<pre>
python setup/ec2.py --sync --master-shell 'spark-submit /root/apps/example/hello_world.py'
</pre>

You could watch the logs on a separate terminal
<pre>
python setup/ec2.py --master-shell 'tail -f /var/log/stove.log'
</pre>