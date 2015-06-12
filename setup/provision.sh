#!/bin/bash -e


function installsysonmaster {
    echo ">>>>>>>>>>>>>>>> installsysonmaster <<<<<<<<<<<<<<<<<"
    yum install build-essential make automake gcc gcc-c++ kernel-devel git-core fuse fuse-devel fuse-libs ant -y
}

function installsysonslaves {
    echo ">>>>>>>>>>>>>>>> installsysonslaves <<<<<<<<<<<<<<<<<"
    for dest in $(</root/spark-ec2/slaves); do
      ssh ${dest} 'yum install build-essential make automake gcc gcc-c++ kernel-devel git-core -y'
    done
}
function installpython27onmaster {
    echo ">>>>>>>>>>>>>>>> installpython27onmaster <<<<<<<<<<<<<<<<<"
    yum install python27-devel -y
    rm /usr/bin/python
    ln -s /usr/bin/python2.7 /usr/bin/python
    cp /usr/bin/yum /usr/bin/_yum_before_27
    sed -i s/python/python2.6/g /usr/bin/yum
    sed -i s/python2.6/python2.6/g /usr/bin/yum
    python -V
}


function installpython27onslaves {
    echo ">>>>>>>>>>>>>>>> installpython27onslaves <<<<<<<<<<<<<<<<<"
    for dest in $(</root/spark-ec2/slaves); do
      ssh ${dest} 'yum install python27-devel -y'
      ssh ${dest} 'rm /usr/bin/python'
      ssh ${dest} 'ln -s /usr/bin/python2.7 /usr/bin/python'
      ssh ${dest} 'cp /usr/bin/yum /usr/bin/_yum_before_27'
      ssh ${dest} 'sed -i s/python/python2.6/g /usr/bin/yum'
      ssh ${dest} 'sed -i s/python2.6/python2.6/g /usr/bin/yum'
      ssh ${dest} 'python -V'
    done
}

function installpiponmaster {
    echo ">>>>>>>>>>>>>>>> installpiponmaster <<<<<<<<<<<<<<<<<"
    curl https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py | /usr/bin/python27
    easy_install-2.7 pip
}

function installpiponslaves {
    echo ">>>>>>>>>>>>>>>> installpiponslaves <<<<<<<<<<<<<<<<<"
    for dest in $(</root/spark-ec2/slaves); do
      echo ${dest}
      ssh ${dest} 'curl https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py | /usr/bin/python27'
      ssh ${dest} 'easy_install-2.7 pip'
    done
}

function installdependenciesonmaster {
    echo ">>>>>>>>>>>>>>>> installdependenciesonmaster <<<<<<<<<<<<<<<<<"
    pip install -r /root/Sparks/requirements.txt
}


function installdependenciesonslaves {
    echo ">>>>>>>>>>>>>>>> installdependenciesonslaves <<<<<<<<<<<<<<<<<"
    for dest in $(</root/spark-ec2/slaves); do
      scp /root/Sparks/requirements.txt ${dest}:/tmp
      ssh ${dest} 'pip install -r /tmp/requirements.txt'
    done
}

function configuremaster {
    echo ">>>>>>>>>>>>>>>> Configuring Master <<<<<<<<<<<<<<<<<"
}

function configureslaves {
    echo ">>>>>>>>>>>>>>>> Configuring Slaves <<<<<<<<<<<<<<<<<"
}

function gangliarestartonslaves {
    echo ">>>>>>>>>>>>>>>> installdependenciesonslaves <<<<<<<<<<<<<<<<<"
    for dest in $(</root/spark-ec2/slaves); do
      ssh ${dest} 'service gmond restart'
    done
}


installsysonmaster
installsysonslaves
installpython27onmaster
installpython27onslaves
installpiponmaster
installpiponslaves
installdependenciesonmaster
installdependenciesonslaves
configuremaster
configureslaves
#gangliarestartonslaves