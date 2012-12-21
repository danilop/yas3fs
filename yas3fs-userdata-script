#!/bin/bash
set -e -x
export YAS3FS_PATH=/opt/yas3fs
yum -y install fuse fuse-libs
yum -y install git # You need to install git on S3-backed AMI
yum -y update python-boto
easy_install pip
pip install fusepy
git clone git://github.com/danilop/yas3fs.git $YAS3FS_PATH
chmod u+x $YAS3FS_PATH/yas3fs.py
sed -i'' 's/^# user_allow_other/user_allow_other/' /etc/fuse.conf # uncomment user_allow_other
# You need a IAM Role to access the S3/SNS/SQS resources used by yas3fs
$YAS3FS_PATH/yas3fs.py /mnt/storage --url s3://danilop-fs/storage --mkdir --log /opt/yas3fs.log
# Execute a configuration/startup script here, e.g.
# /mnt/storage/start.sh
