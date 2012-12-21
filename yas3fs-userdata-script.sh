#!/bin/bash
YAS3FS_PATH=/opt/yas3fs
BUCKET_NAME=your_bucket
BUCKET_PATH=can_be_empty
LOG_PATH=/var/log
yum -y install fuse fuse-libs
yum -y install git # You need to install git on S3-backed AMI
yum -y update python-boto
easy_install pip
pip install fusepy
git clone git://github.com/danilop/yas3fs.git $YAS3FS_PATH
chmod u+x $YAS3FS_PATH/yas3fs.py
sed -i'' 's/^# user_allow_other/user_allow_other/' /etc/fuse.conf # uncomment user_allow_other
# You need a IAM Role to access the S3/SNS/SQS resources used by yas3fs
$YAS3FS_PATH/yas3fs.py /mnt/storage --url s3://$BUCKET_NAME/$BUCKET_PATH --mkdir --log $LOG_PATH/yas3fs.log
# On an EC2 instance you can add the following options
# to use the ephemeral storage
# to cache on disk 100GB of files larger than 1MB
# --cache-path /media/ephemeral0/yas3fs --cache-disk-size 102400 --cache-on-disk 1
# Execute a configuration/startup script here, e.g.
# /mnt/storage/start.sh
