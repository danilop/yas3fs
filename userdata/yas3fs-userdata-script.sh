#!/bin/bash
MOUNT_PATH=/mnt/storage
YAS3FS_PATH=/opt/yas3fs
AWS_REGION=eu-west-1
BUCKET_NAME=your_bucket
BUCKET_PATH=can_be_empty
SNS_TOPIC=topic_arn # Optional
LOG_PATH=/var/log
yum -y update # Optional
yum -y install fuse fuse-libs
yum -y install git # You need to install git on S3-backed AMI
yum -y update python-boto
easy_install pip
pip install fusepy
git clone git://github.com/danilop/yas3fs.git $YAS3FS_PATH
sed -i'' 's/^# user_allow_other/user_allow_other/' /etc/fuse.conf # uncomment user_allow_other
# You need a IAM Role to access the S3/SNS/SQS resources used by yas3fs
$YAS3FS_PATH/yas3fs.py $MOUNT_PATH --region $AWS_REGION --url s3://$BUCKET_NAME/$BUCKET_PATH --mkdir --log $LOG_PATH/yas3fs.log # --topic $SNS_TOPIC --new-queue
# On an EC2 instance you can add the following options
# to use the ephemeral storage
# to cache on disk 100GB of files larger than 1MB
# --cache-path /media/ephemeral0/yas3fs --cache-disk-size 102400 --cache-on-disk 1
# Execute a configuration/startup script here, e.g.
# $MOUNT_PATH/start.sh
