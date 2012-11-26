# Yet Another S3-backed File System: yas3fs

YAS3FS (Yet Another S3-backed File System) is a Filesystem in Userspace (FUSE) interface to Amazon S3.

* It allows to mount an S3 bucket (or a part of it) as a local folder.
* For maximum speed all data read from S3 is cached locally on the node.
* SNS notifications are used to update other nodes that something has changed on S3 and they need to invalidate their cache.
* Notifications can be listened using HTTP or SQS endpoints.
* With buffering enabled (the default) files can be accessed during the download from S3.
* If the cache grows to its maximum size the least accessed files are removed.
* AWS credentials can be passed using AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY environmental variables.
* In an EC2 instance a IAM role can be used to give access to S3/SNS/SQS resources.

On EC2 the command line doesn't need any information on the actual server and can easily be used within an Auto Scaling group.

To mount an S3 bucket without using SNS (i.e. single node):

yas3fs.py /path/to/mount --url=s3://bucket/path 

To mount an S3 bucket using SNS and listening to an HTTP endpoint:

yas3fs.py /path/to/mount --url=s3://bucket/path --topic TOPIC-ARN --ec2-hostname --port N

The security group must allow inbound traffic from SNS on the selected port.

To mount an S3 bucket using SNS and listening to an SQS endpoint:

yas3fs.py /path/to/mount --url=s3://bucket/path --topic TOPIC-ARN --new-queue

I strongly suggest to start yas3fs for the first time with the "-d" (debug) option, to see if there is any error. When everything works it can be interrupted (with ^C) and restarted to run in background (it's the default with no "-d" / "-f" options).

If you want to do a quick test here's the installation procedure on EC2 depending on the OS flavor (Linux or Mac):

* Create an S3 bucket in the region you work
* You don't need to create anything in the bucket as the initial path (if any) is created by the tool on the first mount
* If you want to use an existing S3 bucket you can use the "--no-metadata" option to not use user metadata to persist file system attr/xattr
* Create an SNS topic in the same region as the S3 bucket and write down the full topic ARN (you need it to run the tool if more than one client is connected to the same bucket/path)
* Create a IAM Role that gives access to the S3 and SNS/SQS resources you need or pass the AWS credentials to the tool using environmental variables (see "-h")
* I used the eu-west-1 region in my sample, but you can replace that with any region you want. If no region is specified it defaults to us-east-1.

**On Amazon Linux 2012.09**

sudo yum -y install fuse fuse-libs

sudo easy_install pip

sudo pip install fusepy

wget http://danilopoccia.s3.amazonaws.com/yas3fs.py

chmod u+x yas3fs.py

./yas3fs.py -h # See the usage

sudo vi /etc/fuse.conf # uncomment user_allow_other

mkdir LOCAL-PATH

./yas3fs.py LOCAL-PATH --url=s3://BUCKET/PATH --topic TOPIC-ARN --new-queue --region eu-west-1

**On Ubuntu Server 12.04.1 LTS**

sudo aptitude install fuse-utils libfuse2 python-pip

sudo pip install --upgrade boto fusepy

wget https://danilopoccia.s3.amazonaws.com/yas3fs.py

chmod u+x yas3fs.py

./yas3fs.py -h # See the usage

sudo vi /etc/fuse.conf  # uncomment user_allow_other

sudo chmod a+r /etc/fuse.conf # make it readable by anybody, it is not the default on Ubuntu

mkdir LOCAL-PATH

./yas3fs.py LOCAL-PATH --url=s3://BUCKET/PATH --topic TOPIC-ARN --new-queue --region eu-west-1

**On a Mac (tested under Mountain Lion)**

- Install FUSE for OS X from [http://osxfuse.github.com](http://osxfuse.github.com)

- To install the Python M2Crypto module, download the most suitable "egg" from [http://chandlerproject.org/Projects/MeTooCrypto#Downloads](http://chandlerproject.org/Projects/MeTooCrypto#Downloads)

sudo easy_install M2Crypto-*.egg

sudo easy_install boto

sudo easy_install fusepy

wget https://danilopoccia.s3.amazonaws.com/yas3fs.py

chmod u+x yas3fs.py

./yas3fs.py -h # See the usage

mkdir LOCAL-PATH

./yas3fs.py LOCAL-PATH --url=s3://BUCKET/PATH --topic TOPIC-ARN --new-queue --region eu-west-1

If something does not work as expected you can use the "-d" option to run in foreground in debug mode.

**Unmount**

To unmount the file system on Linux:

fusermount -u LOCAL-PATH

To unmount the file system on a Mac you can use 'umount'.

**Full Usage**

yas3fs.py -h

Usage: yas3fs.py &lt;mountpoint&gt; [options]

YAS3FS (Yet Another S3-backed File System) is a Filesystem in Userspace (FUSE) interface to Amazon S3.

It allows to mount an S3 bucket (or a part of it) as a local folder.
For maximum speed all data read from S3 is cached locally on the node.
SNS notifications are used to update other nodes that something has changed on S3 and they need to invalidate their cache.
Notifications can be listened using HTTP or SQS endpoints.
With buffering enabled (the default) files can be accessed during the download from S3.
If the cache grows to its maximum size the least accessed files are removed.
AWS credentials can be passed using AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY environmental variables.
In an EC2 instance a IAM role can be used to give access to S3/SNS/SQS resources.

Options:

  -h, --help         show this help message and exit

  --url=URL          the S3 path to mount in s3://BUCKET/PATH format, PATH can
                     be empty, can contain subfolders and is created on first
                     mount if not found in the BUCKET

  --region=REGION    AWS region to use for SNS/SQS (default is us-east-1)

  --topic=ARN        SNS topic ARN

  --hostname=HOST    hostname to listen to SNS HTTP notifications

  --ec2-hostname     get public hostname from EC2 instance metadata (overrides
                     '--hostname')

  --port=N           TCP port to listen to SNS HTTP notifications

  --queue=NAME       SQS queue name, a new queue is created if it doesn't
                     exist

  --new-queue        create a new SQS queue that is deleted on unmount
                     (overrides '--queue', queue name is BUCKET-PATH-N with
                     alphanumeric characters only)

  --queue-wait=N     SQS queue wait time in seconds (using long polling, 0 to
                     disable, default is 0 seconds)

  --queue-polling=N  SQS queue polling interval in seconds (default is 1
                     seconds)

  --cache-entries=N  max number of entries to cache (default is 1000000
                     entries)

  --cache-size=N     max size of the cache in MB (default is 1024 MB)

  --cache-check=N    interval between cache memory checks in seconds (default
                     is 10 seconds)

  --buffer-size=N    download buffer size in KB (0 to disable buffering,
                     default is 10240 KB)

  --no-metadata      don't write user metadata on S3 to persist file system
                     attr/xattr

  --id=ID            a unique ID identifying this node in a cluster, hostname
                     or queue name are used if not provided

  -f, --foreground   run in foreground

  -d, --debug        print debug information (implies '-f')


**Happy file sharing!**

