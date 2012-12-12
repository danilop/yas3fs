### Yet Another S3-backed File System: yas3fs

YAS3FS (Yet Another S3-backed File System) is a [Filesystem in Userspace (FUSE)](http://fuse.sourceforge.net)
interface to [Amazon S3](http://aws.amazon.com/s3/).

**This is a personal project. No relation whatsoever exists between this project and my employer.**

* It allows to mount an S3 bucket (or a part of it, if you specify a path) as a local folder.
* It works on Linux and Mac OS X.
* For maximum speed all data read from S3 is cached in memory locally on the node.
* It can be used on more than one node to create a "shared" file system (i.e. a yas3fs "cluster").
* [SNS](http://aws.amazon.com/sns/) notifications are used to update other nodes in the cluster that something has changed on S3 and they need to invalidate their cache.
* Notifications can be listened using HTTP or [SQS](http://aws.amazon.com/sqs/) endpoints.
* With buffering enabled (the default) files can be accessed during the download from S3 (e.g. for streaming).
* If the cache grows to its maximum size, the less recently accessed files are removed.
* AWS credentials can be passed using AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY environmental variables.
* In an [EC2](http://aws.amazon.com/ec2/) instance a [IAM](http://aws.amazon.com/iam/) role can be used to give access to S3/SNS/SQS resources.
* It is written in Python (2.6) using [boto](https://github.com/boto/boto) and [fusepy](https://github.com/terencehonles/fusepy).

### License

Copyright (c) 2012 Danilo Poccia, http://blog.danilopoccia.net

This code is licensed under the The MIT License (MIT). Please see the LICENSE.txt file that accompanies this project for the terms of use.

### Introduction

On EC2 the command line doesn't need any information on the actual server and can easily be used
within an [Auto Scaling](http://aws.amazon.com/autoscaling/) group.

To mount an S3 bucket without using SNS (i.e. for a single node):

    yas3fs.py /path/to/mount --url=s3://bucket/path 

To persist file system metadata such as attr/xattr yas3fs is using S3 User Metadata.
To mount an S3 bucket without actually writing metadata in it,
e.g. because it is a bucket you mainly use as a repository and not as a file system,
you can use the `--no-metadata` option.

To mount an S3 bucket using SNS and listening to an SQS endpoint:

    yas3fs.py /path/to/mount --url=s3://bucket/path --topic TOPIC-ARN --new-queue

To mount an S3 bucket using SNS and listening to an HTTP endpoint (on EC2):

    yas3fs.py /path/to/mount --url=s3://bucket/path --topic TOPIC-ARN --ec2-hostname --port N

On EC2 the security group must allow inbound traffic from SNS on the selected port.

I strongly suggest to start yas3fs for the first time with the `-d` (debug) option, to see if there is any error.
When everything works it can be interrupted (with `^C`) and restarted to run in background
(it's the default with no `-d` / `-f` options).

### Quick Installation

If you want to do a quick test here's the installation procedure depending on the OS flavor (Linux or Mac):

* Create an S3 bucket in the AWS region you prefer.
* You don't need to create anything in the bucket as the initial path (if any) is created by the tool on the first mount.
* If you want to use an existing S3 bucket you can use the `--no-metadata` option to not use user metadata to persist file system attr/xattr.
* Create an SNS topic in the same region as the S3 bucket and write down the full topic ARN (you need it to run the tool if more than one client is connected to the same bucket/path).
* Create a IAM Role that gives access to the S3 and SNS/SQS resources you need or pass the AWS credentials to the tool using environmental variables (see `-h`).
* I used the `eu-west-1` region in my sample, but you can replace that with any region you want. If no region is specified it defaults to `us-east-1`.

**On EC2 with Amazon Linux 2012.09**

    sudo yum -y install fuse fuse-libs
    sudo easy_install pip
    sudo pip install fusepy
    git clone git://github.com/danilop/yas3fs.git
    cd yas3fs
    chmod u+x yas3fs.py
    ./yas3fs.py -h # See the usage
    sudo vi /etc/fuse.conf # uncomment user_allow_other
    mkdir LOCAL-PATH
    ./yas3fs.py LOCAL-PATH --url=s3://BUCKET/PATH --topic TOPIC-ARN --new-queue --region eu-west-1

**On EC2 with Ubuntu Server 12.04.1 LTS**

    sudo aptitude install fuse-utils libfuse2 python-pip
    sudo pip install --upgrade boto fusepy
    git clone git://github.com/danilop/yas3fs.git
    cd yas3fs
    chmod u+x yas3fs.py
    ./yas3fs.py -h # See the usage
    sudo vi /etc/fuse.conf  # uncomment user_allow_other
    sudo chmod a+r /etc/fuse.conf # make it readable by anybody, it is not the default on Ubuntu
    mkdir LOCAL-PATH
    ./yas3fs.py LOCAL-PATH --url=s3://BUCKET/PATH --topic TOPIC-ARN --new-queue --region eu-west-1

**On a Mac (tested under Mountain Lion)**

Install FUSE for OS X from <http://osxfuse.github.com>

To install the Python [M2Crypto](http://chandlerproject.org/Projects/MeTooCrypto) module,
download the most suitable "egg" from
<http://chandlerproject.org/Projects/MeTooCrypto#Downloads>.

    sudo easy_install M2Crypto-*.egg
    sudo easy_install boto
    sudo easy_install fusepy
    git clone git://github.com/danilop/yas3fs.git
    cd yas3fs
    chmod u+x yas3fs.py
    ./yas3fs.py -h # See the usage
    mkdir LOCAL-PATH
    ./yas3fs.py LOCAL-PATH --url=s3://BUCKET/PATH --topic TOPIC-ARN --new-queue --region eu-west-1

If something does not work as expected you can use the `-d` option to run in foreground in debug mode.

**Unmount**

To unmount the file system on Linux:

    fusermount -u LOCAL-PATH

To unmount the file system on a Mac you can use `umount`.

### Full Usage

    yas3fs.py -h
    
    Usage: yas3fs.py <mountpoint> [options]

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
      --prefetch         start downloading file content as soon as the file is
                         discovered
      --id=ID            a unique ID identifying this node in a cluster (hostname,
                         queue name or UUID Version 1 as per RFC 4122 are used if
                         not provided)
      --log=FILE         the filename to use for logs
      --mkdir            create mountpoint if not found (create intermediate
                         directories as required)
      -f, --foreground   run in foreground
      -d, --debug        print debug information (implies '-f')

### Notification Syntax & Use

You can use the SNS topic for other purposes than keeping the cache of the nodes in sync.
Those are some sample use cases:

* You can listen to the SNS topic to be updated on changes on S3 (if done through yas3fs).
* You can publish on the SNS topic to manage the overall "cluster" of yas3fs nodes.

The SNS notification syntax is based on [JSON (JavaScript Object Notation)](http://www.json.org):

    [ "node_id", "action", ... ]

The following `action`(s) are currently implemented:

* `mkdir` (new directory): `[ "node_id", "mkdir", "path" ]`
* `rmdir` (remove directory): `[ "node_id", "rmdir", "path" ]`
* `mknod` (new empty file): `[ "node_id", "mknod", "path" ]`
* `unlink` (remove file): `[ "node_id", "unlink", "path" ]`
* `symlink` (new symbolic link): `[ "node_id", "symlink", "path" ]`
* `rename` (rename file or directory): `[ "node_id", "rename", "old_path", "new_path" ]`
* `flush` (updated file): `[ "node_id", "flush", "path", "new_md5" ]` (`path` and `new_md5` are optional)
* `md` (updated metadata, e.g. attr/xattr): `[ "node_id", "md", "path", "metadata_name" ]`
* `reset` (reset cache): `[ "node_id", "reset" ]`
* `cache` (change cache config): `[ "node_id", "cache" , "entries" or "size", new_value ]`
* `buffer` (change buffer config): `[ "node_id", "buffer", "size", new_value ]`
* `prefetch` (change prefetch config): `[ "node_id", "prefetch", "on" or "off" ]`
* `url` (change S3 url): `[ "node_id", "url", "s3://BUCKET/PATH" ]`

Every node will listen to notifications coming from a `node_id` different from its own id.
As an example, if you want to reset the cache of all the nodes in a yas3fs cluster,
you can send the following notification to the SNS topic (assuming there is no node with id equal to `all`):

    [ "all", "reset" ]

To send the notification tou can use the SNS web console.

In the same way, if you uploaded a new file (or updated an old one) directly on S3 
you can invalidate the caches of all the nodes in the yas3fs cluster for that `path` sending this SNS notification:

    [ "all", "flush", "path" ]

The `path` is the relative path of the file system (`/` corresponding to the mount point)
and doesn't include any S3 path (i.e. prefix) as given in the `--url` option.

To change the size of the cache on all nodes, e.g. to bring it from 1GB (the current default) to 10GB,
you can publish (the size is in MB as in the corresponding command line option):

    [ "all", "cache", "size", 10240 ]

To change the buffer size used to download the content (and make it available for reads) from the default of 10MB (optimized for a full download speed) to 256KB (optimized for a streaming service) you can use (the size is in KB, as in the corresponding command line option):

    [ "all", "buffer", "size", 256 ]

Similarly, to activate download prefetch on all nodes you can use:

    [ "all", "prefetch", "on" ]

You can even change dinamically the S3 URL (i.e. the bucket and/or the path prefix) that are mounted:

    [ "all", "url", "s3://BUCKET/PATH" ]

Happy File Sharing!
