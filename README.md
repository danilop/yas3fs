### Yet Another S3-backed File System: yas3fs

YAS3FS (Yet Another S3-backed File System) is a [Filesystem in Userspace (FUSE)](http://fuse.sourceforge.net)
interface to [Amazon S3](http://aws.amazon.com/s3/).
It was inspired by [s3fs](http://code.google.com/p/s3fs/) but rewritten from scratch to implement
a distributed cache synchronized by [Amazon SNS](http://aws.amazon.com/sns/) notifications.
A web console is provided to easily monitor the nodes of a cluster through the [YAS3FS Console](https://github.com/danilop/yas3fs-console) project.

**If you use YAS3FS please share your experience on the [wiki](https://github.com/danilop/yas3fs/wiki), thanks!**

* It allows to mount an S3 bucket (or a part of it, if you specify a path) as a local folder.
* It works on Linux and Mac OS X.
* For maximum speed all data read from S3 is cached locally on the node, in memory or on disk, depending of the file size.
* Parallel multi-part downloads are used if there are reads in the middle of the file (e.g. for streaming).
* Parallel multi-part uploads are used for files larger than a specified size.
* With buffering enabled (the default) files can be accessed during the download from S3 (e.g. for streaming).
* It can be used on more than one node to create a "shared" file system (i.e. a yas3fs "cluster").
* [SNS](http://aws.amazon.com/sns/) notifications are used to update other nodes in the cluster that something has changed on S3 and they need to invalidate their cache.
* Notifications can be listened using HTTP or [SQS](http://aws.amazon.com/sqs/) endpoints.
* If the cache grows to its maximum size, the less recently accessed files are removed.
* AWS credentials can be passed using AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY environment variables.
* In an [EC2](http://aws.amazon.com/ec2/) instance a [IAM](http://aws.amazon.com/iam/) role can be used to give access to S3/SNS/SQS resources.
* It is written in Python (2.6) using [boto](https://github.com/boto/boto) and [fusepy](https://github.com/terencehonles/fusepy).

This is a personal project. No relation whatsoever exists between this project and my employer.

### License

Copyright (c) 2012-2013 Danilo Poccia, http://blog.danilopoccia.net

This code is licensed under the The MIT License (MIT). Please see the LICENSE file that accompanies this project for the terms of use.

### Introduction

This is the logical architecture of yas3fs:

![yas3fs Logical Architecture](http://blog.danilopoccia.net/wp-content/uploads/sites/2/2012/11/yas3fs.png.scaled500.png)

I strongly suggest to start yas3fs for the first time with the `-df` (debug + foreground) options, to see if there is any error.
When everything works it can be interrupted (with `^C`) and restarted to run in background
(it's the default with no `-f` options).

To mount an S3 bucket without using SNS (i.e. for a single node):

    yas3fs s3://bucket/path /path/to/mount

To persist file system metadata such as attr/xattr yas3fs is using S3 User Metadata.
To mount an S3 bucket without actually writing metadata in it,
e.g. because it is a bucket you mainly use as a repository and not as a file system,
you can use the `--no-metadata` option.

To mount an S3 bucket using SNS and listening to an SQS endpoint:

    yas3fs s3://bucket/path /path/to/mount --topic TOPIC-ARN --new-queue

To mount an S3 bucket using SNS and listening to an HTTP endpoint (on EC2):

    yas3fs s3://bucket/path /path/to/mount --url --topic TOPIC-ARN --ec2-hostname --port N

On EC2 the security group must allow inbound traffic from SNS on the selected port.

On EC2 the command line doesn't need any information on the actual server and can easily be used
within an [Auto Scaling](http://aws.amazon.com/autoscaling/) group.

### Quick Installation

Requires [Python](http://www.python.org/download/) 2.6 or higher.
Install using [pip](http://www.pip-installer.org/en/latest/).

    pip install yas3fs

If you want to do a quick test here's the installation procedure depending on the OS flavor (Linux or Mac):

* Create an S3 bucket in the AWS region you prefer.
* You don't need to create anything in the bucket as the initial path (if any) is created by the tool on the first mount.
* If you want to use an existing S3 bucket you can use the `--no-metadata` option to not use user metadata to persist file system attr/xattr.
* If you want to have more than one node in sync, create an SNS topic in the same region as the S3 bucket and write down the full topic ARN (you need it to run the tool if more than one client is connected to the same bucket/path).
* Create a IAM Role that gives access to the S3 and SNS/SQS resources you need or pass the AWS credentials to the tool using environment variables (see `-h`).

**On Amazon Linux**

    sudo yum -y install fuse fuse-libs
    sudo easy_install pip
    sudo pip install -U setuptools # need updated version
    sudo pip install yas3fs # assume root installation
    sudo sed -i'' 's/^# *user_allow_other/user_allow_other/' /etc/fuse.conf # uncomment user_allow_other
    yas3fs -h # See the usage
    mkdir LOCAL-PATH
    # For single host mount
    yas3fs s3://BUCKET/PATH LOCAL-PATH
    # For multiple hosts mount
    yas3fs s3://BUCKET/PATH LOCAL-PATH --topic TOPIC-ARN --new-queue

**On Ubuntu Linux**

    sudo apt-get upgrade
    sudo apt-get install fuse python-pip 
    sudo pip install yas3fs # assume root installation
    sudo sed -i'' 's/^# *user_allow_other/user_allow_other/' /etc/fuse.conf # uncomment user_allow_other
    sudo chmod a+r /etc/fuse.conf # make it readable by anybody, it is not the default on Ubuntu
    yas3fs -h # See the usage
    mkdir LOCAL-PATH
    # For single host mount
    yas3fs s3://BUCKET/PATH LOCAL-PATH
    # For multiple hosts mount
    yas3fs s3://BUCKET/PATH LOCAL-PATH --topic TOPIC-ARN --new-queue

**On a Mac with OS X**

Install FUSE for OS X from <http://osxfuse.github.com>.

    sudo pip install yas3fs # assume root installation
    mkdir LOCAL-PATH
    # For single host mount
    yas3fs LOCAL-PATH --url s3://BUCKET/PATH
    # For multiple hosts mount
    yas3fs LOCAL-PATH --url s3://BUCKET/PATH --topic TOPIC-ARN --new-queue

To listen to SNS HTTP notifications (I usually suggest to use SQS instead) with a Mac
you need to install the Python [M2Crypto](http://chandlerproject.org/Projects/MeTooCrypto) module,
download the most suitable "egg" from
<http://chandlerproject.org/Projects/MeTooCrypto#Downloads>.

    sudo easy_install M2Crypto-*.egg

If something does not work as expected you can use the `-df` options to run in foreground and in debug mode.

**Unmount**

To unmount the file system on Linux:

    fusermount -u LOCAL-PATH

To unmount the file system on a Mac you can use `umount`.

### Full Usage

    yas3fs -h

    usage: yas3fs [-h] [--region REGION] [--topic ARN] [--new-queue]
                  [--queue NAME] [--queue-wait N] [--queue-polling N]
                  [--hostname HOSTNAME] [--use-ec2-hostname] [--port N]
                  [--cache-entries N] [--cache-mem-size N] [--cache-disk-size N]
                  [--cache-path PATH] [--cache-on-disk N] [--cache-check N]
                  [--download-num N] [--prefetch-num N] [--buffer-size N]
                  [--buffer-prefetch N] [--no-metadata] [--prefetch] [--mp-size N]
                  [--mp-num N] [--mp-retries N] [--id ID] [--mkdir MKDIR]
                  [--uid N] [--gid N] [--umask MASK] [-l FILE] [-f] [-d]
                  S3Path LocalPath

    YAS3FS (Yet Another S3-backed File System) is a Filesystem in Userspace (FUSE)
    interface to Amazon S3. It allows to mount an S3 bucket (or a part of it, if
    you specify a path) as a local folder. It works on Linux and Mac OS X. For
    maximum speed all data read from S3 is cached locally on the node, in memory
    or on disk, depending of the file size. Parallel multi-part downloads are used
    if there are reads in the middle of the file (e.g. for streaming). Parallel
    multi-part uploads are used for files larger than a specified size. With
    buffering enabled (the default) files can be accessed during the download from
    S3 (e.g. for streaming). It can be used on more than one node to create a
    "shared" file system (i.e. a yas3fs "cluster"). SNS notifications are used to
    update other nodes in the cluster that something has changed on S3 and they
    need to invalidate their cache. Notifications can be delivered to HTTP or SQS
    endpoints. If the cache grows to its maximum size, the less recently accessed
    files are removed. AWS credentials can be passed using AWS_ACCESS_KEY_ID and
    AWS_SECRET_ACCESS_KEY environment variables. In an EC2 instance a IAM role can
    be used to give access to S3/SNS/SQS resources. AWS_DEFAULT_REGION environment
    variable can be used to set the default AWS region.

    positional arguments:
      S3Path               the S3 path to mount in s3://BUCKET/PATH format, PATH
                           can be empty, can contain subfolders and is created on
                           first mount if not found in the BUCKET
      LocalPath            the local mount point

    optional arguments:
      -h, --help           show this help message and exit
      --region REGION      AWS region to use for SNS and SQS (default is us-
                           east-1)
      --topic ARN          SNS topic ARN
      --new-queue          create a new SQS queue that is deleted on unmount to
                           listen to SNS notifications, overrides --queue, queue
                           name is BUCKET-PATH-ID with alphanumeric characters
                           only
      --queue NAME         SQS queue name to listen to SNS notifications, a new
                           queue is created if it doesn't exist
      --queue-wait N       SQS queue wait time in seconds (using long polling, 0
                           to disable, default is 20 seconds)
      --queue-polling N    SQS queue polling interval in seconds (default is 0
                           seconds)
      --hostname HOSTNAME  public hostname to listen to SNS HTTP notifications
      --use-ec2-hostname   get public hostname to listen to SNS HTTP notifications
                           from EC2 instance metadata (overrides --hostname)
      --port N             TCP port to listen to SNS HTTP notifications
      --cache-entries N    max number of entries to cache (default is 100000
                           entries)
      --cache-mem-size N   max size of the memory cache in MB (default is 128 MB)
      --cache-disk-size N  max size of the disk cache in MB (default is 1024 MB)
      --cache-path PATH    local path to use for disk cache (default is
                           /tmp/yas3fs/BUCKET/PATH)
      --cache-on-disk N    use disk (instead of memory) cache for files greater
                           than the given size in bytes (default is 0 bytes)
      --cache-check N      interval between cache size checks in seconds (default
                           is 5 seconds)
      --download-num N     number of parallel downloads (default is 4)
      --prefetch-num N     number of parallel prefetching downloads (default is 2)
      --buffer-size N      download buffer size in KB (0 to disable buffering,
                           default is 10240 KB)
      --buffer-prefetch N  number of buffers to prefetch (default is 0)
      --no-metadata        don't write user metadata on S3 to persist file system
                           attr/xattr
      --prefetch           download file/directory content as soon as the file is
                           discovered(doesn't download file content if download
                           buffers are used)
      --mp-size N          size of parts to use for multipart upload in MB
                           (default value is 100 MB, the minimum allowed by S3 is
                           5 MB)
      --mp-num N           max number of parallel multipart uploads per file (0 to
                           disable multipart upload, default is 4)
      --mp-retries N       max number of retries in uploading a part (default is
                           3)
      --id ID              a unique ID identifying this node in a cluster (default
                           is a UUID)
      --mkdir MKDIR        create mountpoint if not found (and create intermediate
                           directories as required)
      --uid N              default UID
      --gid N              default GID
      --umask MASK         default umask
      -l FILE, --log FILE  filename for logs
      -f, --foreground     run in foreground
      -d, --debug          show debug info
      -V, --version        show program's version number and exit

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
* `upload` (new or updated file): `[ "node_id", "upload", "path", "new_md5" ]` (`path` and `new_md5` are optional)
* `md` (updated metadata, e.g. attr/xattr): `[ "node_id", "md", "path", "metadata_name" ]`
* `reset` (reset cache): `[ "node_id", "reset" ]`
* `cache` (change cache config): `[ "node_id", "cache" , "entries" or "mem" or "disk", new_value ]`
* `buffer` (change buffer config): `[ "node_id", "buffer", "size" or "prefetch", new_value ]`
* `prefetch` (change prefetch config): `[ "node_id", "prefetch", "on" or "off" ]`
* `url` (change S3 url): `[ "node_id", "url", "s3://BUCKET/PATH" ]`

Every node will listen to notifications coming from a `node_id` different from its own id.
As an example, if you want to reset the cache of all the nodes in a yas3fs cluster,
you can send the following notification to the SNS topic (assuming there is no node with id equal to `all`):

    [ "all", "reset" ]

To send the notification you can use the SNS web console or any command line tool that supports SNS, such as [AWS CLI](http://aws.amazon.com/cli/).

In the same way, if you uploaded a new file (or updated an old one) directly on S3 
you can invalidate the caches of all the nodes in the yas3fs cluster for that `path` sending this SNS notification:

    [ "all", "upload", "path" ]

The `path` is the relative path of the file system (`/` corresponding to the mount point)
and doesn't include any S3 path (i.e. prefix) as given in the `--url` option.

To change the size of the memory cache on all nodes, e.g. to bring it from 1GB (the current default) to 10GB,
you can publish (the size is in MB as in the corresponding command line option):

    [ "all", "cache", "mem", 10240 ]

To change the size of the disk cache on all nodes, e.g. to bring it from 10GB (the current default) to 1TB,
you can publish (the size is in MB as in the corresponding command line option):

    [ "all", "cache", "disk", 1048576 ]

To change the buffer size used to download the content (and make it available for reads) from the default of 10MB (optimized for a full download speed) to 256KB (optimized for a streaming service) you can use (the size is in KB, as in the corresponding command line option):

    [ "all", "buffer", "size", 256 ]

To change buffer prefetch from the default of 0 to 1 (optimized for sequential access) you can publish:

    [ "all", "buffer", "prefetch", 1 ]

Similarly, to activate download prefetch of all files on all nodes you can use:

    [ "all", "prefetch", "on" ]

To change the multipart upload size to 100MB:

    [ "all", "multipart", "size", 102400 ]

To change the maximum number of parallel threads to use for multipart uploads to 16:

    [ "all", "multipart", "num", 16 ]

To change the maximum number of retries for multipart uploads to 10:

    [ "all", "multipart", "retries", 10 ]

You can even change dinamically the mounted S3 URL (i.e. the bucket and/or the path prefix):

    [ "all", "url", "s3://BUCKET/PATH" ]

To check the status of all the yas3fs instances listening to a topic you can use:

    [ "all", "ping" ]

To the previous message all yas3fs instances will answer publishing a message on the topic with this content:

    [ "id", "status", hostname, number of entries in cache, cache memory size,
      cache disk size, download queue length, prefetch queue length ]

Happy File Sharing!
