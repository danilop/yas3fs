#!/usr/bin/env python
from __future__ import print_function

"""
Yet Another S3-backed File System, or yas3fs
is a FUSE file system that is designed for speed
caching data locally and using SNS to notify
other nodes for changes that need cache invalidation.
"""

import argparse
import logging
import os
import os.path
import pprint
import sys
import time

from .fuse import FUSE
import utils

from ._version import __version__
from .YAS3FS import YAS3FS


class ISO8601Formatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        if datefmt:
            return super(ISO8601Formatter, self).formatTime(record, datefmt)

        ct = self.converter(record.created)
        return "%s.%03d" % (time.strftime("%Y-%m-%dT%H:%M:%S", ct), record.msecs)


class CompressedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """ compress old files
    from http://roadtodistributed.blogspot.com/2011/04/compressed-rotatingfilehandler-for.html
    """
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0):
        logging.handlers.RotatingFileHandler.__init__(
            self, filename, mode, maxBytes, backupCount, encoding, delay)

    def doRollover(self):
        self.stream.close()
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = "%s.%d.gz" % (self.baseFilename, i)
                dfn = "%s.%d.gz" % (self.baseFilename, i + 1)
                if os.path.exists(sfn):
                    # print "%s -> %s" % (sfn, dfn)
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            dfn = self.baseFilename + ".1.gz"
            if os.path.exists(dfn):
                os.remove(dfn)
            import gzip
            try:
                f_in = open(self.baseFilename, 'rb')
                f_out = gzip.open(dfn, 'wb')
                f_out.writelines(f_in)
            except:
                if not os.path.exists(dfn):
                    if os.path.exists(self.baseFilename):
                        os.rename(self.baseFilename, dfn)
            finally:
                if "f_out" in dir() and f_out is not None:
                    f_out.close()
                if "f_in" in dir() and f_in is not None:
                    f_in.close()
            if os.path.exists(self.baseFilename):
                os.remove(self.baseFilename)
            # os.rename(self.baseFilename, dfn)
            # print "%s -> %s" % (self.baseFilename, dfn)
        self.mode = 'w'
        self.stream = self._open()


# CLI Parser
def cli_parser(cli):
    try:
        default_aws_region = os.environ['AWS_DEFAULT_REGION']
    except KeyError:
        default_aws_region = 'us-east-1'

    description = """
YAS3FS (Yet Another S3-backed File System) is a Filesystem in Userspace (FUSE) interface to Amazon S3.
It allows to mount an S3 bucket (or a part of it, if you specify a path) as a local folder.
It works on Linux and Mac OS X.
For maximum speed all data read from S3 is cached locally on the node, in memory or on disk, depending of the file size.
Parallel multi-part downloads are used if there are reads in the middle of the file (e.g. for streaming).
Parallel multi-part uploads are used for files larger than a specified size.
With buffering enabled (the default) files can be accessed during the download from S3 (e.g. for streaming).
It can be used on more than one node to create a "shared" file system (i.e. a yas3fs "cluster").
SNS notifications are used to update other nodes in the cluster that something has changed on S3 and they need to invalidate their cache.
Notifications can be delivered to HTTP or SQS endpoints.
If the cache grows to its maximum size, the less recently accessed files are removed.
Signed URLs are provided through Extended file attributes (xattr).
AWS credentials can be passed using AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.
In an EC2 instance a IAM role can be used to give access to S3/SNS/SQS resources.
AWS_DEFAULT_REGION environment variable can be used to set the default AWS region."""

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('s3path', metavar='S3Path',
                        help='the S3 path to mount in s3://BUCKET/PATH format, ' +
                        'PATH can be empty, can contain subfolders and is created on first mount if not found in the BUCKET')
    parser.add_argument('mountpoint', metavar='LocalPath',
                        help='the local mount point')
    parser.add_argument('--region', default=default_aws_region,
                        help='AWS region to use for SNS and SQS (default is %(default)s)')
    parser.add_argument('--topic', metavar='ARN',
                        help='SNS topic ARN')
    parser.add_argument('--new-queue', action='store_true',
                        help='create a new SQS queue that is deleted on unmount to listen to SNS notifications, ' +
                        'overrides --queue, queue name is BUCKET-PATH-ID with alphanumeric characters only')
    parser.add_argument('--new-queue-with-hostname', action='store_true',
                        help='create a new SQS queue with hostname in queuename, ' +
                        'overrides --queue, queue name is BUCKET-PATH-ID with alphanumeric characters only')
    parser.add_argument('--queue', metavar='NAME',
                        help='SQS queue name to listen to SNS notifications, a new queue is created if it doesn\'t exist')
    parser.add_argument('--queue-wait', metavar='N', type=int, default=20,
                        help='SQS queue wait time in seconds (using long polling, 0 to disable, default is %(default)s seconds)')
    parser.add_argument('--queue-polling', metavar='N', type=int, default=0,
                        help='SQS queue polling interval in seconds (default is %(default)s seconds)')
    parser.add_argument('--hostname',
                        help='public hostname to listen to SNS HTTP notifications')
    parser.add_argument('--use-ec2-hostname', action='store_true',
                        help='get public hostname to listen to SNS HTTP notifications ' +
                        'from EC2 instance metadata (overrides --hostname)')
    parser.add_argument('--port', metavar='N',
                        help='TCP port to listen to SNS HTTP notifications')
    parser.add_argument('--cache-entries', metavar='N', type=int, default=100000,
                        help='max number of entries to cache (default is %(default)s entries)')
    parser.add_argument('--cache-mem-size', metavar='N', type=int, default=128,
                        help='max size of the memory cache in MB (default is %(default)s MB)')
    parser.add_argument('--cache-disk-size', metavar='N', type=int, default=1024,
                        help='max size of the disk cache in MB (default is %(default)s MB)')
    parser.add_argument('--cache-path', metavar='PATH', default='',
                        help='local path to use for disk cache (default is /tmp/yas3fs/BUCKET/PATH)')
    parser.add_argument('--recheck-s3', action='store_true',
                        help='Cached ENOENT (error no entry) rechecks S3 for new file/directory')
    parser.add_argument('--cache-on-disk', metavar='N', type=int, default=0,
                        help='use disk (instead of memory) cache for files greater than the given size in bytes ' +
                        '(default is %(default)s bytes)')
    parser.add_argument('--cache-check', metavar='N', type=int, default=5,
                        help='interval between cache size checks in seconds (default is %(default)s seconds)')
    parser.add_argument('--s3-num', metavar='N', type=int, default=32,
                        help='number of parallel S3 calls (0 to disable writeback, default is %(default)s)')
    parser.add_argument('--s3-retries', metavar='N', type=int, default=3,
                        help='number of of times to retry any s3 write operation (default is %(default)s)')
    parser.add_argument('--s3-retries-sleep', metavar='N', type=int, default=1,
                        help='retry sleep in seconds between s3 write operations (default is %(default)s)')
    parser.add_argument('--s3-use-sigv4', action='store_true',
                        help='use AWS signature version 4 for authentication (required for some regions)')
    parser.add_argument('--s3-endpoint',
                        help='endpoint of the s3 bucket, required with --s3-use-sigv4')
    parser.add_argument('--download-num', metavar='N', type=int, default=4,
                        help='number of parallel downloads (default is %(default)s)')
    parser.add_argument('--download-retries-num', metavar='N', type=int, default=60,
                        help='max number of retries when downloading (default is %(default)s)')
    parser.add_argument('--download-retries-sleep', metavar='N', type=int, default=1,
                        help='how long to sleep in seconds between download retries (default is %(default)s seconds)')
    parser.add_argument('--read-retries-num', metavar='N', type=int, default=10,
                        help='max number of retries when read() is invoked (default is %(default)s)')
    parser.add_argument('--read-retries-sleep', metavar='N', type=int, default=1,
                        help='how long to sleep in seconds between read() retries (default is %(default)s seconds)')
    parser.add_argument('--prefetch-num', metavar='N', type=int, default=2,
                        help='number of parallel prefetching downloads (default is %(default)s)')
    parser.add_argument('--st-blksize', metavar='N', type=int, default=None,
                        help='st_blksize to return to getattr() callers in bytes, optional')
    parser.add_argument('--buffer-size', metavar='N', type=int, default=10240,
                        help='download buffer size in KB (0 to disable buffering, default is %(default)s KB)')
    parser.add_argument('--buffer-prefetch', metavar='N', type=int, default=0,
                        help='number of buffers to prefetch (default is %(default)s)')
    parser.add_argument('--no-metadata', action='store_true',
                        help='don\'t write user metadata on S3 to persist file system attr/xattr')
    parser.add_argument('--prefetch', action='store_true',
                        help='download file/directory content as soon as it is discovered ' +
                        '(doesn\'t download file content if download buffers are used)')
    parser.add_argument('--mp-size', metavar='N', type=int, default=100,
                        help='size of parts to use for multipart upload in MB ' +
                        '(default value is %(default)s MB, the minimum allowed by S3 is 5 MB)')
    parser.add_argument('--mp-num', metavar='N', type=int, default=4,
                        help='max number of parallel multipart uploads per file ' +
                        '(0 to disable multipart upload, default is %(default)s)')
    parser.add_argument('--mp-retries', metavar='N', type=int, default=3,
                        help='max number of retries in uploading a part (default is %(default)s)')
    parser.add_argument('--aws-managed-encryption', action='store_true',
                        help='Enable AWS managed encryption (sets header x-amz-server-side-encryption = AES256)')
    parser.add_argument('--id',
                        help='a unique ID identifying this node in a cluster (default is a UUID)')
    parser.add_argument('--mkdir', action='store_true',
                        help='create mountpoint if not found (and create intermediate directories as required)')
    parser.add_argument('--nonempty', action='store_true',
                        help='allows mounts over a non-empty file or directory')
    parser.add_argument('--uid', metavar='N',
                        help='default UID')
    parser.add_argument('--gid', metavar='N',
                        help='default GID')
    parser.add_argument('--umask', metavar='MASK',
                        help='default umask')
    parser.add_argument('--read-only', action='store_true',
                        help='mount read only')
    parser.add_argument('--expiration', metavar='N', type=int, default=30*24*60*60,
                        help='default expiration for signed URL via xattrs (in seconds, default is 30 days)')
    parser.add_argument('--requester-pays', action='store_true',
                        help='requester pays for S3 interactions, the bucket must have Requester Pays enabled')
    parser.add_argument('--no-allow-other', action='store_true',
                        help='Do not allow other users to access mounted directory')
    parser.add_argument('--with-plugin-file', metavar='FILE',
                        help="YAS3FSPlugin file")
    parser.add_argument('--with-plugin-class', metavar='CLASS',
                        help="YAS3FSPlugin class, if this is not set it will take the first child of YAS3FSPlugin from exception handler file")

    parser.add_argument('-l', '--log', metavar='FILE',
                        help='filename for logs')
    parser.add_argument('--log-mb-size', metavar='N', type=int, default=100,
                        help='max size of log file')
    parser.add_argument('--log-backup-count', metavar='N', type=int, default=10,
                        help='number of backups log files')
    parser.add_argument('--log-backup-gzip', action='store_true',
                        help='flag to gzip backup files')
    parser.add_argument('-f', '--foreground', action='store_true',
                        help='run in foreground')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='show debug info')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s {version}'.format(version=__version__))

    return parser.parse_args(cli)


# Main
def main():
    options = cli_parser(sys.argv[1:])

    global pp
    pp = pprint.PrettyPrinter(indent=1)

    global logger
    logger = logging.getLogger('yas3fs')
    formatter = ISO8601Formatter('%(threadName)s %(asctime)s %(levelname)s %(message)s')
    if options.log:  # Rotate log files at 100MB size
        log_size = options.log_mb_size * 1024 * 1024
        if options.log_backup_gzip:
            logHandler = CompressedRotatingFileHandler(options.log, maxBytes=log_size, backupCount=options.log_backup_count)
        else:
            logHandler = logging.handlers.RotatingFileHandler(options.log, maxBytes=log_size, backupCount=options.log_backup_count)

        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
    if options.foreground or not options.log:
        logHandler = logging.StreamHandler()
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)

    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    sys.excepthook = utils.custom_sys_excepthook  # This is not working for new threads that start afterwards

    logger.debug("options = %s" % options)

    if options.mkdir:
        utils.create_dirs(options.mountpoint)

    mount_options = {
        'mountpoint': options.mountpoint,
        'fsname': 'yas3fs',
        'foreground': options.foreground,
        'allow_other': True,
        'auto_cache': True,
        'atime': False,
        'max_read': 131072,
        'max_write': 131072,
        'max_readahead': 131072,
        'direct_io': True
        }
    if options.no_allow_other:
        mount_options["allow_other"] = False
    if options.uid:
        mount_options['uid'] = options.uid
    if options.gid:
        mount_options['gid'] = options.gid
    if options.umask:
        mount_options['umask'] = options.umask
    if options.read_only:
        mount_options['ro'] = True

    if options.nonempty:
        mount_options['nonempty'] = True

    options.darwin = (sys.platform == "darwin")
    if options.darwin:
        mount_options['volname'] = os.path.basename(options.mountpoint)
        mount_options['noappledouble'] = True
        mount_options['daemon_timeout'] = 3600
        # mount_options['auto_xattr'] = True # To use xattr
        # mount_options['local'] = True # local option is quite unstable
    else:
        mount_options['big_writes'] = True  # Not working on OSX

    fuse = FUSE(YAS3FS(options), **mount_options)


if __name__ == '__main__':
    main()
