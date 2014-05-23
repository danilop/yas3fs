#!/usr/bin/env python

"""
Yet Another S3-backed File System, or yas3fs
is a FUSE file system that is designed for speed
caching data locally and using SNS to notify
other nodes for changes that need cache invalidation.
"""

import argparse
import errno  
import stat  
import time
import os
import os.path
import mimetypes
import sys
import json
import urlparse
import threading
import Queue
import socket
import BaseHTTPServer
import urllib2
import itertools
import base64
import logging
import signal
import io
import re
import uuid
import copy
import traceback
import datetime as dt
import gc # For debug only

import boto
import boto.s3        
import boto.sns
import boto.sqs
import boto.utils

from sys import exit

from boto.s3.key import Key 

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
from _version import __version__

class Interval():
    """ Simple integer interval arthmetic."""
    def __init__(self):
        self.l = [] # A list of tuples
    def add(self, t):
        assert t[0] <= t[1]
        nl = []
        for i in self.l:
            i0 = i[0] - 1 # To take into account consecutive _integer_ intervals
            i1 = i[1] + 1 # Same as above
            if (i0 <= t[0] and t[0] <= i1) or (i0 <= t[1] and t[1]<= i1) or (t[0] <= i[0] and i[1] <= t[1]):
                t[0] = min(i[0], t[0]) # Enlarge t interval
                t[1] = max(i[1], t[1])
            else:
                nl.append(i)
        nl.append(t)
        self.l = nl
    def contains(self, t):
        assert t[0] <= t[1]
        for i in self.l:
            if (i[0] <= t[0] and t[1] <= i[1]):
                return True
        return False
    def intersects(self, t):
        assert t[0] <= t[1]
        for i in self.l:
            if (i[0] <= t[0] and t[0] <= i[1]) or (i[0] <= t[1] and t[1]<= i[1]) or (t[0] <= i[0] and i[1] <= t[1]):
                return True
        return False

class LinkedListElement():
    """ The element of a linked list."""
    def __init__(self, value, next=None):
        self.value = value
        if next:
            self.append(next)
        else:
            self.next = None
            self.prev = None
    def delete(self):
        self.prev.next = self.next
        self.next.prev = self.prev
        return self.value
    def append(self, next):
        self.prev = next.prev
        self.next = next
        next.prev.next = self
        next.prev = self

class LinkedList():
    """ A linked list that is used by yas3fs as a LRU index
    for the file system cache."""
    def __init__(self):
        self.tail = LinkedListElement(None)
        self.head = LinkedListElement(None)
        self.head.next = self.tail
        self.tail.prev = self.head
        self.index = {}
        self.lock = threading.RLock()
    def append(self, value):
        with self.lock:
            if value not in self.index:
                new = LinkedListElement(value, self.tail)
                self.index[value] = new
    def popleft(self):
        with self.lock:
            if self.head.next != self.tail:
        	value = self.head.next.delete()
        	del self.index[value]
        	return value
            else:
        	return None
    def delete(self, value):
        with self.lock:
            if value in self.index:
        	self.index[value].delete()
        	del self.index[value]
    def move_to_the_tail(self, value):
        with self.lock:
            if value in self.index:
                old = self.index[value]
                old.delete()
                old.append(self.tail)

class FSRange():
    """A range used to manage buffered downloads from S3."""
    io_wait = 3.0 # 3 seconds
    def __init__(self):
        self.interval = Interval()
        self.ongoing_intervals = {}
        self.event = threading.Event()
        self.lock = threading.RLock()
    def wait(self):
        self.event.wait(self.io_wait)
    def wake(self, again=True):
        with self.lock:
            e = self.event
            if again:
                self.event = threading.Event()
            e.set()

class FSData():
    """The data (content) associated with a file."""
    stores = [ 'mem', 'disk' ]
    unknown_store = "Unknown store"
    def __init__(self, cache, store, path):
        self.cache = cache
        self.store = store
        self.path = path
        self.props = {}
        self.size = 0
        self.etag = None # Something better ???
        if store == 'mem':
            self.content = io.BytesIO()
        elif store == 'disk':
            previous_file = False
            filename = self.cache.get_cache_filename(self.path)
            if os.path.isfile(filename):
                logger.debug("found previous cache file '%s'" % filename)
                # There's a file already there
                self.content = open(filename, mode='rb+')
                self.update_size()
                self.content.close()
                self.set('new', None) # Not sure it is the latest version
                # Now search for an etag file
                etag_filename = self.cache.get_cache_etags_filename(self.path)
                if os.path.isfile(etag_filename):
                    logger.debug("found previous cache etag file '%s'" % etag_filename)
                    with open(etag_filename, mode='r') as etag_file:
                        self.etag = etag_file.read()
                    previous_file = True
            if not previous_file:
                logger.debug("creating new cache file '%s'" % filename)
                with self.cache.disk_lock:
                    create_dirs_for_file(filename)
                    open(filename, mode='w').close() # To create an empty file (and overwrite a previous file)
                logger.debug("created new cache file '%s'" % filename)
            self.content = None # Not open, yet
        else:
            raise FSData.unknown_store
    def get_lock(self):
        return self.cache.get_lock(self.path)
    def open(self):
        with self.get_lock():
            if not self.has('open'):
                if self.store == 'disk':
                    filename = self.cache.get_cache_filename(self.path)
                    self.content = open(filename, mode='rb+')
            self.inc('open')
    def close(self):
        with self.get_lock():
            self.dec('open')
            if not self.has('open'):
                if self.store == 'disk':
                    self.content.close()
                    self.content = None
    def update_etag(self, new_etag):
        with self.get_lock():
            if new_etag != self.etag:
                self.etag = new_etag
                if self.store == 'disk':
                    filename = self.cache.get_cache_etags_filename(self.path)
                    with self.cache.disk_lock:
                        create_dirs_for_file(filename)
                        with open(filename, mode='w') as etag_file:
                            etag_file.write(new_etag)
    def get_current_size(self):
        if self.content:
            with self.get_lock():
                self.content.seek(0,2)
                return self.content.tell()
        else:
            return 0 # There's no content...
    def update_size(self, final=False):
        with self.get_lock():
            if final:
                current_size = 0 # The entry is to be deleted
            else:
                current_size = self.get_current_size()
            delta = current_size - self.size
            self.size = current_size
        with self.cache.data_size_lock:
            self.cache.size[self.store] += delta
    def get_content(self):
        with self.get_lock():
            if self.store == 'disk':
                filename = self.cache.get_cache_filename(self.path)
                return open(filename, mode='rb+')
            else:
                return self.content
    def get_content_as_string(self):
        if self.store == 'mem':
            with self.get_lock():
                return self.content.getvalue()
        elif self.store == 'disk':
            with self.get_lock():                
                self.content.seek(0) # Go to the beginning
                return self.content.read()
        else:
            raise FSData.unknown_store
    def has(self, prop):
        with self.get_lock():
            return prop in self.props
    def get(self, prop):
        with self.get_lock():
            try:
                return self.props[prop]
            except KeyError:
                return None
    def set(self, prop, value):
        with self.get_lock():
            self.props[prop] = value
    def inc(self, prop):
        with self.get_lock():
            try:
                self.props[prop] += 1
            except KeyError:
                self.props[prop] = 1
    def dec(self, prop):
        with self.get_lock():
            try:
                if self.props[prop] > 1:
                    self.props[prop] -= 1
                else:
                    del self.props[prop]
            except KeyError:
                pass # Nothing to do
    def delete(self, prop=None):
        with self.get_lock():
            if prop == None:
                if self.store == 'disk':
                    filename = self.cache.get_cache_filename(self.path)
                    with self.cache.disk_lock:
                        if os.path.isfile(filename):
                            logger.debug("unlink cache file '%s'" % filename)
                            os.unlink(filename)
                            remove_empty_dirs_for_file(filename)
                    etag_filename = self.cache.get_cache_etags_filename(self.path)
                    with self.cache.disk_lock:
                        if os.path.isfile(etag_filename):
                            logger.debug("unlink cache etag file '%s'" % etag_filename)
                            os.unlink(etag_filename)
                            remove_empty_dirs_for_file(etag_filename)
                self.content = None # If not
                self.update_size(True)
                for p in self.props.keys():
                    self.delete(p)
            elif prop in self.props:
                if prop == 'range':
                    logger.debug('there is a range to delete')
                    data_range = self.get(prop)
                else:
                    data_range = None
                del self.props[prop]
                if data_range:
                    logger.debug('wake after range delete')
                    data_range.wake(False) # To make downloading threads go on... and then exit
    def rename(self, new_path):
        with self.get_lock():
            if self.store == 'disk':
                filename = self.cache.get_cache_filename(self.path)
                new_filename = self.cache.get_cache_filename(new_path)
                etag_filename = self.cache.get_cache_etags_filename(self.path)
                new_etag_filename = self.cache.get_cache_etags_filename(new_path)
                with self.cache.disk_lock:
                    create_dirs_for_file(new_filename)
                    os.rename(filename, new_filename)
                with self.cache.disk_lock:
                    remove_empty_dirs_for_file(filename)
                if os.path.isfile(etag_filename):
                    with self.cache.disk_lock:
                        create_dirs_for_file(new_etag_filename)
                        os.rename(etag_filename, new_etag_filename)
                    with self.cache.disk_lock:
                        remove_empty_dirs_for_file(etag_filename)
                if self.content:
                    self.content = open(new_filename, mode='rb+')
            self.path = new_path

class FSCache():
    """ File System Cache """
    def __init__(self, cache_path=None):
        self.cache_path = cache_path
        self.lock = threading.RLock()
        self.disk_lock = threading.RLock() # To safely remove empty disk directories
        self.data_size_lock = threading.RLock()
        self.reset_all()
    def reset_all(self):
         with self.lock:
             self.entries = {}
             self.new_locks = {} # New locks (still) without entry in the cache
             self.unused_locks = {} # Paths with unused locks that will be removed on the next purge if remain unused
             self.lru = LinkedList()
             self.size = {}
             for store in FSData.stores:
                self.size[store] = 0
    def get_memory_usage(self):
        return [ len(self.entries) ] + [ self.size[store] for store in FSData.stores ]
    def get_cache_filename(self, path):
        return self.cache_path + '/files' + path # path begins with '/'
    def get_cache_etags_filename(self, path):
        return self.cache_path + '/etags' + path # path begins with '/'
    def get_lock(self, path):
        with self.lock: # Global cache lock, used only for giving file-level locks
            try:
                return self.entries[path]['lock']
            except KeyError:
                try:
                    return self.new_locks[path]
                except KeyError:
                    new_lock = threading.RLock()
                    self.new_locks[path] = new_lock
                    return new_lock;
    def add(self, path):
        with self.get_lock(path):
            if not path in self.entries:
                self.entries[path] = {}
                self.entries[path]['lock'] = self.new_locks[path]
                del self.new_locks[path]
                self.lru.append(path)
    def delete(self, path, prop=None):
        with self.get_lock(path):
            if path in self.entries:
                if prop == None:
                    for p in self.entries[path].keys():
                        self.delete(path, p)
                    del self.entries[path]
                    self.lru.delete(path)
                else:
                    if prop in self.entries[path]:
                        if prop == 'data':
                            data = self.entries[path][prop]
                            data.delete() # To clean stuff, e.g. remove cache files
                        elif prop == 'lock':
                            # Preserve lock, let the unused locks check remove it later
                            self.new_locks[path] = self.entries[path][prop]
                        del self.entries[path][prop]
    def rename(self, path, new_path):
        with self.get_lock(path) and self.get_lock(new_path):
            if path in self.entries:
                self.delete(path, 'key') # Cannot be renamed
                self.delete(new_path) # Assume overwrite
                if 'data' in self.entries[path]:
                    data = self.entries[path]['data']
                    with data.get_lock():
                        data.rename(new_path)
                self.entries[new_path] = self.entries[path]
                self.lru.append(new_path)
                self.lru.delete(path)
                del self.entries[path] # So that the next reset doesn't delete the entry props
    def get(self, path, prop=None):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        try:
            if prop == None:
                return self.entries[path]
            else:
                return self.entries[path][prop]
        except KeyError:
            return None
    def set(self, path, prop, value):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
        	if prop in self.entries[path]:
                    self.delete(path, prop)
        	self.entries[path][prop] = value
        	return True
            else:
        	return False
    def inc(self, path, prop):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
                try:
                    self.entries[path][prop] += 1
                except KeyError:
                    self.entries[path][prop] = 1
    def dec(self, path, prop):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
                try:
                    if self.entries[path][prop] > 1:
                        self.entries[path][prop] -= 1
                    else:
                        del self.entries[path][prop]
                except KeyError:
                    pass # Nothing to do
    def reset(self, path):
        with self.get_lock(path):
            self.delete(path)
            self.add(path)
    def has(self, path, prop=None):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        if prop == None:
            return path in self.entries
        else:
            try:
                return prop in self.entries[path]
            except KeyError:
                return False
    def is_empty(self, path): # To improve readability
        if self.has(path) and not self.has(path, 'attr'):
            return True
        else:
            return False 
        ###try:
        ###    return len(self.get(path)) <= 1 # Empty or just with 'lock'
        ###except TypeError: # if get returns None
        ###    return False
    def is_not_empty(self, path): # To improve readability
        if self.has(path) and self.has(path, 'attr'):
            return True
        else:
            return False 
        ###try:
        ###    return len(self.get(path)) > 1 # More than just 'lock'
        ###except TypeError: # if get returns None
        ###    return False
 
class SNS_HTTPServer(BaseHTTPServer.HTTPServer):
    """ HTTP Server to receive SNS notifications via HTTP """
    def set_fs(self, fs):
        self.fs = fs

class SNS_HTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """ HTTP Request Handler to receive SNS notifications via HTTP """
    def do_POST(self):
        if self.path != self.server.fs.http_listen_path:
            self.send_response(404)
            return
 
        content_len = int(self.headers.getheader('content-length'))
        post_body = self.rfile.read(content_len)

        message_type = self.headers.getheader('x-amz-sns-message-type')
        message_content = json.loads(post_body)

        # Check SNS signature, I was not able to use boto for this...

        url = message_content['SigningCertURL']
        if not hasattr(self, 'certificate_url') or self.certificate_url != url:
            logger.debug('downloading certificate')
            self.certificate_url = url
            self.certificate = urllib2.urlopen(url).read()
 
        signature_version = message_content['SignatureVersion']
        if signature_version != '1':
            logger.debug('unknown signature version')
            self.send_response(404)
            return

        signature = message_content['Signature']

        del message_content['SigningCertURL']
        del message_content['SignatureVersion']
        del message_content['Signature']
        if 'UnsubscribeURL' in message_content:
            del message_content['UnsubscribeURL']
        string_to_sign = '\n'.join(list(itertools.chain.from_iterable(
                    [ (k, message_content[k]) for k in sorted(message_content.iterkeys()) ]
                    ))) + '\n'

        cert = M2Crypto.X509.load_cert_string(self.certificate)
        pub_key = cert.get_pubkey().get_rsa()
        verify_evp = M2Crypto.EVP.PKey()
        verify_evp.assign_rsa(pub_key)
        verify_evp.reset_context(md='sha1')
        verify_evp.verify_init()
        verify_evp.verify_update(string_to_sign.encode('ascii'))

        if verify_evp.verify_final(signature.decode('base64')):
            self.send_response(200)
            if message_type== 'Notification':
        	message = message_content['Message']
        	logger.debug('message = %s' % message)
                self.server.fs.process_message(message)
            elif message_type == 'SubscriptionConfirmation':
                token = message_content['Token']
                response = self.server.fs.sns.confirm_subscription(self.server.fs.sns_topic_arn, token)
                self.server.fs.http_subscription = response['ConfirmSubscriptionResponse']['ConfirmSubscriptionResult']['SubscriptionArn']
                logger.debug('SNS HTTP subscription = %s' % self.server.fs.http_subscription)
            else:
                logger.debug('unknown message type')
            return
        else:
            logger.debug('wrong signature')

        # If nothing better, return 404
        self.send_response(404)

    def do_GET(self):
        logger.debug('http get')
        self.send_response(404)
    def do_HEAD(self):
        logger.debug('http head')
        self.send_response(404)

class PartOfFSData():
    """ To read just a part of an existing FSData, inspired by FileChunkIO """
    def __init__(self, data, lock, start, length):
        self.data = data
        self.start = start
        self.length = length
        self.pos = 0
        self.lock = lock
    def seek(self, offset, whence=0):
        logger.debug("seek '%i' '%i'" % (offset, whence))
        if whence == 0:
            self.pos = offset
        elif whence == 1:
            self.pos = self.pos + offset
        elif whence == 2:
            self.pos = self.length + offset
    def tell(self):
        return self.pos
    def read(self, n=-1):
        logger.debug("read '%i' '%s' '%s' at '%i' starting from '%i' for '%i'"
                     % (n, self.data, self.data.content, self.pos, self.start, self.length))
        if n >= 0:
            n = min([n, self.length - self.pos])
            with self.lock:
                self.data.content.seek(self.start + self.pos)
                s = self.data.content.read(n)
            self.pos += len(s)
            return s
        else:
            return self.readall()
    def readall(self):
        return self.read(self.length - self.pos)

class YAS3FS(LoggingMixIn, Operations):
    """ Main FUSE Operations class for fusepy """
    def __init__(self, options):
        logger.info("Version: %s" % __version__)
        # Some constants
        ### self.http_listen_path_length = 30
        self.running = True
        self.check_status_interval = 5.0 # Seconds, no need to configure that
        self.s3_retries = 3 # Maximum number of S3 retries (outside of boto)
        self.yas3fs_xattrs = [ 'yas3fs.bucket', 'yas3fs.key', 'yas3fs.URL', 'yas3fs.signedURL',
                               'yas3fs.expiration' ]

        # Initialization
        global debug
        debug = options.debug

        # Parameters and options handling
        self.aws_region = options.region
        s3url = urlparse.urlparse(options.s3path.lower())
        if s3url.scheme != 's3':
            error_and_exit("The S3 path to mount must be in URL format: s3://BUCKET/PATH")
        self.s3_bucket_name = s3url.netloc
        logger.info("S3 bucket: '%s'" % self.s3_bucket_name)
        self.s3_prefix = s3url.path.strip('/')
        logger.info("S3 prefix (can be empty): '%s'" % self.s3_prefix)
        if self.s3_bucket_name == '':
            error_and_exit("The S3 bucket cannot be empty")
        self.sns_topic_arn = options.topic
        if self.sns_topic_arn:
            logger.info("AWS region for SNS and SQS: '" + self.aws_region + "'")
            logger.info("SNS topic ARN: '%s'" % self.sns_topic_arn)
        self.sqs_queue_name = options.queue # must be different for each client
        self.new_queue = options.new_queue
        self.queue_wait_time = options.queue_wait
        self.queue_polling_interval = options.queue_polling
        if self.sqs_queue_name:
            logger.info("SQS queue name: '%s'" % self.sqs_queue_name)
        if self.sqs_queue_name or self.new_queue:
            logger.info("SQS queue wait time (in seconds): '%i'" % self.queue_wait_time)
            logger.info("SQS queue polling interval (in seconds): '%i'" % self.queue_polling_interval)
        self.cache_entries = options.cache_entries
        logger.info("Cache entries: '%i'" % self.cache_entries)
        self.cache_mem_size = options.cache_mem_size * (1024 * 1024) # To convert MB to bytes
        logger.info("Cache memory size (in bytes): '%i'" % self.cache_mem_size)
        self.cache_disk_size = options.cache_disk_size * (1024 * 1024) # To convert MB to bytes
        logger.info("Cache disk size (in bytes): '%i'" % self.cache_disk_size)
        self.cache_on_disk = options.cache_on_disk # Bytes
        logger.info("Cache on disk if file size greater than (in bytes): '%i'" % self.cache_on_disk)
        self.cache_check_interval = options.cache_check # seconds
        logger.info("Cache check interval (in seconds): '%i'" % self.cache_check_interval)
        if options.use_ec2_hostname:
            instance_metadata = boto.utils.get_instance_metadata() # Very slow (to fail) outside of EC2
            self.hostname = instance_metadata['public-hostname']
        else:
            self.hostname = options.hostname
        if self.hostname:
            logger.info("Public hostname to listen to SNS HTTP notifications: '%s'" % self.hostname)
        self.sns_http_port = int(options.port or '0')
        if options.port:
            import M2Crypto # Required to check integrity of SNS HTTP notifications
            logger.info("TCP port to listen to SNS HTTP notifications: '%i'" % self.sns_http_port)
        self.s3_num = options.s3_num
        logger.info("Number of parallel S3 threads (0 to disable writeback): '%i'" % self.s3_num)
        self.download_num = options.download_num
        logger.info("Number of parallel donwloading threads: '%i'" % self.download_num)
        self.prefetch_num = options.prefetch_num
        logger.info("Number of parallel prefetching threads: '%i'" % self.prefetch_num)
        self.buffer_size = options.buffer_size * 1024 # To convert KB to bytes
        logger.info("Download buffer size (in KB, 0 to disable buffering): '%i'" % self.buffer_size)
        self.buffer_prefetch = options.buffer_prefetch
        logger.info("Number of buffers to prefetch: '%i'" % self.buffer_prefetch)
        self.write_metadata = not options.no_metadata
        logger.info("Write metadata (file system attr/xattr) on S3: '%s'" % str(self.write_metadata))
        self.full_prefetch = options.prefetch
        logger.info("Download prefetch: '%s'" % str(self.full_prefetch))
        self.multipart_size = options.mp_size * (1024 * 1024) # To convert MB to bytes
        logger.info("Multipart size: '%s'" % str(self.multipart_size))
        self.multipart_num = options.mp_num
        logger.info("Multipart maximum number of parallel threads: '%s'" % str(self.multipart_num))
        self.multipart_retries = options.mp_retries
        logger.info("Multipart maximum number of retries per part: '%s'" % str(self.multipart_retries))
        self.default_expiration = options.expiration
        logger.info("Default expiration for signed URLs via xattrs: '%s'" % str(self.default_expiration))
        self.requester_pays = options.requester_pays
        logger.info("S3 Request Payer: '%s'" % str(self.requester_pays))
        
        if self.requester_pays:
            self.default_headers = { 'x-amz-request-payer' : 'requester' }
        else:
            self.default_headers = {}

        self.darwin = options.darwin # To tailor ENOATTR for OS X

        # Internal Initialization
        if options.cache_path:
            cache_path = options.cache_path
        else:
            cache_path = '/tmp/yas3fs/' + self.s3_bucket_name
            if not self.s3_prefix == '':
                cache_path += '/' + self.s3_prefix
        logger.info("Cache path (on disk): '%s'" % cache_path)
        self.cache = FSCache(cache_path)
        self.publish_queue = Queue.Queue()
        self.s3_queue = {} # Of Queue.Queue()
        for i in range(self.s3_num):
            self.s3_queue[i] = Queue.Queue()
        self.download_queue = Queue.Queue()
        self.prefetch_queue = Queue.Queue()

        # AWS Initialization
        if not self.aws_region in (r.name for r in boto.s3.regions()):
            error_and_exit("wrong AWS region '%s' for S3" % self.aws_region)
        try:
            self.s3 = boto.connect_s3()
        except boto.exception.NoAuthHandlerFound:
            error_and_exit("no AWS credentials found")
        if not self.s3:
            error_and_exit("no S3 connection")
        try:
            self.s3_bucket = self.s3.get_bucket(self.s3_bucket_name)
        except boto.exception.S3ResponseError:
            error_and_exit("S3 bucket not found")

        pattern = re.compile('[\W_]+') # Alphanumeric characters only, to be used for pattern.sub('', s)

        unique_id_list = []
        if options.id:
            unique_id_list.append(pattern.sub('', options.id))
        unique_id_list.append(str(uuid.uuid4()))
        self.unique_id = '-'.join(unique_id_list)
        logger.info("Unique node ID: '%s'" % self.unique_id)
                
        if self.sns_topic_arn:
            if not self.aws_region in (r.name for r in boto.sns.regions()):
                error_and_exit("wrong AWS region '%s' for SNS" % self.aws_region)
            self.sns = boto.sns.connect_to_region(self.aws_region)
            if not self.sns:
                error_and_exit("no SNS connection")
            try:
                topic_attributes = self.sns.get_topic_attributes(self.sns_topic_arn)
            except boto.exception.BotoServerError:
                error_and_exit("SNS topic ARN not found in region '%s' " % self.aws_region)
            if not self.sqs_queue_name and not self.new_queue:
                if not (self.hostname and self.sns_http_port):
                    error_and_exit("With and SNS topic either the SQS queue name or the hostname and port to listen to SNS HTTP notifications must be provided")

        if self.sqs_queue_name or self.new_queue:
            if not self.sns_topic_arn:
                error_and_exit("The SNS topic must be provided when an SQS queue is used")
            if not self.aws_region in (r.name for r in boto.sqs.regions()):
                error_and_exit("wrong AWS region '" + self.aws_region + "' for SQS")
            self.sqs = boto.sqs.connect_to_region(self.aws_region)
            if not self.sqs:
                error_and_exit("no SQS connection")
            if self.new_queue:
                self.sqs_queue_name = '-'.join([ 'yas3fs',
                                               pattern.sub('', self.s3_bucket_name),
                                               pattern.sub('', self.s3_prefix),
                                               self.unique_id ])
                self.queue = None
            else:
                self.queue =  self.sqs.lookup(self.sqs_queue_name)
            if not self.queue:
                self.queue = self.sqs.create_queue(self.sqs_queue_name)
            logger.info("SQS queue name (new): '%s'" % self.sqs_queue_name)
            self.queue.set_message_class(boto.sqs.message.RawMessage) # There is a bug with the default Message class in boto

        if self.hostname or self.sns_http_port:
            if not self.sns_topic_arn:
                error_and_exit("The SNS topic must be provided when the hostname/port to listen to SNS HTTP notifications is given")            

        if self.sns_http_port:
            if not self.hostname:
                error_and_exit("The hostname must be provided with the port to listen to SNS HTTP notifications")
            ### self.http_listen_path = '/sns/' + base64.urlsafe_b64encode(os.urandom(self.http_listen_path_length))
            self.http_listen_path = '/sns'
            self.http_listen_url = "http://%s:%i%s" % (self.hostname, self.sns_http_port, self.http_listen_path)

        if self.multipart_size < 5242880:
            error_and_exit("The minimum size for multipart upload supported by S3 is 5MB")
        if self.multipart_retries < 1:
            error_and_exit("The number of retries for multipart uploads cannot be less than 1")

        signal.signal(signal.SIGINT, self.handler)

    def check_threads(self, first=False):
        logger.debug("check_threads '%s'" % first)

        if first:
            display = 'Starting'
        else:
            display = 'Restarting'

        for i in range(self.s3_num):
            if thread_is_not_alive(self.s3_threads[i]):
                logger.debug("%s S3 thread #%i" % (display, i))
                self.s3_threads[i] = TracebackLoggingThread(target=self.get_to_do_on_s3, args=(i,))
                self.s3_threads[i].deamon = False
                self.s3_threads[i].start()

        for i in range(self.download_num):
            if thread_is_not_alive(self.download_threads[i]):
                logger.debug("%s download thread #%i" % (display, i))
                self.download_threads[i] = TracebackLoggingThread(target=self.download)
                self.download_threads[i].deamon = True
                self.download_threads[i].start()

        for i in range(self.prefetch_num):
            if thread_is_not_alive(self.prefetch_threads[i]):
                logger.debug("%s prefetch thread #%i" % (display, i))
                self.prefetch_threads[i] = TracebackLoggingThread(target=self.download, args=(True,))
                self.prefetch_threads[i].deamon = True
                self.prefetch_threads[i].start()

        if self.sns_topic_arn:
            if thread_is_not_alive(self.publish_thread):
                logger.debug("%s publish thread" % display)
                self.publish_thread = TracebackLoggingThread(target=self.publish_messages)
                self.publish_thread.daemon = True
                self.publish_thread.start()

        if self.sqs_queue_name:
            if thread_is_not_alive(self.queue_listen_thread):
                logger.debug("%s queue listen thread" % display)
                self.queue_listen_thread = TracebackLoggingThread(target=self.listen_for_messages_over_sqs)
                self.queue_listen_thread.daemon = True
                self.queue_listen_thread.start()

        if self.sns_http_port:
            if thread_is_not_alive(self.http_listen_thread):
                logger.debug("%s HTTP listen thread" % display)
                self.http_listen_thread = TracebackLoggingThread(target=self.listen_for_messages_over_http)
                self.http_listen_thread.daemon = True
                self.http_listen_thread.start()

        if thread_is_not_alive(self.check_cache_thread):
            logger.debug("%s check cache thread" % display)
            self.check_cache_thread = TracebackLoggingThread(target=self.check_cache_size)
            self.check_cache_thread.daemon = True
            self.check_cache_thread.start()

    def init(self, path):
        logger.debug("init '%s'" % (path))

        self.s3_threads = {}
        for i in range(self.s3_num):
            self.s3_threads[i] = None
        self.download_threads = {}
        for i in range(self.download_num):
            self.download_threads[i] = None
        self.prefetch_threads = {}
        for i in range(self.prefetch_num):
            self.prefetch_threads[i] = None
       
        self.publish_thread = None
        self.queue_listen_thread = None
        self.http_listen_thread = None

        self.check_cache_thread = None

        self.check_threads(first=True)

        self.check_status_thread = TracebackLoggingThread(target=self.check_status)
        self.check_status_thread.daemon = True
        self.check_status_thread.start()

        if self.sqs_queue_name:
            logger.debug("Subscribing '%s' to '%s'" % (self.sqs_queue_name, self.sns_topic_arn))
            response = self.sns.subscribe_sqs_queue(self.sns_topic_arn, self.queue)
            self.sqs_subscription = response['SubscribeResponse']['SubscribeResult']['SubscriptionArn']
            logger.debug('SNS SQS subscription = %s' % self.sqs_subscription)

        if self.sns_http_port:
            self.http_listen_thread = None
            self.sns.subscribe(self.sns_topic_arn, 'http', self.http_listen_url)

    def handler(signum, frame):
        self.destroy('/')

    def flush_all_cache(self):
        logger.debug("flush_all_cache")
        with self.cache.lock:
            for path in self.cache.entries:
                data = self.cache.get(path, 'data')
                if data and data.has('change'):
                    self.upload_to_s3(path, data)
 
    def destroy(self, path):
        logger.debug("destroy '%s'" % (path))
        # Cleanup for unmount
        logger.info('File system unmount...')

        self.running = False

        if self.http_listen_thread:
            self.httpd.shutdown() # To stop HTTP listen thread
            logger.info("waiting for HTTP listen thread to shutdown...")
            self.http_listen_thread.join(5.0) # 5 seconds should be enough   
            logger.info("HTTP listen thread ended")
            self.sns.unsubscribe(self.http_subscription)
            logger.info("Unsubscribed SNS HTTP endpoint")
        if self.queue_listen_thread:
            self.sqs_queue_name = None # To stop queue listen thread
            logger.info("waiting for SQS listen thread to shutdown...")
            self.queue_listen_thread.join(self.queue_wait_time + 1.0)
            logger.info("SQS listen thread ended")
            self.sns.unsubscribe(self.sqs_subscription)
            logger.info("Unsubscribed SNS SQS endpoint")
            if self.new_queue:
                if self.sqs.delete_queue(self.queue):
                    logger.info("New queue deleted")
                else:
                    logger.error("New queue was not deleted")

        self.flush_all_cache()

        if self.sns_topic_arn:
            while not self.publish_queue.empty():
                time.sleep(1.0)
            self.sns_topic_arn = None # To stop publish thread
            logger.info("waiting for SNS publish thread to shutdown...")
            self.publish_thread.join(2.0) # 2 seconds should be enough
        if  self.cache_entries:
            self.cache_entries = 0 # To stop memory thread
            logger.info("waiting for check cache thread to shutdown...")
            self.check_cache_thread.join(self.cache_check_interval + 1.0)
        logger.info('File system unmounted.')
        
    def listen_for_messages_over_http(self):
        logger.info("Listening on: '%s'" % self.http_listen_url)
        server_class = SNS_HTTPServer
        handler_class = SNS_HTTPRequestHandler
        server_address = ('', self.sns_http_port)
        self.httpd = server_class(server_address, handler_class)
        self.httpd.set_fs(self)
        self.httpd.serve_forever()

    def listen_for_messages_over_sqs(self):
        logger.info("Listening on queue: '%s'" % self.queue.name)
        while self.sqs_queue_name:
            if self.queue_wait_time > 0:
                # Using SQS long polling, needs boto >= 2.7.0
                messages = self.queue.get_messages(10, wait_time_seconds=self.queue_wait_time)
            else:
                messages = self.queue.get_messages(10)
            logger.debug("Got %i messages from SQS" % len(messages))
            if messages:
                for m in messages:
                    content = json.loads(m.get_body())
                    message = content['Message'].encode('ascii')
                    self.process_message(message)
                    m.delete()
            else:
                if self.queue_polling_interval > 0:
                    time.sleep(self.queue_polling_interval)

    def invalidate_cache(self, path, etag=None):
        logger.debug("invalidate_cache '%s' '%s'" % (path, etag))
        with self.cache.get_lock(path):
            self.cache.delete(path, 'key')
            self.cache.delete(path, 'attr')
            self.cache.delete(path, 'xattr')
            data = self.cache.get(path, 'data')
            if data:
                if data.has('range'):
                    self.cache.delete(path, 'data')
                else:
                    data.set('new', etag)
            if self.cache.is_empty(path):
                self.cache.delete(path) # But keep it in the parent readdir

    def delete_cache(self, path):
        logger.debug("delete_cache '%s'" % (path))
        with self.cache.get_lock(path):
            self.cache.delete(path)
            self.reset_parent_readdir(path)

    def process_message(self, messages):
        logger.debug("process_message '%s'" % (messages))
        c = json.loads(messages)
        if not c[0] == self.unique_id: # discard message coming from itself
            if c[1] in ( 'mkdir', 'mknod', 'symlink' ) and c[2] != None:
                self.delete_cache(c[2])
            elif c[1] in ( 'rmdir', 'unlink' ) and c[2] != None:
                self.delete_cache(c[2])
            elif c[1] == 'rename' and c[2] != None and c[3] != None:
                self.delete_cache(c[2])
                self.delete_cache(c[3])
            elif c[1] == 'upload':
                if c[2] != None:
                    self.invalidate_cache(c[2], c[3])
                else: # Invalidate all the cached data
                    for path in self.cache.entries.keys():
                        self.invalidate_cache(path)
            elif c[1] == 'md':
                if c[2]:
                    self.cache.delete(c[3], 'key')
                    self.cache.delete(c[3], c[2])
            elif c[1] == 'reset':
                with self.cache.lock:
                    self.flush_all_cache()
                    self.cache.reset_all() # Completely reset the cache
            elif c[1] == 'url':
                with self.cache.lock:
                    self.flush_all_cache()
                    self.cache.reset_all() # Completely reset the cache
                    s3url = urlparse.urlparse(c[2])
                    if s3url.scheme != 's3':
                        error_and_exit("The S3 path to mount must be in URL format: s3://BUCKET/PATH")
                    self.s3_bucket_name = s3url.netloc
                    logger.info("S3 bucket: '%s'" % self.s3_bucket_name)
                    self.s3_prefix = s3url.path.strip('/')
                    logger.info("S3 prefix: '%s'" % self.s3_prefix)
                    try:
                        self.s3_bucket = self.s3.get_bucket(self.s3_bucket_name)
                    except boto.exception.S3ResponseError:
                        error_and_exit("S3 bucket not found")
            elif c[1] == 'cache':
                if c[2] == 'entries' and c[3] > 0:
                    self.cache_entries = int(c[3])
                elif c[2] == 'mem' and c[3] > 0:
                    self.cache_mem_size = int(c[3]) * (1024 * 1024) # MB
                elif c[2] == 'disk' and c[3] > 0:
                    self.cache_disk_size = int(c[3]) * (1024 * 1024) # MB
            elif c[1] == 'buffer' and c[3] >= 0:
                if c[2] == 'size':
                    self.buffer_size = int(c[3]) * 1024 # KB
                elif c[2] == 'prefetch':
                    self.buffer_prefetch = int(c[3])
            elif c[1] == 'prefetch':
                if c[2] == 'on':
                    self.full_prefetch = True
                elif c[2] == 'off':
                    self.full_prefetch = False
            elif c[1] == 'multipart':
                if c[2] == 'size' and c[3] >= 5120:
                    self.multipart_size = c[3] * 1024
                elif c[2] == 'num' and c[3] >= 0:
                    self.multipart_num = c[3]
                elif c[2] == 'retries' and c[3] >= 1:
                    self.multipart_retries = c[3]
            elif c[1] == 'ping':
                self.publish_status()

    def publish_status(self):
        hostname = socket.getfqdn()
        num_entries, mem_size, disk_size = self.cache.get_memory_usage()
        dq = self.download_queue.qsize()
        pq = self.prefetch_queue.qsize()
        s3q = 0
        for i in range(0, self.s3_num):
            s3q += self.s3_queue[i].qsize()
        message = [ 'status', hostname, num_entries, mem_size, disk_size, dq, pq, s3q ]
        self.publish(message)

    def publish_messages(self):
        while self.sns_topic_arn:
            try:
                message = self.publish_queue.get(True, 1) # 1 second time-out
                message.insert(0, self.unique_id)
                full_message = json.dumps(message)
                self.sns.publish(self.sns_topic_arn, full_message.encode('ascii'))
                self.publish_queue.task_done()
            except Queue.Empty:
                pass
                
    def publish(self, message):
        if self.sns_topic_arn:
            logger.debug("publish '%s'" % (message))
            self.publish_queue.put(message)

    def check_status(self):
        logger.debug("check_status")

        while self.cache_entries:

            num_entries, mem_size, disk_size = self.cache.get_memory_usage()
            s3q = 0 ### Remove duplicate code
            for i in range(0, self.s3_num):
                s3q += self.s3_queue[i].qsize()
            logger.info("entries, mem_size, disk_size, download_queue, prefetch_queue, s3_queue: %i, %i, %i, %i, %i, %i"
                        % (num_entries, mem_size, disk_size,
                           self.download_queue.qsize(), self.prefetch_queue.qsize(), s3q))

            if debug:
                logger.debug("new_locks, unused_locks: %i, %i"
                             % (len(self.cache.new_locks), len(self.cache.unused_locks)))
                (threshold0, threshold1, threshold2) = gc.get_threshold()
                (count0, count1, count2) = gc.get_count()
                logger.debug("gc count0/threshold0, count1/threshold1, count2/threshold2: %i/%i, %i/%i, %i/%i"
                             % (count0,threshold0,count1,threshold1,count2,threshold2))

            self.check_threads()

            time.sleep(self.check_status_interval)

    def check_cache_size(self):
        
        logger.debug("check_cache_size")

        while self.cache_entries:

            logger.debug("check_cache_size get_memory_usage")
            num_entries, mem_size, disk_size = self.cache.get_memory_usage()

            purge = False
            if num_entries > self.cache_entries:
                purge = True
                store = ''
            elif mem_size > self.cache_mem_size:
                purge = True
                store = 'mem'
            elif disk_size > self.cache_disk_size:
                purge = True
                store = 'disk'

            if purge:
                # Need to purge something
                path = self.cache.lru.popleft() # Take a path on top of the LRU (least used)
                with self.cache.get_lock(path):
                    if self.cache.has(path): # Path may be deleted before I acquire the lock
                        logger.debug("check_cache_size purge: '%s' '%s' ?" % (store, path))
                        data = self.cache.get(path, 'data')
                        full_delete = False
                        if (not data) or (data and (store == '' or data.store == store) and (not data.has('open')) and (not data.has('change'))):
                            if store == '':
                                logger.debug("check_cache_size purge: '%s' '%s' OK full" % (store, path))
                                self.cache.delete(path) # Remove completely from cache
                                full_delete = True
                            elif data:
                                logger.debug("check_cache_size purge: '%s' '%s' OK data" % (store, path))
                                self.cache.delete(path, 'data') # Just remove data
                            else:
                                logger.debug("check_cache_size purge: '%s' '%s' KO no data" % (store, path))
                        else:
                            logger.debug("check_cache_size purge: '%s' '%s' KO data? %s open? %s change? %s"
                                         % (store, path, data != None, data and data.has('open'), data and data.has('change')))
                        if not full_delete:
                            # The entry is still there, let's append it again at the end of the RLU list
                            self.cache.lru.append(path)
            else:
                # Check for unused locks to be removed
                for path in self.cache.unused_locks.keys():
                    logger.debug("check_cache_size purge unused lock: '%s'" % (path))
                    try:
                        with self.cache.lock and self.cache.new_locks[path]:
                            del self.cache.new_locks[path]
                            logger.debug("check_cache_size purge unused lock: '%s' deleted" % (path))
                    except KeyError:
                        pass
                    try:
                        del self.cache.unused_locks[path]
                        logger.debug("check_cache_size purge unused lock: '%s' removed from list" % (path))
                    except KeyError:
                        pass
                # Look for unused locks to be removed at next iteration (if still "new")
                for path in self.cache.new_locks.keys():
                    logger.debug("check_cache_size purge unused lock: '%s' added to list" % (path))
                    self.cache.unused_locks[path] = True # Just a flag

                # Sleep for some time
                time.sleep(self.cache_check_interval)

    def add_to_parent_readdir(self, path):
        logger.debug("add_to_parent_readdir '%s'" % (path))
        (parent_path, dir) = os.path.split(path)
        logger.debug("add_to_parent_readdir '%s' parent_path '%s'" % (path, parent_path))
        with self.cache.get_lock(parent_path):
            dirs = self.cache.get(parent_path, 'readdir')
            if dirs != None and dirs.count(dir) == 0:
                dirs.append(dir)

    def remove_from_parent_readdir(self, path):
        logger.debug("remove_from_parent_readdir '%s'" % (path))
        (parent_path, dir) = os.path.split(path)
        logger.debug("remove_from_parent_readdir '%s' parent_path '%s'" % (path, parent_path))
        with self.cache.get_lock(parent_path):
            dirs = self.cache.get(parent_path, 'readdir')
            if dirs != None:
                dirs.remove(dir)

    def reset_parent_readdir(self, path):
        logger.debug("reset_parent_readdir '%s'" % (path))
        (parent_path, dir) = os.path.split(path)
        logger.debug("reset_parent_readdir '%s' parent_path '%s'" % (path, parent_path))
        self.cache.delete(parent_path, 'readdir')

    def join_prefix(self, path):
        if self.s3_prefix == '':
            if path != '/':
                return path[1:] # Remove beginning '/'
            else:
                return '.' # To handle '/' with empty s3_prefix
        else:
            return self.s3_prefix + path

    def has_elements(self, iter, num=1):
        logger.debug("has_element '%s' %i" % (iter, num))
        c = 0
        for k in iter:
            logger.debug("has_element '%s' -> '%s'" % (iter, k))
            path = k.name[len(self.s3_prefix):]
            if not self.cache.has(path, 'deleted'):
                c += 1
            if c >= num:
                logger.debug("has_element '%s' OK" % (iter))
                return True
        logger.debug("has_element '%s' KO" % (iter))
        return False

    def folder_has_contents(self, path, num=1):
        logger.debug("folder_has_contents '%s' %i" % (path, num))
        full_path = self.join_prefix(path + '/')
        key_list = self.s3_bucket.list(full_path, '/', headers = self.default_headers)
        return self.has_elements(key_list, num)

    def get_key(self, path, cache=True):
        if cache:
            if self.cache.has(path, 'deleted'):
                logger.debug("get_key from cache deleted '%s'" % (path))
                return None
            key = self.cache.get(path, 'key')
            if key:
                logger.debug("get_key from cache '%s'" % (path))
                return key
            look_on_S3 = True
            if path != '/':
                (parent_path, file) = os.path.split(path)
                dirs = self.cache.get(parent_path, 'readdir')
                if dirs and file not in dirs:
                    look_on_S3 = False # We know it's not there
        if not cache or look_on_S3:
            logger.debug("get_key from S3 #1 '%s'" % (path))
            key = self.s3_bucket.get_key(self.join_prefix(path), headers=self.default_headers)
            if not key and path != '/':
                full_path = path + '/'
                logger.debug("get_key from S3 #2 '%s' '%s'" % (path, full_path))
                key = self.s3_bucket.get_key(self.join_prefix(full_path), headers=self.default_headers)
            if key:
                logger.debug("get_key to cache '%s'" % (path))
                self.cache.set(path, 'key', key)
        else:
            logger.debug("get_key not on S3 '%s'" % (path))
        if not key:
            logger.debug("get_key no '%s'" % (path))
        return key

    def get_metadata(self, path, metadata_name, key=None):
        logger.debug("get_metadata -> '%s' '%s' '%s'" % (path, metadata_name, key))
        with self.cache.get_lock(path): # To avoid consistency issues, e.g. with a concurrent purge
            if self.cache.has(path, metadata_name):
                metadata_values = self.cache.get(path, metadata_name)
            else:
                if not key:
                    key = self.get_key(path)
                if not key:
                    if path == '/': # First time mount of a new file system
                        self.mkdir(path, 0755)
                        logger.debug("get_metadata -> '%s' '%s' '%s' First time mount"
                                     % (path, metadata_name, key))
                        return self.cache.get(path, metadata_name)
                    else:
                        if not self.folder_has_contents(path):
                            self.cache.add(path) # It is empty to cache further checks
                            logger.debug("get_metadata '%s' '%s' '%s' no S3 return None"
                                         % (path, metadata_name, key))
                            return None
                        else:
                            logger.debug("get_metadata '%s' '%s' '%s' S3 found"
                                         % (path, metadata_name, key))
                metadata_values = {}
                if key:
                    s = key.get_metadata(metadata_name)
                else:
                    s = None
                if s:
                    try:
                        metadata_values = json.loads(s)
                    except ValueError: # For legacy attribute encoding
                        for kv in s.split(';'):
                            k, v = kv.split('=')
                            if v.isdigit():
                                metadata_values[k] = int(v)
                            elif v.replace(".", "", 1).isdigit():
                                metadata_values[k] = float(v)
                            else:
                                metadata_values[k] = v
                if metadata_name == 'attr': # Custom exception(s)
                    if key:
                        metadata_values['st_size'] = key.size
                    else:
                        metadata_values['st_size'] = 0
                    if not s: # Set default attr to browse any S3 bucket TODO directories
                        uid, gid = get_uid_gid()
                        metadata_values['st_uid'] = uid
                        metadata_values['st_gid'] = gid
                        if key and key.name != '' and key.name[-1] != '/':
                            metadata_values['st_mode'] = (stat.S_IFREG | 0755)
                        else:
                            metadata_values['st_mode'] = (stat.S_IFDIR | 0755)
                        if key and key.last_modified:
                            now = time.mktime(time.strptime(key.last_modified, "%a, %d %b %Y %H:%M:%S %Z"))
                        else:
                            now = get_current_time()
                        metadata_values['st_mtime'] = now
                        metadata_values['st_atime'] = now
                        metadata_values['st_ctime'] = now
                self.cache.add(path)
                self.cache.set(path, metadata_name, metadata_values)
            logger.debug("get_metadata <- '%s' '%s' '%s' '%s'" % (path, metadata_name, key, metadata_values))
            return metadata_values

    def set_metadata(self, path, metadata_name=None, metadata_values=None, key=None):
        logger.debug("set_metadata '%s' '%s' '%s'" % (path, metadata_name, key))
        with self.cache.get_lock(path):
            if not metadata_values == None:
                self.cache.set(path, metadata_name, metadata_values)
            data = self.cache.get(path, 'data')
            if self.write_metadata and (key or (not data) or (data and not data.has('change'))):
                # No change in progress, I should write now
                if not key:
                    key = self.get_key(path)
                    logger.debug("set_metadata '%s' '%s' '%s' Key" % (path, metadata_name, key))
                new_key = False
                if not key and self.folder_has_contents(path):
                    if path != '/' or self.write_metadata:
                        full_path = path + '/'
                        key = Key(self.s3_bucket)
                        key.key = self.join_prefix(full_path)
                        new_key = True
                if key:
                    if metadata_name:
                        values = metadata_values
                        if values == None:
                            values = self.cache.get(path, metadata_name)
                        if values == None or not any(values):
                            try:
                                del key.metadata[metadata_name]
                            except KeyError:
                                pass
                        else:
                            try:
                                key.metadata[metadata_name] = json.dumps(values)
                            except UnicodeDecodeError:
                                logger.info("set_metadata '%s' '%s' '%s' cannot decode unicode, not written on S3"
                                            % (path, metadata_name, key))
                                pass # Ignore the binary values - something better TODO ???
                    if (not data) or (data and (not data.has('change'))):
                        logger.debug("set_metadata '%s' '%s' S3" % (path, key))
                        pub = [ 'md', metadata_name, path ]
                        if new_key:
                            logger.debug("set_metadata '%s' '%s' S3 new key" % (path, key))
                            ### key.set_contents_from_string('', headers={'Content-Type': 'application/x-directory'})
                            headers = { 'Content-Type': 'application/x-directory' }
                            headers.update(self.default_headers)
                            cmds = [ [ 'set_contents_from_string', [ '' ], { 'headers': headers } ] ]
                            self.do_on_s3(key, pub, cmds)
                        else:
                            ### key.copy(key.bucket.name, key.name, key.metadata, preserve_acl=False)
                            cmds = [ [ 'copy', [ key.bucket.name, key.name, key.metadata ],
                                       { 'preserve_acl': False } ] ]
                            self.do_on_s3(key, pub, cmds)
                        ###self.publish(['md', metadata_name, path])
                    
    def getattr(self, path, fh=None):
        logger.debug("getattr -> '%s' '%s'" % (path, fh))
        with self.cache.get_lock(path): # To avoid consistency issues, e.g. with a concurrent purge
            if self.cache.is_empty(path):
                logger.debug("getattr <- '%s' '%s' cache ENOENT" % (path, fh))
                raise FuseOSError(errno.ENOENT)
            attr = self.get_metadata(path, 'attr')
            if attr == None:
                logger.debug("getattr <- '%s' '%s' ENOENT" % (path, fh))
                raise FuseOSError(errno.ENOENT)
            if attr['st_size'] == 0 and stat.S_ISDIR(attr['st_mode']):
                attr['st_size'] = 4096 # For compatibility...
            attr['st_nlink'] = 1 # Something better TODO ???
            if self.full_prefetch: # Prefetch
                if stat.S_ISDIR(attr['st_mode']):
                    self.readdir(path)
                else:
                    self.check_data(path)
            logger.debug("getattr <- '%s' '%s' '%s'" % (path, fh, attr))
            return attr

    def readdir(self, path, fh=None):
        logger.debug("readdir '%s' '%s'" % (path, fh))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("readdir '%s' '%s' ENOENT" % (path, fh))
                raise FuseOSError(errno.ENOENT)
            self.cache.add(path)
            dirs = self.cache.get(path, 'readdir')
            if not dirs:
                logger.debug("readdir '%s' '%s' no cache" % (path, fh))
                full_path = self.join_prefix(path)
                if full_path == '.':
                    full_path = ''
                elif full_path != '' and full_path[-1] != '/':
                    full_path += '/'
                logger.debug("readdir '%s' '%s' S3 list '%s'" % (path, fh, full_path))
                key_list = self.s3_bucket.list(full_path, '/', headers = self.default_headers)
                dirs = ['.', '..']
                for k in key_list:
                    logger.debug("readdir '%s' '%s' S3 list key '%s'" % (path, fh, k))
                    d = k.name[len(full_path):]
                    if len(d) > 0:
                        if d == '.':
                            continue # I need this for whole S3 buckets mounted without a prefix, I use '.' for '/' metadata
                        d_path = k.name[len(self.s3_prefix):]
                        if d[-1] == '/':
                            d = d[:-1]
                        if self.cache.has(d_path, 'deleted'):
                            continue
                        dirs.append(d)
                self.cache.set(path, 'readdir', dirs)

            logger.debug("readdir '%s' '%s' '%s'" % (path, fh, dirs))
            return dirs

    def mkdir(self, path, mode):
        logger.debug("mkdir '%s' '%s'" % (path, mode))
        with self.cache.get_lock(path):
            if self.cache.is_not_empty(path):
                logger.debug("mkdir cache '%s' EEXIST" % self.cache.get(path))
                raise FuseOSError(errno.EEXIST)
            k = self.get_key(path)
            if k:
                logger.debug("mkdir key '%s' EEXIST" % self.cache.get(path))
                raise FuseOSError(errno.EEXIST)
            now = get_current_time()
            uid, gid = get_uid_gid()
            attr = { 'st_uid': uid,
                     'st_gid': gid,
                     'st_atime': now,
                     'st_mtime': now,
                     'st_ctime': now,
                     'st_size': 0,
                     'st_mode': (stat.S_IFDIR | mode)
                 }
            self.cache.delete(path)
            self.cache.add(path)
            data = FSData(self.cache, 'mem', path)
            self.cache.set(path, 'data', data)
            data.set('change', True)
            k = Key(self.s3_bucket)
            self.set_metadata(path, 'attr', attr, k)
            self.set_metadata(path, 'xattr', {}, k)
            self.cache.set(path, 'key', k)
            if path != '/':
                full_path = path + '/'
                self.cache.set(path, 'readdir', ['.', '..']) # the directory is empty
                self.add_to_parent_readdir(path)
            else:
                full_path = path # To manage '/' with an empty s3_prefix

            if path != '/' or self.write_metadata:
                k.key = self.join_prefix(full_path)
                logger.debug("mkdir '%s' '%s' '%s' S3" % (path, mode, k))
                ###k.set_contents_from_string('', headers={'Content-Type': 'application/x-directory'})
                pub = [ 'mkdir', path ]
                headers = { 'Content-Type': 'application/x-directory'}
                headers.update(self.default_headers)
                cmds = [ [ 'set_contents_from_string', [ '' ], { 'headers': headers } ] ]
                self.do_on_s3(k, pub, cmds)
            data.delete('change')
            ###if path != '/': ### Do I need this???
            ###    self.publish(['mkdir', path])

            return 0
 
    def symlink(self, path, link):
        logger.debug("symlink '%s' '%s'" % (path, link))
        with self.cache.get_lock(path):
            if self.cache.is_not_empty(path):
                logger.debug("symlink cache '%s' '%s' EEXIST" % (path, link))
                raise FuseOSError(errno.EEXIST)
            k = self.get_key(path)
            if k:
                logger.debug("symlink key '%s' '%s' EEXIST" % (path, link))
                raise FuseOSError(errno.EEXIST)
            now = get_current_time()
            uid, gid = get_uid_gid()
            attr = {}
            attr['st_uid'] = uid
            attr['st_gid'] = gid
            attr['st_atime'] = now
            attr['st_mtime'] = now
            attr['st_ctime'] = now
            attr['st_size'] = 0
            attr['st_mode'] = (stat.S_IFLNK | 0755)
            self.cache.delete(path)
            self.cache.add(path)
            if self.cache_on_disk > 0:
                data = FSData(self.cache, 'mem', path) # New files (almost) always cache in mem - is it ok ???
            else:
                data = FSData(self.cache, 'disk', path)
            self.cache.set(path, 'data', data)
            data.set('change', True)
            k = Key(self.s3_bucket)
            self.set_metadata(path, 'attr', attr, k)
            self.set_metadata(path, 'xattr', {}, k)
            data.open()
            self.write(path, link, 0)
            data.close()
            k.key = self.join_prefix(path)
            self.cache.set(path, 'key', k)
            self.add_to_parent_readdir(path)
            logger.debug("symlink '%s' '%s' '%s' S3" % (path, link, k))
            ###k.set_contents_from_string(link, headers={'Content-Type': 'application/x-symlink'})
            pub = [ 'symlink', path ]
            headers = { 'Content-Type': 'application/x-symlink' }
            headers.update(self.default_headers)
            cmds = [ [ 'set_contents_from_string', [ link ], { 'headers': headers } ] ]
            self.do_on_s3(k, pub, cmds)
            data.delete('change')
            ###self.publish(['symlink', path])

            return 0

    def check_data(self, path): 
        logger.debug("check_data '%s'" % (path))
        with self.cache.get_lock(path):
            data = self.cache.get(path, 'data')
            if not data or data.has('new'):
                k = self.get_key(path)
                if not k:
                    logger.debug("check_data '%s' no key" % (path))
                    return False
                if not data:
                    if k.size < self.cache_on_disk:
                        data = FSData(self.cache, 'mem', path)
                    else:
                        data = FSData(self.cache, 'disk', path)
                    self.cache.set(path, 'data', data)
                new_etag = data.get('new')
                etag = k.etag[1:-1]
                if not new_etag or new_etag == etag:
                    data.delete('new')
                else: # I'm not sure I got the latest version
                    logger.debug("check_data '%s' etag is different" % (path))
                    self.cache.delete(path, 'key') # Next time get the key from S3
                    data.set('new', None) # Next time don't check the Etag
                if data.etag == etag:
                    logger.debug("check_data '%s' etag is the same, data is usable" % (path))
                    return True
                data.update_size()
                if k.size == 0:
                    logger.debug("check_data '%s' nothing to download" % (path))
                    return True # No need to download anything
                elif self.buffer_size > 0: # Use buffers
                    if not data.has('range'):
                        data.set('range', FSRange())
                    logger.debug("check_data '%s' created empty data object" % (path))
                else: # Download at once
                    k.get_contents_to_file(data.content, headers = self.default_headers)
                    data.update_size()
                    data.update_etag(k.etag[1:-1])
                    logger.debug("check_data '%s' data downloaded at once" % (path))
            else:
                logger.debug("check_data '%s' data already in place" % (path))
            return True

    def enqueue_download_data(self, path, starting_from=0, length=0, prefetch=False):
        logger.debug("enqueue_download_data '%s' %i %i" % (path, starting_from, length))
        start_buffer = int(starting_from / self.buffer_size)
        if length == 0: # Means to the end of file
            key = self.get_key(path)
            number_of_buffers = 1 + int((key.size - 1 - starting_from) / self.buffer_size)
        else:
            end_buffer = int(starting_from + length - 1) / self.buffer_size
            number_of_buffers = 1 + (end_buffer - start_buffer)
        for i in range(number_of_buffers):
            start = (start_buffer + i) * self.buffer_size
            end = start + self.buffer_size - 1
            option_list = (path, start, end)
            if prefetch:
                self.prefetch_queue.put(option_list)
            else:
                self.download_queue.put(option_list)

    def download(self, prefetch=False):
        while self.running:
           try:
               if prefetch:
                   (path, start, end) = self.prefetch_queue.get(True, 1) # 1 second time-out
               else:
                   
                   (path, start, end) = self.download_queue.get(True, 1) # 1 second time-out
               self.download_data(path, start, end)
               if prefetch:
                   self.prefetch_queue.task_done()
               else:
                   self.download_queue.task_done()
           except Queue.Empty:
               pass

    def download_data(self, path, start, end):
        thread_name = threading.current_thread().name
        logger.debug("download_data '%s' %i-%i [thread '%s']" % (path, start, end, thread_name))

        key = copy.deepcopy(self.get_key(path))

        if start > (key.size - 1):
            logger.debug("download_data EOF '%s' %i-%i [thread '%s']" % (path, start, end, thread_name))
            return

        with self.cache.get_lock(path):
            data = self.cache.get(path, 'data')
            if not data:
                logger.debug("download_data no data (before) '%s' [thread '%s']" % (path, thread_name))
                return
            data_range = data.get('range')
            if not data_range:
                logger.debug("download_data no range (before) '%s' [thread '%s']"
                             % (path, thread_name))
                return
            new_interval = [start, min(end, key.size - 1)]
            if data_range.interval.contains(new_interval): ### Can be removed ???
                logger.debug("download_data '%s' %i-%i [thread '%s'] already downloaded"
                             % (path, start, end, thread_name))
                return
            else:
                for i in data_range.ongoing_intervals.itervalues():
                    if i[0] <= new_interval[0] and i[1] >= new_interval[1]:
                        logger.debug("download_data '%s' %i-%i [thread '%s'] already downloading"
                                     % (path, start, end, thread_name))
                        return
            data_range.ongoing_intervals[thread_name] = new_interval

        if new_interval[0] == 0 and new_interval[1] == key.size -1:
            range_headers = {}
        else:
            range_headers = { 'Range' : 'bytes=' + str(new_interval[0]) + '-' + str(new_interval[1]) }

        range_headers.update(self.default_headers) ### Should I check self.requester_pays first?

        retry = True
        while retry:
            logger.debug("download_data range '%s' '%s' [thread '%s']" % (path, range_headers, thread_name))
            try:
                if debug:
                    n1=dt.datetime.now()
                if range_headers: # Use range headers only if necessary
                    bytes = key.get_contents_as_string(headers=range_headers)
                else:
                    bytes = key.get_contents_as_string()
                if debug:
                    n2=dt.datetime.now()
                retry = False
            except Exception as e:
                logger.exception(e)
                logger.info("download_data error '%s' %i-%i [thread '%s'] -> retrying" % (path, start, end, thread_name))
                time.sleep(1.0) # Better wait 1 second before retrying
                key = copy.deepcopy(self.get_key(path)) # Do I need this to overcome error "caching" ???

        if debug:
            elapsed = (n2-n1).microseconds/1e6
            logger.debug("download_data done '%s' %i-%i [thread '%s'] elapsed %.6f" % (path, start, end, thread_name, elapsed))

        with self.cache.get_lock(path):
                data = self.cache.get(path, 'data')
                if not data:
                    logger.debug("download_data no data (after) '%s' [thread '%s']" % (path, thread_name))
                    return
                data_range = data.get('range')
                if not data_range:
                    logger.debug("download_data no range (after) '%s' [thread '%s']" % (path, thread_name))
                    return
                del data_range.ongoing_intervals[thread_name]
                if not bytes:
                    length = 0
                    logger.debug("download_data no bytes '%s' [thread '%s']" % (path, thread_name))
                else:
                    length = len(bytes)
                    logger.debug("download_data %i bytes '%s' [thread '%s']" % (length, path, thread_name))
                if length > 0:
                    with data.get_lock():
                        no_content = False
                        if not data.content: # Usually for prefetches
                            no_content = True
                            data.open()
                        data.content.seek(start)
                        data.content.write(bytes)
                        new_interval = [start, start + length - 1]
                        data_range.interval.add(new_interval)
                        data.update_size()
                        if no_content:
                            data.close()
                        data_range.wake()

        logger.debug("download_data end '%s' %i-%i [thread '%s']" % (path, start, end, thread_name))

        with self.cache.get_lock(path):
            data = self.cache.get(path, 'data')
            data_range = data.get('range')
            if data_range:
                if data_range.interval.contains([0, key.size - 1]): # -1 ???
                    data.delete('range')
                    data.update_etag(key.etag[1:-1])
                    logger.debug("download_data all ended '%s' [thread '%s']" % (path, thread_name))

    def get_to_do_on_s3(self, i):
        while self.running:
           try:
               (key, pub, cmds) = self.s3_queue[i].get(True, 1) # 1 second time-out
               self.do_on_s3_now(key, pub, cmds)
               self.s3_queue[i].task_done()
           except Queue.Empty:
               pass

    def do_on_s3(self, key, pub, cmds):
        if self.s3_num == 0:
            self.do_on_s3_now(key, pub, cmds)
        else:
            i = hash(key.name) % self.s3_num # To distribute files consistently across threads
            self.s3_queue[i].put((key, pub, cmds))
        pass

    def do_on_s3_now(self, key, pub, cmds):
        for c in cmds:
            action = c[0]
            if len(c) > 1:
                args = c[1]
                if len(c) > 2:
                    kargs = c[2]
                else:
                    kargs = None
            else:
                args = None
                kargs = None

            logger.debug("do_on_s3_now action '%s' key '%s' args '%s' kargs '%s'" % (action, key, args, kargs))

            for retry in range(self.s3_retries):
                try:
                    if action == 'delete':
                        path = pub[1]
                        key.delete()
                        self.cache.dec(path, 'deleted')
                    elif action == 'copy':
                        key.copy(*args, **kargs)
                    elif action == 'set_contents_from_string':
                        key.set_contents_from_string(*args,**kargs)
                    elif action == 'set_contents_from_file':
                        data = args[0] # First argument must be data
                        key.set_contents_from_file(data.get_content(),**kargs)
                        etag = key.etag[1:-1]
                        with data.get_lock():
                            data.update_etag(etag)
                            data.delete('change')
                        pub.append(etag)
                    elif action == 'multipart_upload':
                        complete = self.multipart_upload(*args,**kargs)
                        etag = complete.etag[1:-1]
                        self.cache.delete(data.path, 'key')
                        with data.get_lock():
                            data.update_etag(etag)
                            data.delete('change')
                        pub.append(etag)
                    else:
                        logger.error("do_on_s3_now Unknown action '%s'" % action)
                except Exception as e:
                    logger.exception(e)
                    time.sleep(1.0) # Better wait 1 second before retrying 
                    logger.debug("do_on_s3_now action '%s' key '%s' args '%s' kargs '%s' retry %i" % (action, key, args, kargs, retry))

            self.publish(pub)

    def readlink(self, path):
        logger.debug("readlink '%s'" % (path))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("readlink '%s' ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)
            self.cache.add(path)
            if stat.S_ISLNK(self.getattr(path)['st_mode']):
                if not self.check_data(path):
                    logger.debug("readlink '%s' ENOENT" % (path))
                    raise FuseOSError(errno.ENOENT)
                data = self.cache.get(path, 'data')
                if data == None:
                    logger.error("readlink '%s' no data ENOENT" % (path))
                    raise FuseOSError(errno.ENOENT) # ??? That should not happen
                data_range = data.get('range')
                if data_range:
                    self.enqueue_download_data(path)
                    while True:
                        logger.debug("readlink wait '%s'" % (path))
                        data_range.wait()
                        logger.debug("readlink awake '%s'" % (path))
                        data_range = data.get('range')
                        if not data_range:
                            break
                data.open()
                link = data.get_content_as_string()
                data.close()
                return link
            logger.debug("readlink '%s' EINVAL" % (path))
            raise FuseOSError(errno.EINVAL)
 
    def rmdir(self, path):
        logger.debug("rmdir '%s'" % (path))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("rmdir '%s' cache ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)
            k = self.get_key(path)
            if not k and not self.folder_has_contents(path):
                logger.debug("rmdir '%s' key ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)
            dirs = self.cache.get(path, 'readdir')
            if dirs == None:
                if self.folder_has_contents(path, 2): # There is something inside the folder
                    logger.debug("rmdir '%s' S3 ENOTEMPTY" % (path))
                    raise FuseOSError(errno.ENOTEMPTY)
            else:
                if len(dirs) > 2:
                    logger.debug("rmdir '%s' cache ENOTEMPTY" % (path))
                    raise FuseOSError(errno.ENOTEMPTY)
            ###k.delete()
            ###self.publish(['rmdir', path])
            self.cache.reset(path) # Cache invaliation
            self.remove_from_parent_readdir(path)
            if k:
                logger.debug("rmdir '%s' '%s' S3" % (path, k))
                pub = [ 'rmdir', path ]
                cmds = [ [ 'delete', [] , { 'headers': self.default_headers } ] ]
                self.cache.inc(path, 'deleted')
                self.do_on_s3(k, pub, cmds)

            return 0

    def truncate(self, path, size):
        logger.debug("truncate '%s' '%i'" % (path, size))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("truncate '%s' '%i' ENOENT" % (path, size))
                raise FuseOSError(errno.ENOENT)
            self.cache.add(path)
            if not self.check_data(path):
                logger.debug("truncate '%s' '%i' ENOENT" % (path, size))
                raise FuseOSError(errno.ENOENT)
            while True:
                data = self.cache.get(path, 'data')
                if not data:
                    logger.error("truncate '%s' '%i' no data ENOENT" % (path, size))
                    raise FuseOSError(errno.ENOENT) # ??? That should not happen
                data_range = data.get('range')
                if not data_range:
                    break
                if (size == 0) or (data_range.interval.contains([0, size - 1])):
                    data.delete('range')
                    break
                self.enqueue_download_data(path, 0, size)
                logger.debug("truncate wait '%s' '%i'" % (path, size))
                data_range.wait()
                logger.debug("truncate awake '%s' '%i'" % (path, size))
            data.content.truncate(size)
            now = get_current_time()
            attr = self.get_metadata(path, 'attr')
            old_size = attr['st_size']
            data.set('change', True)
            if size != old_size:
                attr['st_size'] = size
                data.update_size()
            attr['st_mtime'] = now
            attr['st_atime'] = now
            return 0

    ### Should work for files in cache but not flushed to S3...
    def rename(self, path, new_path):
        logger.debug("rename '%s' '%s'" % (path, new_path))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("rename '%s' '%s' ENOENT no '%s' from cache" % (path, new_path, path))
                raise FuseOSError(errno.ENOENT)
            key = self.get_key(path)
            if not key and not self.cache.has(path):
                logger.debug("rename '%s' '%s' ENOENT no '%s'" % (path, new_path, path))
                raise FuseOSError(errno.ENOENT)
            new_parent_path = os.path.dirname(new_path)
            new_parent_key = self.get_key(new_parent_path)
            if not new_parent_key and not self.folder_has_contents(new_parent_path):
                logger.debug("rename '%s' '%s' ENOENT no parent path '%s'" % (path, new_path, new_parent_path))
                raise FuseOSError(errno.ENOENT)
            to_copy = {}
        if (key and key.name[-1] == '/') or (not key and self.folder_has_contents(path, 2)):
            full_key = self.join_prefix(path + '/')
            key_list = self.s3_bucket.list(full_key, headers = self.default_headers) # Don't need to set a delimeter here
            for k in key_list:
                source = k.name
                target = self.join_prefix(new_path + source[len(full_key) - 1:])
                to_copy[source] = target
        if not key: # Otherwise we miss the folder if there is no curresponding object on S3
            to_copy[self.join_prefix(path)] = self.join_prefix(new_path)
        for source, target in to_copy.iteritems():
            source_path = source[len(self.s3_prefix):].rstrip('/')
            if source_path[0] != '/':
                source_path = '/' + source_path
            if self.cache.has(source_path, 'deleted'): # Do not remove deleted items
                continue
            target_path = target[len(self.s3_prefix):].rstrip('/')
            if target_path[0] != '/':
                target_path = '/' + target_path
            logger.debug("renaming '%s' ('%s') -> '%s' ('%s')" % (source, source_path, target, target_path))
            self.cache.rename(source_path, target_path)
            key = self.s3_bucket.get_key(source, headers=self.default_headers)
            if key: # For files in cache but still not flushed to S3
                self.cache.inc(source_path, 'deleted')
                self.rename_on_s3(key, target, source_path, target_path)

        self.remove_from_parent_readdir(path)
        self.add_to_parent_readdir(new_path)

    def rename_on_s3(self, key, target, source_path, target_path):
        # Otherwise we loose the Content-Type with S3 Copy
        key.metadata['Content-Type'] = key.content_type
        ### key.copy(key.bucket.name, target, key.metadata, preserve_acl=False)
        pub = [ 'rename', source_path, target_path ]
        cmds = [ [ 'copy', [ key.bucket.name, target, key.metadata ],
                   { 'preserve_acl': False } ],
                 [ 'delete', [], { 'headers': self.default_headers } ] ]
        self.do_on_s3(key, pub, cmds)
        ###key.delete()
        ###self.publish(['rename', source_path, target_path])

    def mknod(self, path, mode, dev=None):
        logger.debug("mknod '%s' '%i' '%s'" % (path, mode, dev))
        with self.cache.get_lock(path):
            if self.cache.is_not_empty(file):
                logger.debug("mknod '%s' '%i' '%s' cache EEXIST" % (path, mode, dev))
                raise FuseOSError(errno.EEXIST)
            k = self.get_key(path)
            if k:
                logger.debug("mknod '%s' '%i' '%s' key EEXIST" % (path, mode, dev))
                raise FuseOSError(errno.EEXIST)
            self.cache.add(path)
            now = get_current_time()
            uid, gid = get_uid_gid()
            attr = {}
            attr['st_uid'] = uid
            attr['st_gid'] = gid
            attr['st_mode'] = int(stat.S_IFREG | mode)
            attr['st_atime'] = now
            attr['st_mtime'] = now
            attr['st_ctime'] = now
            attr['st_size'] = 0 # New file
            if self.cache_on_disk > 0:
                data = FSData(self.cache, 'mem', path) # New files (almost) always cache in mem - is it ok ???
            else:
                data = FSData(self.cache, 'disk', path)
            self.cache.set(path, 'data', data)
            data.set('change', True)
            self.set_metadata(path, 'attr', attr)
            self.set_metadata(path, 'xattr', {})
            self.add_to_parent_readdir(path)
            self.publish(['mknod', path])
            return 0

    def unlink(self, path):
        logger.debug("unlink '%s'" % (path))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("unlink '%s' ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)
            k = self.get_key(path)
            if not k and not self.cache.has(path):
                logger.debug("unlink '%s' ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)
            self.cache.reset(path) # Cache invaliation
            self.remove_from_parent_readdir(path)
            if k:
                logger.debug("unlink '%s' '%s' S3" % (path, k))
                ###k.delete()
                ###self.publish(['unlink', path])
                pub = [ 'unlink', path ]
                cmds = [ [ 'delete', [], { 'headers': self.default_headers } ] ]
                self.cache.inc(path, 'deleted')
                self.do_on_s3(k, pub, cmds)

	return 0

    def create(self, path, mode, fi=None):
        logger.debug("create '%s' '%i' '%s'" % (path, mode, fi))
	return self.open(path, mode)

    def open(self, path, flags):
        logger.debug("open '%s' '%i'" % (path, flags))
        with self.cache.get_lock(path):
            self.cache.add(path)
            if not self.check_data(path):
                self.mknod(path, flags)
            self.cache.get(path, 'data').open()
            logger.debug("open '%s' '%i' '%s'" % (path, flags, self.cache.get(path, 'data').get('open')))
	return 0

    def release(self, path, flags):
        logger.debug("release '%s' '%i'" % (path, flags))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("release '%s' '%i' ENOENT" % (path, flags))
                raise FuseOSError(errno.ENOENT)
            data = self.cache.get(path, 'data')
            if data:
                if data.has('change') and data.get('open') == 1: # Last one to release the file
                    self.upload_to_s3(path, data)
                data.close() # Close after upload to have data.content populated for disk cache
                logger.debug("release '%s' '%i' '%s'" % (path, flags, data.get('open')))
            else:
                logger.debug("release '%s' '%i'" % (path, flags))
	return 0

    def read(self, path, length, offset, fh=None):
        logger.debug("read '%s' '%i' '%i' '%s'" % (path, length, offset, fh))
        if not self.cache.has(path) or self.cache.is_empty(path):
            logger.debug("read '%s' '%i' '%i' '%s' ENOENT" % (path, length, offset, fh))
            raise FuseOSError(errno.ENOENT)
        while True:
            data = self.cache.get(path, 'data')
            if not data:
                logger.debug("read '%s' '%i' '%i' '%s' no data" % (path, length, offset, fh))  
                return '' # Something better ???
            data_range = data.get('range')
            if data_range == None:
                logger.debug("read '%s' '%i' '%i' '%s' no range" % (path, length, offset, fh))
                break
	    attr = self.get_metadata(path, 'attr')
            file_size = attr['st_size']
            end_interval = min(offset + length, file_size) - 1
            if offset > end_interval:
                logger.debug("read '%s' '%i' '%i' '%s' offset=%i > end_interval=%i" %((path, length, offset, fh, offset, end_interval)))
                return '' # Is this ok ???
            read_interval = [offset, end_interval]
            if data_range.interval.contains(read_interval):
                if self.buffer_prefetch:
                    prefetch_start = end_interval + 1
                    prefetch_length = self.buffer_size * self.buffer_prefetch
                    logger.debug("download prefetch '%s' '%i' '%i'" % (path, prefetch_start, prefetch_length))
                    prefetch_end_interval = min(prefetch_start + prefetch_length, file_size) - 1
                    if prefetch_start < prefetch_end_interval:
                        prefetch_interval = [prefetch_start, prefetch_end_interval]
                        if not data_range.interval.contains(prefetch_interval):
                            self.enqueue_download_data(path, prefetch_start, prefetch_length, prefetch=True)
                logger.debug("read '%s' '%i' '%i' '%s' in range" % (path, length, offset, fh))                
                break
            else:
                logger.debug("read '%s' '%i' '%i' '%s' out of range" % (path, length, offset, fh))
                self.enqueue_download_data(path, offset, length)
            logger.debug("read wait '%s' '%i' '%i' '%s'" % (path, length, offset, fh))
            data_range.wait()
            logger.debug("read awake '%s' '%i' '%i' '%s'" % (path, length, offset, fh))
            # update atime just in the cache ???
        with data.get_lock():
            if not data.content:
                logger.debug("read '%s' '%i' '%i' '%s' no content" % (path, length, offset, fh))
                return '' # Something better ???
            data.content.seek(offset)
            return data.content.read(length)

    def write(self, path, new_data, offset, fh=None):
        logger.debug("write '%s' '%i' '%i' '%s'" % (path, len(new_data), offset, fh))
        if not self.cache.has(path) or self.cache.is_empty(path):
            logger.debug("write '%s' '%i' '%i' '%s' ENOENT" % (path, len(new_data), offset, fh))
            raise FuseOSError(errno.ENOENT)
        if isinstance(new_data, unicode): # Fix for unicode
            logger.debug("write '%s' '%i' '%i' '%s' unicode fix" % (path, len(new_data), offset, fh))
            new_data = str(new_data)
	length = len(new_data)

        data = self.cache.get(path, 'data')
        data_range = data.get('range')

        if data_range:
            self.enqueue_download_data(path)
            while data_range:
                logger.debug("write wait '%s' '%i' '%i' '%s'" % (path, len(new_data), offset, fh))            
                data_range.wait()
                logger.debug("write awake '%s' '%i' '%i' '%s'" % (path, len(new_data), offset, fh))            
                data_range = data.get('range')
                
	with data.get_lock():
            if not data.content:
                logger.info("write awake '%s' '%i' '%i' '%s' no content" % (path, len(new_data), offset, fh))            
                return 0
            logger.debug("write '%s' '%i' '%i' '%s' '%s' content" % (path, len(new_data), offset, fh, data.content))
            data.content.seek(offset)
            data.content.write(new_data)
            data.set('change', True)
	    now = get_current_time()
	    attr = self.get_metadata(path, 'attr')
            old_size = attr['st_size']
            new_size = max(old_size, offset + length)
            if new_size != old_size:
                attr['st_size'] = new_size
                data.update_size()
            attr['st_mtime'] = now
            attr['st_atime'] = now
        return length
                         
    def upload_to_s3(self, path, data):
        logger.debug("upload_to_s3 '%s'" % path)
        k = self.get_key(path)
        if not k: # New key
            k = Key(self.s3_bucket)
            k.key = self.join_prefix(path)
            self.cache.set(path, 'key', k)
        now = get_current_time()
        attr = self.get_metadata(path, 'attr', k)
        attr['st_atime'] = now
        attr['st_mtime'] = now
        self.set_metadata(path, 'attr', None, k) # To update key metadata before upload to S3
        self.set_metadata(path, 'xattr', None, k) # To update key metadata before upload to S3
        mimetype = mimetypes.guess_type(path)[0] or 'application/octet-stream'
        if k.size == None:
            old_size = 0
        else:
            old_size = k.size

        written = False
        pub = [ 'upload', path ] # Add Etag before publish
        headers = { 'Content-Type': mimetype }
        headers.update(self.default_headers)
        if self.multipart_num > 0:
            full_size = attr['st_size']
            if full_size > self.multipart_size:
                logger.debug("upload_to_s3 '%s' '%s' '%s' S3 multipart" % (path, k, mimetype))
                cmds = [ [ 'multipart_upload', [ k.name, data, full_size ], { headers: headers, metadata:k.metadata } ] ]
                written = True
        if not written:
            logger.debug("upload_to_s3 '%s' '%s' '%s' S3" % (path, k, mimetype))
            ###k.set_contents_from_file(data.content, headers=headers)
            cmds = [ [ 'set_contents_from_file', [ data ], { 'headers': headers } ] ]
        self.do_on_s3(k, pub, cmds)
        ###self.publish(['upload', path, etag])
        logger.debug("upload_to_s3 '%s' done" % path)

    def multipart_upload(self, key_path, data, full_size, headers, metadata):
        logger.debug("multipart_upload '%s' '%s' '%s' '%s'" % (key_path, data, full_size, headers))
        part_num = 0
        part_pos = 0
        part_queue = Queue.Queue()
        multipart_size = max(self.multipart_size, full_size / 100) # No more than 100 parts...
        logger.debug("multipart_upload '%s' multipart_size '%s'" % (key_path, multipart_size))
        upload_lock = threading.Lock()
        while part_pos < full_size:
            bytes_left = full_size - part_pos
            if bytes_left > self.multipart_size:
                part_size = self.multipart_size
            else:
                part_size = bytes_left
            part_num += 1
            part_queue.put([ part_num, PartOfFSData(data, upload_lock, part_pos, part_size) ])
            part_pos += part_size
            logger.debug("part from %i for %i" % (part_pos, part_size))
        logger.debug("initiate_multipart_upload '%s' '%s'" % (key_path, headers))
        num_threads = min(part_num, self.multipart_num)
        logger.debug("multipart_upload '%s' num_threads '%s'" % (key_path, num_threads))
        mpu = self.s3_bucket.initiate_multipart_upload(key_path, headers=headers, metadata=metadata)
        for i in range(num_threads): 
            t = TracebackLoggingThread(target=self.part_upload, args=(mpu, part_queue))
            t.demon = True
            t.start()
            logger.debug("multipart_upload thread '%i' started" % i)
        logger.debug("multipart_upload all threads started '%s' '%s' '%s'" % (key_path, data, headers))
        part_queue.join()
        logger.debug("multipart_upload all threads joined '%s' '%s' '%s'" % (key_path, data, headers))
        if len(mpu.get_all_parts()) == part_num:
            logger.debug("multipart_upload ok '%s' '%s' '%s'" % (key_path, data, headers))
            new_key = mpu.complete_upload()
        else:
            logger.debug("multipart_upload cancel '%s' '%s' '%s' '%i' != '%i'" % (key_path, data, headers, mpu.get_all_parts(), part_num))
            mpu.cancel_upload()
            new_key = None
        return new_key

    def part_upload(self, mpu, part_queue):
        logger.debug("new thread!")
        try:
            while (True):
                logger.debug("trying to get a part from the queue")
                [ num, part ] = part_queue.get(False)
                for retry in range(self.multipart_retries):
                    logger.debug("begin upload of part %i retry %i" % (num, retry))
                    try:
                        mpu.upload_part_from_file(fp=part, part_num=num)
                        break
                    except Exception as e:
                        logger.exception(e)
                        logger.info("error during multipart upload part %i retry %i: %s"
                                    % (num, retry, sys.exc_info()[0]))
                        time.sleep(1.0) # Better wait 1 second before retrying  
                logger.debug("end upload of part %i retry %i" % (num, retry))
                part_queue.task_done()
        except Queue.Empty:
            logger.debug("the queue is empty")
            
    def chmod(self, path, mode):
        logger.debug("chmod '%s' '%i'" % (path, mode))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("chmod '%s' '%i' ENOENT" % (path, mode))
                raise FuseOSError(errno.ENOENT)
            attr = self.get_metadata(path, 'attr')
            if attr < 0:
                return attr
            if attr['st_mode'] != mode:
                attr['st_mode'] = mode
                self.set_metadata(path, 'attr')
            return 0

    def chown(self, path, uid, gid):
        logger.debug("chown '%s' '%i' '%i'" % (path, uid, gid))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("chown '%s' '%i' '%i' ENOENT" % (path, uid, gid))
                raise FuseOSError(errno.ENOENT)
            attr = self.get_metadata(path, 'attr')
            if attr < 0:
                return attr
            changed = False
            if uid != -1 and attr['st_uid'] != uid:
                attr['st_uid'] = uid
                changed = True
            if gid != -1 and attr['st_gid'] != gid:
                attr['st_gid'] = gid
                changed = True
            if changed:
                self.set_metadata(path, 'attr')
            return 0

    def utimens(self, path, times=None):
        logger.debug("utimens '%s' '%s'" % (path, times))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("utimens '%s' '%s' ENOENT" % (path, times))
                raise FuseOSError(errno.ENOENT)
            now = get_current_time()
            atime, mtime = times if times else (now, now)
            attr = self.get_metadata(path, 'attr')
            if attr < 0:
                return attr
            attr['st_atime'] = atime
            attr['st_mtime'] = mtime
            self.set_metadata(path, 'attr')
            return 0

    def getxattr(self, path, name, position=0):
        logger.debug("getxattr '%s' '%s' '%i'" % (path, name, position))
        if self.cache.is_empty(path):
            logger.debug("getxattr '%s' '%s' '%i' ENOENT" % (path, name, position))
            raise FuseOSError(errno.ENOENT)
        if name == 'yas3fs.bucket':
            return self.s3_bucket_name
        elif name == 'yas3fs.key':
            key = self.get_key(path)
            if key:
                return key.key
        elif name == 'yas3fs.URL':
            key = self.get_key(path)
            if key:
                tmp_key = copy.copy(key)
                tmp_key.metadata = {} # To remove unnecessary metadata headers
                return tmp_key.generate_url(expires_in=0, headers=self.default_headers, query_auth=False)
        xattr = self.get_metadata(path, 'xattr')
        if xattr == None:
            logger.debug("getxattr <- '%s' '%s' '%i' ENOENT" % (path, name, position))
            raise FuseOSError(errno.ENOENT)
        if name == 'yas3fs.signedURL':
            key = self.get_key(path)
            if key:
                try:
                    seconds = int(xattr['yas3fs.expiration'])
                except KeyError:
                    seconds = self.default_expiration
                tmp_key = copy.copy(key)
                tmp_key.metadata = {} # To remove unnecessary metadata headers
                return tmp_key.generate_url(expires_in=seconds, headers=self.default_headers)
        elif name == 'yas3fs.expiration':
            key = self.get_key(path)
            if key:
                if name not in xattr:
                    return str(self.default_expiration) + ' (default)'
        try:
            return xattr[name]
        except KeyError:
            if self.darwin:
                raise FuseOSError(errno.ENOENT) # Should return ENOATTR
            else:
                return '' # Should return ENOATTR

    def listxattr(self, path):
        logger.debug("listxattr '%s'" % (path))
        if self.cache.is_empty(path):
            logger.debug("listxattr '%s' ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)
        xattr = self.get_metadata(path, 'xattr')
        if xattr == None:
            logger.debug("listxattr <- '%s' '%s' '%i' ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)
        return set(self.yas3fs_xattrs + xattr.keys())

    def removexattr(self, path, name):
        logger.debug("removexattr '%s''%s'" % (path, name))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("removexattr '%s' '%s' ENOENT" % (path, name))
                raise FuseOSError(errno.ENOENT)
            xattr = self.get_metadata(path, 'xattr')
            try:
                del xattr[name]
                self.set_metadata(path, 'xattr')
            except KeyError:
                if name not in self.yas3fs_xattrs:
                    logger.debug("removexattr '%s' '%s' should ENOATTR" % (path, name))
                    if self.darwin:
                        raise FuseOSError(errno.ENOENT) # Should return ENOATTR
                    else:
                        return '' # Should return ENOATTR
            return 0

    def setxattr(self, path, name, value, options, position=0):
        logger.debug("setxattr '%s' '%s'" % (path, name))
        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("setxattr '%s' '%s' ENOENT" % (path, name))
                raise FuseOSError(errno.ENOENT)
            if name in [ 'yas3fs.bucket', 'yas3fs.key', 'yas3fs.signedURL30d' ]:
                return 0 # Do nothing    
            xattr = self.get_metadata(path, 'xattr')
            if xattr < 0:
                return xattr
            if name not in xattr or xattr[name] != value:
                xattr[name] = value
                self.set_metadata(path, 'xattr')
            return 0

    def statfs(self, path):
        logger.debug("statfs '%s'" % (path))
        """Returns a dictionary with keys identical to the statvfs C
           structure of statvfs(3).
           The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
           On Mac OS X f_bsize and f_frsize must be a power of 2
           (minimum 512)."""
        return {
            "f_namemax" : 512,
            "f_bsize" : 1024 * 1024,
            "f_blocks" : 1024 * 1024 * 1024,
            "f_bfree" : 1024 * 1024 * 1024,
            "f_bavail" : 1024 * 1024 * 1024,
            "f_files" : 1024 * 1024 * 1024,
            "f_favail" : 1024 * 1024 * 1024,
            "f_ffree" : 1024 * 1024 * 1024
            }
        return {}

class TracebackLoggingThread(threading.Thread):
    def run(self):
        try:
            super(TracebackLoggingThread, self).run()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            logger.exception("Uncaught Exception in Thread")
            raise

### Utility functions

def error_and_exit(error, exitCode=1):
    logger.error(error + ", use -h for help.")
    exit(exitCode)

def create_dirs(dirname):
    logger.debug("create_dirs '%s'" % dirname)
    try:
        os.makedirs(dirname)
        logger.debug("create_dirs '%s' done" % dirname)
    except OSError as exc: # Python >2.5                                                                 
        if exc.errno == errno.EEXIST and os.path.isdir(dirname):
            logger.debug("create_dirs '%s' already there" % dirname)
            pass
        else:
            raise

def remove_empty_dirs(dirname):
    logger.debug("remove_empty_dirs '%s'" % (dirname))
    try:
        os.removedirs(dirname)
        logger.debug("remove_empty_dirs '%s' done" % (dirname))
    except OSError as exc: # Python >2.5
        if exc.errno == errno.ENOTEMPTY:
            logger.debug("remove_empty_dirs '%s' not empty" % (dirname))
            pass
        else:
            raise

def create_dirs_for_file(filename):
    logger.debug("create_dirs_for_file '%s'" % filename)
    dirname = os.path.dirname(filename)
    create_dirs(dirname)

def remove_empty_dirs_for_file(filename):
    logger.debug("remove_empty_dirs_for_file '%s'" % filename)
    dirname = os.path.dirname(filename)
    remove_empty_dirs(dirname)

def get_current_time():
    return time.mktime(time.gmtime())

def get_uid_gid():
    uid, gid, pid = fuse_get_context()
    return int(uid), int(gid)

def thread_is_not_alive(t):
    return t == None or not t.is_alive()

def custom_sys_excepthook(type, value, traceback):
    logger.exception("Uncaught Exception")

### Main

def main():

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
    parser.add_argument('--cache-on-disk', metavar='N', type=int, default=0,
                        help='use disk (instead of memory) cache for files greater than the given size in bytes ' +
                        '(default is %(default)s bytes)')
    parser.add_argument('--cache-check', metavar='N', type=int, default=5,
                        help='interval between cache size checks in seconds (default is %(default)s seconds)')
    parser.add_argument('--s3-num', metavar='N', type=int, default=32,
                        help='number of parallel S3 calls (0 to disable writeback, default is %(default)s)')
    parser.add_argument('--download-num', metavar='N', type=int, default=4,
                        help='number of parallel downloads (default is %(default)s)')
    parser.add_argument('--prefetch-num', metavar='N', type=int, default=2,
                        help='number of parallel prefetching downloads (default is %(default)s)')
    parser.add_argument('--buffer-size', metavar='N', type=int, default=10240,
                        help='download buffer size in KB (0 to disable buffering, default is %(default)s KB)')
    parser.add_argument('--buffer-prefetch', metavar='N', type=int, default=0,
                        help='number of buffers to prefetch (default is %(default)s)')
    parser.add_argument('--no-metadata', action='store_true',
                        help='don\'t write user metadata on S3 to persist file system attr/xattr')
    parser.add_argument('--prefetch', action='store_true',
                        help='download file/directory content as soon as it is discovered ' +
                        '(doesn\'t download file content if download buffers are used)')
    parser.add_argument('--mp-size',metavar='N', type=int, default=100,
                        help='size of parts to use for multipart upload in MB ' +
                        '(default value is %(default)s MB, the minimum allowed by S3 is 5 MB)')
    parser.add_argument('--mp-num', metavar='N', type=int, default=4,
                        help='max number of parallel multipart uploads per file ' +
                        '(0 to disable multipart upload, default is %(default)s)')
    parser.add_argument('--mp-retries', metavar='N', type=int, default=3,
                        help='max number of retries in uploading a part (default is %(default)s)')
    parser.add_argument('--id',
                        help='a unique ID identifying this node in a cluster (default is a UUID)')
    parser.add_argument('--mkdir', action='store_true',
                        help='create mountpoint if not found (and create intermediate directories as required)')
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
    parser.add_argument('-l', '--log', metavar='FILE',
                        help='filename for logs')
    parser.add_argument('-f', '--foreground', action='store_true',
                        help='run in foreground')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='show debug info')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s {version}'.format(version=__version__))

    options = parser.parse_args()

    global logger
    logger = logging.getLogger('yas3fs')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    if options.log: # Rotate log files at 100MB size
        logHandler = logging.handlers.RotatingFileHandler(options.log, maxBytes=100*1024*1024, backupCount=10)
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

    sys.excepthook = custom_sys_excepthook # This is not working for new threads that start afterwards
        
    logger.debug("options = %s" % options)

    if options.mkdir:
        create_dirs(options.mountpoint)

    mount_options = {
        'mountpoint':options.mountpoint,
        'fsname':'yas3fs',
        'foreground':options.foreground,
        'allow_other':True,
        'auto_cache':True,
        'atime':False,
        'max_read':131072,
        'max_write':131072,
        'max_readahead':131072,
        'direct_io':True
        }

    if options.uid:
        mount_options['uid'] = options.uid
    if options.gid:
        mount_options['gid'] = options.gid
    if options.umask:
        mount_options['umask'] = options.umask
    if options.read_only:
        mount_options['ro'] = True

    options.darwin = (sys.platform == "darwin")
    if options.darwin:
        mount_options['volname'] = os.path.basename(options.mountpoint)
        mount_options['noappledouble'] = True
        mount_options['daemon_timeout'] = 3600
        # mount_options['auto_xattr'] = True # To use xattr
        # mount_options['local'] = True # local option is quite unstable
    else:
        mount_options['big_writes'] = True # Not working on OSX

    fuse = FUSE(YAS3FS(options), **mount_options)

if __name__ == '__main__':

    main()
