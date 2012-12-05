#!/usr/bin/python

"""
Yet Another S3-backed File System, or yas3fs
is a FUSE file system that is designed for speed
caching data locally and using SNS to notify
other nodes for changes that need cache invalidation.
"""

import errno  
import stat  
import time
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
import M2Crypto
import base64
import hashlib
import logging
import signal
import io
import re

import boto
import boto.s3        
import boto.sns
import boto.sqs
import boto.utils

from sys import argv, exit
from optparse import OptionParser

from boto.s3.key import Key 

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context

class LinkedListElement():
    """ The element of a linked list."""
    def __init__(self, value, prev=None, next=None):
        self.value = value
        self.prev = prev
        self.next = next
    def delete(self):
        self.prev.next = self.next
        self.next.prev = self.prev
        value = self.value
        del self
        return value

class LinkedList():
    """ A linked list that is used by yas3fs as a LRU index
    for the file system cache."""
    def __init__(self):
        self.head = LinkedListElement(None)
        self.tail = LinkedListElement(None)
        self.head.next = self.tail
        self.tail.prev = self.head
        self.index = {}
        self.lock = threading.RLock()
    def append(self, value):
        with self.lock:
            new = LinkedListElement(value, self.tail.prev, self.tail)
            self.tail.prev.next = new
            self.tail.prev = new
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
                return True
            else:
                return False
    def move_to_the_tail(self, value):
        with self.lock:
            if self.delete(value):
                self.append(value)

class FSCache():
    """ File System Cache """
    def __init__(self):
        self.lock = threading.RLock()
        self.reset_all()
    def reset_all(self):
        with self.lock:
            self.entries = {}
            self.lru = LinkedList()
            self.props_size = 0
    def get_memory_usage(self):
        return len(self.entries), self.props_size
    def add(self, path):
        with self.lock:
            if not self.has(path):
                self.entries[path] = {}
                self.lru.append(path)
    def delete(self, path, prop=None):
        with self.lock:
            if path in self.entries:
        	if prop == None:
        	    for prop in self.entries[path]:
        		self.props_size -= sys.getsizeof(self.entries[path][prop])
        	    del self.entries[path]
        	    self.lru.delete(path)
        	else:
        	    if prop in self.entries[path]:
        		self.props_size -= sys.getsizeof(self.entries[path][prop])
        		del self.entries[path][prop]
    def rename(self, path, new_path):
        with self.lock:
            if path in self.entries:
                self.delete(new_path)
                self.entries[new_path] = self.entries[path]
                self.delete(new_path, 'key') # cannot be renamed
                self.lru.append(new_path)
                del self.entries[path]
                self.lru.delete(path)

    def get(self, path, prop=None):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        try:
            if prop == None:
                return self.entries[path]
            else:
                if prop in self.entries[path]:
                    return self.entries[path][prop]
        except KeyError:
            return None
    def set(self, path, prop, value):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        with self.lock:
            if path in self.entries:
        	if prop in self.entries[path]:
        	    self.props_size -= sys.getsizeof(self.entries[path][prop])
        	self.entries[path][prop] = value
        	self.props_size += sys.getsizeof(self.entries[path][prop])
        	return True
            else:
        	return False
    def reset(self, path):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        with self.lock:
            if path in self.entries:
        	for prop in self.entries[path]:
        	    self.props_size -= sys.getsizeof(self.entries[path][prop])
        	self.entries[path] = {}
    def inc(self, path, prop):
        with self.lock:
            if path in self.entries:
        	if prop in self.entries[path]:
        	    self.set(path, prop, self.entries[path][prop] + 1)
        	else:
        	    self.set(path, prop, 1)
    def dec(self, path, prop):
        with self.lock:
            if path in self.entries:
        	if prop in self.entries[path]:
        	    if self.entries[path][prop] > 1:
        		self.set(path, prop, self.entries[path][prop] - 1)
        	    else:
        		self.delete(path, prop)
    def has(self, path, prop=None):
        if prop == None:
            if path in self.entries:
                return True
            return False
        else:
            try:
                if prop in self.entries[path]:
                    return True
            except KeyError:
                pass
            return False
    def is_empty(self, path, prop=None): # A wrapper to improve readability
        return not self.get(path, prop)

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
            if message_type == 'Notification':
        	changes = message_content['Message']
        	logger.debug('changes = %s' % changes)
                self.server.fs.sync_cache(changes)
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

class YAS3FS(LoggingMixIn, Operations):
    """ Main FUSE Operations class for fusepy """
    def __init__(self, options):
        # Some constants
        ### self.http_listen_path_length = 30

        # Initialization
        self.cache = FSCache()
        self.publish_queue = Queue.Queue()

        global debug
        debug = options.debug

        self.aws_region = options.region # Not used by S3

        if not options.url:
            errorAndExit("The S3 path to mount in URL format must be provided")
        s3url = urlparse.urlparse(options.url.lower())
        if s3url.scheme != 's3':
            errorAndExit("The S3 path to mount must be in URL format: s3://BUCKET/PATH")
        self.s3_bucket_name = s3url.netloc
        logger.info("S3 bucket: '%s'" % self.s3_bucket_name)
        self.s3_prefix = s3url.path.strip('/')
        logger.info("S3 prefix: '%s'" % self.s3_prefix)
        self.sns_topic_arn = options.topic
        if self.sns_topic_arn:
            logger.info("AWS region for SNS/SQS: '" + self.aws_region + "'")
            logger.info("SNS topic ARN: '%s'" % self.sns_topic_arn)
        self.sqs_queue_name = options.queue # must be different for each client
        self.new_queue = options.new_queue
        self.queue_wait_time = int(options.queue_wait_time)
        self.queue_polling_interval = int(options.queue_polling_interval)
        if self.sqs_queue_name:
            logger.info("SQS queue name: '%s'" % self.sqs_queue_name)
        if self.sqs_queue_name or self.new_queue:
            logger.info("SQS queue wait time (in seconds): '%i'" % self.queue_wait_time)
            logger.info("SQS queue polling interval (in seconds): '%i'" % self.queue_polling_interval)
        self.max_num_entries = int(options.cache_max_num_entries)
        logger.info("Cache max entries: '%i'" % self.max_num_entries)
        self.max_props_size = int(options.cache_max_props_size) * (1024 * 1024) # To convert MB to bytes
        logger.info("Cache max size (in bytes): '%i'" % self.max_props_size)
        self.check_memory_interval = int(options.cache_check_interval) # seconds
        logger.info("Cache check memory interval (in seconds): '%i'" % self.check_memory_interval)
        if options.ec2_hostname:
            instance_metadata = boto.utils.get_instance_metadata() # This is very slow (to fail) if used outside of EC2
            self.hostname = instance_metadata['public-hostname']
        else:
            self.hostname = options.hostname
        if self.hostname:
            logger.info("Hostname to listen to SNS HTTP notifications: '%s'" % self.hostname)
        self.sns_http_port = int(options.port or '0')
        if options.port:
            logger.info(" TCP port to listen to SNS HTTP notifications: '%i'" % self.sns_http_port)
        self.buffer_size = int(options.buffer_size) * 1024 # To convert KB to bytes
        logger.info("Download buffer size (in KB, 0 to disable buffering): '%i'" % self.buffer_size)
        self.write_metadata = options.write_metadata
        logger.info("Write metadata (file system attr/xattr) on S3: '%s'" % str(self.write_metadata))
        self.prefetch = options.prefetch
        logger.info("Download prefetch: '%s'" % str(self.prefetch))

        # AWS Initialization
        try:
            self.s3 = boto.connect_s3() # Not using AWS region for S3, got an error otherwise, depending on the bucket
        except boto.exception.NoAuthHandlerFound:
            errorAndExit("no AWS credentials found")
        if not self.s3:
            errorAndExit("no S3 connection")
        try:
            self.s3_bucket = self.s3.get_bucket(self.s3_bucket_name)
        except boto.exception.S3ResponseError:
            errorAndExit("S3 bucket not found")

        if self.sns_topic_arn:
            if not self.aws_region in (r.name for r in boto.sns.regions()):
                errorAndExit("wrong AWS region '%s' for SNS" % self.aws_region)
            self.sns = boto.sns.connect_to_region(self.aws_region)
            if not self.sns:
                errorAndExit("no SNS connection")
            try:
                topic_attributes = self.sns.get_topic_attributes(self.sns_topic_arn)
            except boto.exception.BotoServerError:
                errorAndExit("SNS topic ARN not found in region '%s' " % self.aws_region)
            if not self.sqs_queue_name and not self.new_queue:
                if not (self.hostname and self.sns_http_port):
                    errorAndExit("With and SNS topic either the SQS queue name or the hostname and port to listen to SNS HTTP notifications must be provided")

        if self.sqs_queue_name or self.new_queue:
            if not self.sns_topic_arn:
                errorAndExit("The SNS topic must be provided when an SQS queue is used")
            if not self.aws_region in (r.name for r in boto.sqs.regions()):
                errorAndExit("wrong AWS region '" + self.aws_region + "' for SQS")
            self.sqs = boto.sqs.connect_to_region(self.aws_region)
            if not self.sqs:
                errorAndExit("no SQS connection")
            if self.new_queue:
                pattern = re.compile('[\W_]+') # Alphanumeric characters only
                queue_name = pattern.sub('', self.s3_bucket_name) + '-' + pattern.sub('', self.s3_prefix) + '-'
                i = 1
                while True:
                    try:
                        while self.sqs.lookup(queue_name + str(i)):
                            i += 1
                        self.sqs_queue_name = queue_name + str(i)
                        self.queue = self.sqs.create_queue(self.sqs_queue_name)
                        break
                    except boto.exception.SQSError:
                        i += 1
            else:
                self.queue =  self.sqs.lookup(self.sqs_queue_name)
            if not self.queue:
                self.queue = self.sqs.create_queue(self.sqs_queue_name)
            logger.info("SQS queue name (new): '%s'" % self.sqs_queue_name)
            self.queue.set_message_class(boto.sqs.message.RawMessage) # There is a bug with the default Message class in boto

        self.unique_id = options.id or self.hostname or self.sqs_queue_name
        if self.unique_id:
            logger.info("Unique node ID: '%s'" % self.unique_id)
                
        if self.hostname or self.sns_http_port:
            if not self.sns_topic_arn:
                errorAndExit("The SNS topic must be provided when the hostname/port to listen to SNS HTTP notifications is given")            

        if self.sns_http_port:
            if not self.hostname:
                errorAndExit("The hostname must be provided with the port to listen to SNS HTTP notifications")
            ### self.http_listen_path = '/sns/' + base64.urlsafe_b64encode(os.urandom(self.http_listen_path_length))
            self.http_listen_path = '/sns'
            self.http_listen_url = "http://%s:%i%s" % (self.hostname, self.sns_http_port, self.http_listen_path)

        signal.signal(signal.SIGINT, self.handler)

    def init(self, path):
        self.publish_thread = threading.Thread(target=self.publish_changes)
        self.publish_thread.daemon = True
        self.publish_thread.start()

        if self.sqs_queue_name:
            self.queue_listen_thread = threading.Thread(target=self.listen_for_changes_over_sqs)
            self.queue_listen_thread.daemon = True
            self.queue_listen_thread.start()
            response = self.sns.subscribe_sqs_queue(self.sns_topic_arn, self.queue)
            self.sqs_subscription = response['SubscribeResponse']['SubscribeResult']['SubscriptionArn']
            logger.debug('SNS SQS subscription = %s' % self.sqs_subscription)


        if self.sns_http_port:
            self.http_listen_thread = threading.Thread(target=self.listen_for_changes_over_http)
            self.http_listen_thread.daemon = True
            self.http_listen_thread.start()
            self.sns.subscribe(self.sns_topic_arn, 'http', self.http_listen_url)

        self.memory_thread = threading.Thread(target=self.check_memory_usage)
        self.memory_thread.daemon = True
        self.memory_thread.start()

    def handler(signum, frame):
        self.destroy('/')

    def flush_all_cache(self):
        with self.cache.lock:
            for path in self.cache.entries:
                if self.cache.has(path, 'change'):
                    self.flush(path)
 
    def destroy(self, path):
        # Cleanup for unmount
        logger.info('file system unmount')
        try:
            self.httpd.shutdown()
        except AttributeError:
            pass
        self.flush_all_cache()
        try:
            if self.sqs_subscription:
                self.sns.unsubscribe(self.sqs_subscription)
                if self.new_queue:
                    self.sqs.delete_queue(self.queue, force_deletion=True)
        except AttributeError:
            pass
        try:
            if self.http_subscription:
                self.sns.unsubscribe(self.http_subscription)
        except AttributeError:
            pass

    def listen_for_changes_over_http(self):
        logger.info("listening on %s" % self.http_listen_url)
        server_class = SNS_HTTPServer
        handler_class = SNS_HTTPRequestHandler
        server_address = ('', self.sns_http_port)
        self.httpd = server_class(server_address, handler_class)
        self.httpd.set_fs(self)
        self.httpd.serve_forever()

    def listen_for_changes_over_sqs(self):
        logger.info("listening on queue %s" % self.queue.name)
        while True:
            if self.queue_wait_time > 0:
                messages = self.queue.get_messages(10, wait_time_seconds=self.queue_wait_time) # Using SQS long polling, needs boto > 2.6.0
            else:
                messages = self.queue.get_messages(10)
            if messages:
                for m in messages:
                    content = json.loads(m.get_body())
                    changes = content['Message'].encode('ascii')
                    self.sync_cache(changes)
                    m.delete()
            else:
                time.sleep(self.queue_polling_interval)

    def invalidate_cache(self, path, md5=None):
        with self.cache.lock:
            self.cache.delete(path, 'key')
            if self.cache.is_empty(path):
                self.cache.delete(path)
            elif self.cache.has(path, 'data'):
                self.cache.set(path, 'new-data', md5)
                if self.prefetch:
                    t = threading.Thread(target=self.getattr, args=(path, None, False))
                    t.daemon = True
                    t.start()

    def delete_cache(self, path, tryPrefetch):
        with self.cache.lock:
            self.cache.delete(path)
            self.reset_parent_readdir(path)
            if tryPrefetch and self.prefetch:
                t = threading.Thread(target=self.getattr, args=(path, None, False))
                t.daemon = True
                t.start()

    def sync_cache(self, changes):
        c = json.loads(changes)
        if not c[0] == self.unique_id: # discard message coming from itself
            if c[1] in ( 'mkdir', 'mknod', 'symlink' ) and c[2] != None:
                self.delete_cache(c[2], True)
            elif c[1] in ( 'rmdir', 'unlink' ) and c[2] != None:
                self.delete_cache(c[2], False)
            elif c[1] == 'rename' and c[2] != None and c[3] != None:
                self.delete_cache(c[2], False)
                self.delete_cache(c[3], True)
            elif c[1] == 'flush':
                if c[2] != None:
                    self.invalidate_cache(c[2], c[3])
                else: # Invalidate all the cached data
                    for path in self.cache.entries:
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
                        errorAndExit("The S3 path to mount must be in URL format: s3://BUCKET/PATH")
                    self.s3_bucket_name = s3url.netloc
                    logger.info("S3 bucket: '%s'" % self.s3_bucket_name)
                    self.s3_prefix = s3url.path.strip('/')
                    logger.info("S3 prefix: '%s'" % self.s3_prefix)
                    try:
                        self.s3_bucket = self.s3.get_bucket(self.s3_bucket_name)
                    except boto.exception.S3ResponseError:
                        errorAndExit("S3 bucket not found")
            elif c[1] == 'cache':
                if c[2] == 'entries' and c[3] > 0:
                    self.max_num_entries = int(c[3])
            elif c[2] == 'size' and c[3] > 0:
                self.max_props_size = int(c[3]) * (1024 * 1024) # MB
            elif c[1] == 'buffer' and c[3] >= 0:
                if c[2] == 'size':
                    self.buffer_size = int(c[3]) * 1024 # KB
            elif c[1] == 'prefetch':
                if c[2] == 'on':
                    self.prefetch = True
                elif c[2] == 'off':
                    self.prefetch = False

    def publish_changes(self):
        while self.sns_topic_arn:
            message = self.publish_queue.get()
            message.insert(0, self.unique_id)
            full_message = json.dumps(message)
            self.sns.publish(self.sns_topic_arn, full_message.encode('ascii'))
            self.publish_queue.task_done()
                
    def publish(self, message):
        self.publish_queue.put(message)

    def check_memory_usage(self):
        while True:
            num_entries, props_size = self.cache.get_memory_usage()
            logger.debug("num_entries, props_size: %i, %i" % (num_entries, props_size))
            if num_entries > self.max_num_entries or props_size > self.max_props_size:
                with self.cache.lock:
                    path = self.cache.lru.popleft()
                    logger.debug("purge: %s ?" % path)
                    if self.cache.get(path, 'open') or self.cache.has(path, 'change'):
                        logger.debug("purge: no")
                        logger.debug("open: %i" % self.cache.get(path, 'open'))
                        logger.debug("change: %s" % self.cache.has(path, 'change'))
                        self.cache.lru.append(path)
                    else:
                        logger.debug("purge: yes")
                        self.cache.delete(path)
            else: 
                time.sleep(self.check_memory_interval)

    def add_to_parent_readdir(self, path):
        (parent_path, dir) = os.path.split(path)
        if self.cache.has(parent_path, 'readdir') and self.cache.get(parent_path, 'readdir').count(dir) == 0:
            self.cache.get(parent_path, 'readdir').append(dir)

    def remove_from_parent_readdir(self, path):
        (parent_path, dir) = os.path.split(path)
        if self.cache.has(parent_path, 'readdir') and self.cache.get(parent_path, 'readdir').count(dir) > 0:
            self.cache.get(parent_path, 'readdir').remove(dir)

    def reset_parent_readdir(self, path):
        (parent_path, dir) = os.path.split(path)
        self.cache.delete(parent_path, 'readdir')

    def join_prefix(self, path):
        if self.s3_prefix == '':
            return path[1:] # Remove beginning "/"
        else:
            return self.s3_prefix + path

    def get_key(self, path):
        key = self.cache.get(path, 'key')
        if key:
            return key
        key = self.s3_bucket.get_key(self.join_prefix(path))
        if not key:
            key = self.s3_bucket.get_key(self.join_prefix(path + '/'))
        self.cache.set(path, 'key', key)
        return key

    def get_metadata(self, path, metadata_name, key=None, raiseError=True):
        if not self.cache.has(path, metadata_name):
            if not key:
                key = self.get_key(path)
            if not key:
                if path == '/': # First time mount of a new file system
                    self.cache.delete(path)
                    self.mkdir('', 0755)
                    self.cache.rename('', path)
                    return self.cache.get(path, metadata_name)
                else:
                    full_path = self.join_prefix(path + '/')
                    key_list = self.s3_bucket.list(full_path) # Don't need to set a delimeter here
                    if len(list(key_list)) == 0:
                        self.cache.add(path) # It is empty to cache further checks
                        if raiseError:
                            raise FuseOSError(errno.ENOENT)
                        else:
                            return None
            metadata_values = {}
            if key:
                s = key.get_metadata(metadata_name)
            else:
                s = None
            if metadata_name == 'attr': # Custom exception(s)
                if key:
                    metadata_values['st_size'] = str(key.size)
                else:
                    metadata_values['st_size'] = '0'                
                if not s: # Set default attr to browse any S3 bucket TODO directories
		    uid, gid, pid = fuse_get_context()
 		    metadata_values['st_uid'] = str(int(uid))
 		    metadata_values['st_gid'] = str(int(gid))
                    if key and key.name != '' and key.name[-1] != '/':
                        metadata_values['st_mode'] = str(stat.S_IFREG | 0755)
                    else:
                        metadata_values['st_mode'] = str(stat.S_IFDIR | 0755)
                    if key and key.last_modified:
                        now = str(time.mktime(time.strptime(key.last_modified, "%a, %d %b %Y %H:%M:%S %Z")))
                    else:
                        now = str(time.time()) # Do something better ??? 
                    metadata_values['st_mtime'] = now
                    metadata_values['st_atime'] = now
                    metadata_values['st_ctime'] = now
	    if s:
		for kv in s.split(';'):
		    k, v = kv.split('=')
		    metadata_values[k] = v
	    self.cache.add(path)
	    self.cache.set(path, metadata_name, metadata_values)
	return self.cache.get(path, metadata_name)

    def set_metadata(self, path, metadata_name, metadata_values, key=None):
	self.cache.set(path, metadata_name, metadata_values)
        if self.write_metadata and (key or not self.cache.has(path, 'change')): # No change in progress, I should write now
	    if not key:
                key = self.get_key(path)
	    if key:
		if metadata_values:
		    s = ';'.join(['%s=%s' % (k,v) for k,v in metadata_values.iteritems()
				  if not (metadata_name == 'attr' and k == 'st_size')])
		    key.metadata[metadata_name] = s
		elif metadata_name in key.metadata:
		    del key.metadata[metadata_name]
                if not self.cache.has(path, 'change'):
                    md = key.metadata
                    md['Content-Type'] = key.content_type # Otherwise we loose the Content-Type with Copy
                    key.copy(key.bucket.name, key.name, md, preserve_acl=False) # Do I need to preserve ACL?
                    self.publish(['md', metadata_name, path])

    def getattr(self, path, fh=None, raiseError=True):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
	attr = self.get_metadata(path, 'attr', raiseError=raiseError)
        if attr == None:
            return None
	st = {}
	st['st_mode'] = int(attr['st_mode'])
	st['st_atime'] = float(attr['st_atime']) # Should I update this ???
	st['st_mtime'] = float(attr['st_mtime']) # Should I use k.last_modified ???
	st['st_ctime'] = float(attr['st_ctime'])
        st['st_uid'] = int(attr['st_uid'])
        st['st_gid'] = int(attr['st_gid'])
	st['st_size'] = int(attr['st_size'])
        if stat.S_ISDIR(st['st_mode']) and st['st_size'] == 0:
            st['st_size'] = 4096 # For compatibility...
	st['st_nlink'] = 1 # Something better TODO ???
        if not stat.S_ISDIR(st['st_mode']) and self.prefetch:
            self.check_data(path) # Prefetch
	return st

    def readdir(self, path, fh=None):

	if self.cache.has(path) and self.cache.is_empty(path):
	    raise FuseOSError(errno.ENOENT)

	self.cache.add(path)

	if not self.cache.has(path, 'readdir'):
	    full_path = self.join_prefix(path)
            if full_path != '' and full_path[-1] != '/':
                full_path += '/'
	    key_list = self.s3_bucket.list(full_path, '/')
	    dirs = ['.', '..']
	    for k in key_list:
		d = k.name.encode('ascii')[len(full_path):]
		if len(d) > 0:
		    if d[-1] == '/':
			d = d[:-1]
		    dirs.append(d)
	    self.cache.set(path, 'readdir', dirs)

	return sorted(self.cache.get(path, 'readdir'))

    def mkdir(self, path, mode):
	if self.cache.has(path) and not self.cache.is_empty(path):
	    return FuseOSError(errno.EEXIST)
	k = self.get_key(path)
	if k:
	    return FuseOSError(errno.EEXIST)
	now = str(time.time())
	uid, gid, pid = fuse_get_context()
	attr = {}
	attr['st_uid'] = str(int(uid))
	attr['st_gid'] = str(int(gid))
	attr['st_atime'] = now
	attr['st_mtime'] = now
	attr['st_ctime'] = now
	attr['st_size'] = '0'
	attr['st_mode'] = str(int(stat.S_IFDIR | mode))
	self.cache.delete(path)
	self.cache.add(path)
	self.cache.set(path, 'change', True)
	k = Key(self.s3_bucket)
	self.set_metadata(path, 'attr', attr, k)
	self.set_metadata(path, 'xattr', {}, k)
	k.key = self.join_prefix(path + '/')
	k.set_contents_from_string('', headers={'Content-Type': 'application/x-directory'})
        self.cache.set(path, 'key', k)
	self.cache.delete(path, 'change')
	self.cache.set(path, 'readdir', ['.', '..']) # the directory is empty
	if path != '':
            self.add_to_parent_readdir(path)
            self.publish(['mkdir', path])
	return 0
 
    def symlink(self, path, link):
	if self.cache.has(path) and not self.cache.is_empty(path):
	    return FuseOSError(errno.EEXIST)
	k = self.get_key(path)
	if k:
	    return FuseOSError(errno.EEXIST)
	now = str(time.time())
	uid, gid, pid = fuse_get_context()
	attr = {}
	attr['st_uid'] = str(int(uid))
	attr['st_gid'] = str(int(gid))
	attr['st_atime'] = now
	attr['st_mtime'] = now
	attr['st_ctime'] = now
	attr['st_size'] = len(link)
	attr['st_mode'] = str(stat.S_IFLNK | 0755)
	self.cache.delete(path)
	self.cache.add(path)
	self.cache.set(path, 'change', True)
	k = Key(self.s3_bucket)
	self.set_metadata(path, 'attr', attr, k)
	self.set_metadata(path, 'xattr', {}, k)
	k.key = self.join_prefix(path)
	k.set_contents_from_string(link, headers={'Content-Type': 'application/x-symlink'})
        self.cache.set(path, 'key', k)
	self.cache.delete(path, 'change')
	self.add_to_parent_readdir(path)
	self.publish(['symlink', path])
	return 0

    def check_data(self, path): 
	if not self.cache.has(path, 'data') or self.cache.has(path, 'new-data'):
	    k = self.get_key(path)
            if not k:
		return False
            if k.size == 0:
                data = io.BytesIO()
                self.cache.set(path, 'data', data)
                self.cache.delete(path, 'new-data')
                with self.cache.lock:
                    if self.cache.has(path, 'data-range'):
                        (range, event) = self.cache.get(path, 'data-range')
                        self.cache.delete(path, 'data-range')
                        event.set()
                return True
	    if self.cache.has(path, 'data'):
                new_md5 = self.cache.get(path, 'new-data')
		md5 = hashlib.md5(self.cache.get(path, 'data').getvalue()).hexdigest()
                etag = k.etag[1:-1]
                if not new_md5 or new_md5 == etag:
                    self.cache.delete(path, 'new-data')
                else: # I'm not sure I got the latest version
                    self.cache.delete(path, 'key')
                    self.cache.set(path, 'new-data', None) # Next time don't check the MD5
		if md5 == etag:
		    return True
            self.cache.delete(path, 'attr')
            if self.buffer_size > 0:
                with self.cache.lock:
                    if self.cache.has(path, 'data-range'):
                        return True
                    self.cache.set(path, 'data-range', (0, threading.Event()))
                data = io.BytesIO()
                self.cache.set(path, 'data', data)
                t = threading.Thread(target=self.get_data, args=(path, data, k))
                t.daemon = True
                t.start()
            else:
                data = io.BytesIO()
                self.cache.set(path, 'data', data)
                k.get_contents_to_file(data)
	return True

    def get_data(self, path, data, key):
        key.BufferSize = min(self.buffer_size, key.size) # Is this an optimization or not?

        for bytes in key:
            with self.cache.lock:
                if self.cache.has(path, 'data-range'):
                    (total, event) = self.cache.get(path, 'data-range')
                else:
                    return
            data.seek(total)
            data.write(bytes)
            total += len(bytes)
            self.cache.set(path, 'data-range', (total, threading.Event()))
            event.set()

        (total, event) = self.cache.get(path, 'data-range')
        self.cache.delete(path, 'data-range')
        event.set()

    def readlink(self, path):
	if self.cache.has(path) and self.cache.is_empty(path):
	    raise FuseOSError(errno.ENOENT)
	self.cache.add(path)
	if stat.S_ISLNK(self.getattr(path)['st_mode']):
	    if not self.check_data(path):
		raise FuseOSError(errno.ENOENT)
            while True:
                range = self.cache.get(path, 'data-range')
                if range == None:
                    break
                range[1].wait()
	    return self.cache.get(path, 'data').getvalue()
	return FuseOSError(errno.EINVAL)
 
    def rmdir(self, path):
	if self.cache.has(path) and self.cache.is_empty(path):
	    raise FuseOSError(errno.ENOENT)
	k = self.get_key(path) # Should I use cache here ???
	if not k:
	    raise FuseOSError(errno.ENOENT)
	full_path = self.join_prefix(path + '/')
	key_list = self.s3_bucket.list(full_path) # Don't need to set a delimeter here
	for l in key_list:
	    if l.name != full_path:
		raise FuseOSError(errno.ENOTEMPTY)
	k.delete()
	self.cache.reset(path) # Cache invalidation
	self.remove_from_parent_readdir(path)
	self.publish(['rmdir', path])
	return 0

    def truncate(self, path, size):
	if self.cache.has(path) and self.cache.is_empty(path):
	    raise FuseOSError(errno.ENOENT)
	self.cache.add(path)
	if not self.check_data(path):
	    raise FuseOSError(errno.ENOENT)
        self.cache.get(path, 'data').truncate(size)
	attr = self.get_metadata(path, 'attr')
	attr['st_size'] = str(size)
	self.cache.set(path, 'change', True)
	self.set_metadata(path, 'attr', attr)
	return 0

    def rename(self, path, new_path):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
        key = self.get_key(path)
        if not key:
            raise FuseOSError(errno.ENOENT)
        new_parent_key = self.get_key(os.path.dirname(new_path))
        if not new_parent_key:
            raise FuseOSError(errno.ENOENT)
        to_copy = {}
        if key.name[-1] == '/':
            key_list = self.s3_bucket.list(key.name)
	    for k in key_list:
                source = k.name.encode('ascii')
		target = self.join_prefix(new_path + source[len(key.name) - 1:])
                to_copy[source] = target
        else:
            to_copy[key.name] = self.join_prefix(new_path)
        for source, target in to_copy.iteritems():
            source_path = source[len(self.s3_prefix):].rstrip('/')
            if source_path[0] != '/':
                source_path = '/' + source_path
            target_path = target[len(self.s3_prefix):].rstrip('/')
            if target_path[0] != '/':
                target_path = '/' + target_path
            self.cache.rename(source_path, target_path)
            key = self.s3_bucket.get_key(source)
            md = key.metadata
            md['Content-Type'] = key.content_type # Otherwise we loose the Content-Type with S3 Copy
            key.copy(key.bucket.name, target, md, preserve_acl=False) # Do I need to preserve ACL?
            key.delete()
            self.publish(['rename', source_path, target_path])
        self.remove_from_parent_readdir(path)
        self.add_to_parent_readdir(new_path)

    def mknod(self, path, mode, dev=None):
	if self.cache.has(file):
	    if not self.cache.is_empty(file):
		return FuseOSError(errno.EEXIST)
	else:
	    k = self.get_key(path)
	    if k:
		return FuseOSError(errno.EEXIST)
	    self.cache.add(path)
	now = str(time.time())
	uid, gid, pid = fuse_get_context()
	attr = {}
	attr['st_uid'] = str(int(uid))
	attr['st_gid'] = str(int(gid))
	attr['st_mode'] = str(stat.S_IFREG | mode)
	attr['st_atime'] = now
	attr['st_mtime'] = now
	attr['st_ctime'] = now
	attr['st_size'] = '0' # New file
	self.cache.set(path, 'change', True)
	self.set_metadata(path, 'attr', attr)
	self.set_metadata(path, 'xattr', {})
	self.cache.set(path, 'data', io.BytesIO(''))
	self.add_to_parent_readdir(path)
	self.publish(['mknod', path])
	return 0

    def unlink(self, path):
	if self.cache.has(path) and self.cache.is_empty(path):
	    raise FuseOSError(errno.ENOENT)
	k = self.get_key(path)
	if not k:
	    raise FuseOSError(errno.ENOENT)
	k.delete()
	self.cache.reset(path)
	self.remove_from_parent_readdir(path)
	self.publish(['unlink', path])
	return 0

    def create(self, path, mode, fi=None):
	return self.open(path, mode)

    def open(self, path, flags):
	self.cache.add(path)
	if not self.check_data(path):
	    self.mknod(path, flags)
	self.cache.inc(path, 'open')
	return 0

    def release(self, path, flags):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
	self.cache.dec(path, 'open')
	return 0

    def read(self, path, length, offset, fh=None):
        if not self.cache.has(path) or (self.cache.has(path) and self.cache.is_empty(path)):
            raise FuseOSError(errno.ENOENT)
        while True:
            range = self.cache.get(path, 'data-range')
            if range == None or range[0] >= offset + length:
                break
            range[1].wait()
	# update atime just in the cache ???
        sio = self.cache.get(path, 'data')
        sio.seek(offset)
        return sio.read(length)

    def write(self, path, data, offset, fh=None):
        if not self.cache.has(path) or (self.cache.has(path) and self.cache.is_empty(path)):
            raise FuseOSError(errno.ENOENT)
	length = len(data)
        while True:
            range = self.cache.get(path, 'data-range')
            if range == None or range[0] >= offset + length:
                break
            range[1].wait()
	with self.cache.lock:
            sio = self.cache.get(path, 'data')
            sio.seek(offset)
            sio.write(data)
	    self.cache.set(path, 'data', sio)
	    self.cache.set(path, 'change', True)
	    now = str(time.time())
	    attr = self.get_metadata(path, 'attr')
            attr['st_size'] = str(max(int(attr['st_size']), offset + length))
            attr['st_mtime'] = now
            attr['st_atime'] = now
            self.set_metadata(path, 'attr', attr)
        return length

    def flush(self, path, fh=None):
        if self.cache.has(path) and not self.cache.is_empty(path) and self.cache.has(path, 'change'):
            k = self.get_key(path)
            if not k:
                k = Key(self.s3_bucket)
                k.key = self.join_prefix(path)
                self.cache.set(path, 'key', k)
            now = str(time.time())
            attr = self.get_metadata(path, 'attr', k)
            attr['st_atime'] = now
            attr['st_mtime'] = now
            self.set_metadata(path, 'attr', attr, k)
            xattr = self.get_metadata(path, 'xattr') # Do something better ???
            self.set_metadata(path, 'xattr', xattr, k)
            type = mimetypes.guess_type(path)[0]
            data = self.cache.get(path, 'data')
            data.seek(0)
            k.set_contents_from_file(data, headers={'Content-Type': type})
            self.cache.delete(path, 'change')
            self.publish(['flush', path, k.etag[1:-1]])
        return 0

    def chmod(self, path, mode):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
        attr = self.get_metadata(path, 'attr')
        if attr < 0:
            return attr
        attr['st_mode'] = str(mode)
        self.set_metadata(path, 'attr', attr)
        return 0

    def chown(self, path, uid, gid):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
        attr = self.get_metadata(path, 'attr')
        if attr < 0:
            return attr
        attr['st_uid'] = str(uid)
        attr['st_gid'] = str(gid)
        self.set_metadata(path, 'attr', attr)
        return 0

    def utime(self, path, times=None):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
        now = time.time()
        atime, mtime = times if times else (now, now)
        attr = self.get_metadata(path, 'attr')
        if attr < 0:
            return attr
        attr['st_atime'] = atime
        attr['st_mtime'] = mtime
        self.set_metadata(path, 'attr', attr)
        return 0

    def getxattr(self, path, name, position=0):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
        xattr = self.get_metadata(path, 'xattr')
        try:
            return xattr[name]
        except KeyError:
            return '' # Should return ENOATTR

    def listxattr(self, path):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
        xattr = self.get_metadata(path, 'xattr')
        return xattr.keys()

    def removexattr(self, path, name):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
        xattr = self.get_metadata(path, 'xattr')
        try:
            del xattr[name]
            self.set_metadata(path, 'xattr', xattr)
        except KeyError:
            return '' # Should return ENOATTR
        return 0

    def setxattr(self, path, name, value, options, position=0):
        if self.cache.has(path) and self.cache.is_empty(path):
            raise FuseOSError(errno.ENOENT)
        xattr = self.get_metadata(path, 'xattr')
        if xattr < 0:
            return xattr
        xattr[name] = value
        self.set_metadata(path, 'xattr', xattr)
        return 0

    def statfs(self, path):
        """Returns a dictionary with keys identical to the statvfs C
           structure of statvfs(3).
           The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
           On Mac OS X f_bsize and f_frsize must be a power of 2
           (minimum 512)."""
        return {
            "f_namemax" : 512,
            "f_bsize" : 128 * 1024,
            "f_blocks" : 1024 * 1024 * 1024,
            "f_bfree" : 1024 * 1024 * 1024,
            "f_bavail" : 1024 * 1024 * 1024,
            "f_files" : 1024 * 1024 * 1024,
            "f_favail" : 1024 * 1024 * 1024,
            "f_ffree" : 1024 * 1024 * 1024
            }
        return {}

def errorAndExit(error, exitCode=1):
    logger.error(error + ", use -h for help.")
    exit(exitCode)

if __name__ == '__main__':

    usage = """%prog <mountpoint> [options]

YAS3FS (Yet Another S3-backed File System) is a Filesystem in Userspace (FUSE) interface to Amazon S3.

It allows to mount an S3 bucket (or a part of it) as a local folder.
For maximum speed all data read from S3 is cached locally on the node.
Access to file content is provided during the download from S3 using buffers.
SNS notifications are used to update other nodes that something has changed on S3 and they need to invalidate their cache.
Notifications can be listened using HTTP or SQS endpoints.
With buffering enabled (the default) files can be accessed during the download from S3.
If the cache grows to its maximum size the less recently accessed files are removed.
AWS credentials can be passed using AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environmental variables.
In an EC2 instance a IAM role can be used to give access to S3/SNS/SQS resources."""

    parser = OptionParser(usage=usage)

    parser.add_option("--url", dest="url",
                      help="the S3 path to mount in s3://BUCKET/PATH format, "
                      + "PATH can be empty, can contain subfolders and is created on first mount if not found in the BUCKET",
                      metavar="URL")
    parser.add_option("--region", dest="region",
                      help="AWS region to use for SNS/SQS (default is %default)",
                      metavar="REGION", default="us-east-1")
    parser.add_option("--topic", dest="topic",
                      help="SNS topic ARN", metavar="ARN")
    parser.add_option("--hostname", dest="hostname",
                      help="hostname to listen to SNS HTTP notifications", metavar="HOST")
    parser.add_option("--ec2-hostname", action="store_true", dest="ec2_hostname", default=False,
                      help="get public hostname from EC2 instance metadata (overrides '--hostname')")
    parser.add_option("--port", dest="port",
                      help="TCP port to listen to SNS HTTP notifications", metavar="N")
    parser.add_option("--queue", dest="queue",
                      help="SQS queue name, a new queue is created if it doesn't exist", metavar="NAME")
    parser.add_option("--new-queue", action="store_true", dest="new_queue", default=False,
                      help="create a new SQS queue that is deleted on unmount (overrides '--queue', queue name is BUCKET-PATH-N with alphanumeric characters only)")
    parser.add_option("--queue-wait", dest="queue_wait_time",
                      help="SQS queue wait time in seconds (using long polling, 0 to disable, default is %default seconds)", metavar="N", default=0)
    parser.add_option("--queue-polling", dest="queue_polling_interval",
                      help="SQS queue polling interval in seconds (default is %default seconds)", metavar="N", default=1)
    parser.add_option("--cache-entries", dest="cache_max_num_entries",
                      help="max number of entries to cache (default is %default entries)", metavar="N", default=1000000)
    parser.add_option("--cache-size", dest="cache_max_props_size",
                      help="max size of the cache in MB (default is %default MB)", metavar="N", default=1024)
    parser.add_option("--cache-check", dest="cache_check_interval",
                      help="interval between cache memory checks in seconds (default is %default seconds)", metavar="N", default=10)
    parser.add_option("--buffer-size", dest="buffer_size",
                      help="download buffer size in KB (0 to disable buffering, default is %default KB)", metavar="N", default=10240)
    parser.add_option("--no-metadata", action="store_false", dest="write_metadata", default=True,
                      help="don't write user metadata on S3 to persist file system attr/xattr")
    parser.add_option("--prefetch", action="store_true", dest="prefetch", default=False,
                      help="start downloading file content as soon as the file is discovered")
    parser.add_option("--id", dest="id",
                      help="a unique ID identifying this node in a cluster, hostname or queue name are used if not provided", metavar="ID")
    parser.add_option("--log", dest="logfile",
                      help="the filename to use for logs", metavar="FILE", default="")
    parser.add_option("--mkdir", action="store_true", dest="mkdir", default=False,
                      help="create mountpoint if not found (create intermediate directories as required)")
    parser.add_option("-f", "--foreground", action="store_true", dest="foreground", default=False,
                      help="run in foreground")
    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False,
                      help="print debug information (implies '-f')")

    (options, args) = parser.parse_args()

    logging.basicConfig()
    logger = logging.getLogger('yas3fs')

    if options.logfile != '':
        logHandler = logging.handlers.RotatingFileHandler(options.logfile, maxBytes=1024*1024, backupCount=10)
        logger.addHandler(logHandler)

    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        
    if len(args) < 1:
        errorAndExit("mountpoint must be provided")
    elif len(args) > 1:
        errorAndExit("not more than one mountpoint must be provided")

    mountpoint = args[0]

    if options.mkdir:
        try:
            os.makedirs(mountpoint)
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(mountpoint):
                pass
            else:
                raise

    if sys.platform == "darwin":
        volume_name = os.path.basename(mountpoint)
        fuse = FUSE(YAS3FS(options), mountpoint, fsname="yas3fs",
                    foreground=options.foreground or options.debug,
                    default_permissions=True, allow_other=True,
                    auto_cache=True,
                    auto_xattr=True, volname=volume_name,
                    noappledouble=True, daemon_timeout=3600,
                    local=True)
    else:
        fuse = FUSE(YAS3FS(options), mountpoint, fsname="yas3fs",
                    foreground=options.foreground or options.debug,
                    default_permissions=True, allow_other=True,
                    auto_cache=True)
