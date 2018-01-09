#!/usr/bin/python

from yas3fs.YAS3FSPlugin import YAS3FSPlugin
import json
import os
import re
import errno
from stat import *

import datetime
import time

'''
Upon upload failure
- a log entry is written w/ metadata
- the cache file is mirrored into a recovery directory ajacent to the cache directory
'''

class RecoverYas3fsPlugin(YAS3FSPlugin):
	def epochseconds_to_iso8601(self, s = None):
		t = None
		if s == None:
			dt = datetime.datetime.now()
		else:
			dt = datetime.datetime.utcfromtimestamp(s)

		# truncates microseconds
		dt = dt.replace(microsecond=0)

		rt = dt.isoformat()
		
		return rt

	def stat_to_dict(self, stat):
		fn_map = {
			'st_mode': (ST_MODE, str),
			'st_ino': (ST_INO, str),
			'st_dev': (ST_DEV, str),
			'st_nlink': (ST_NLINK, str),
			'st_uid': (ST_UID, str),
			'st_gid': (ST_GID, str),
			'st_size': (ST_SIZE, str),
			'st_atime': (ST_ATIME, self.epochseconds_to_iso8601),
			'st_mtime': (ST_MTIME, self.epochseconds_to_iso8601),
			'st_ctime': (ST_CTIME, self.epochseconds_to_iso8601)
		}
		d = {}
		for k in fn_map:
			d[k] = fn_map[k][1](stat[fn_map[k][0]])
		return d

	# k,v tuple
	def s3key_json_filter(self, x):
		if x[0] in ('s3bucket'):
			return False
		return True

	def __init__(self, yas3fs, logger=None):
		super(RecoverYas3fsPlugin, self).__init__(yas3fs, logger)
		self.recovery_path = yas3fs.cache.cache_path + "/recovery"
		self.cache = yas3fs.cache

		self.logger.info("PLUGIN Recovery Path '%s'"% self.recovery_path)

		#---------------------------------------------
		# makes a recovery directory
		try:
			os.makedirs(self.recovery_path)
			self.logger.debug("PLUGIN created recovery path '%s' done" % self.recovery_path)
		except OSError as exc: # Python >2.5                                        
			if exc.errno == errno.EEXIST and os.path.isdir(self.recovery_path):
				self.logger.debug("PLUGIN create_dirs '%s' already there" % self.recovery_path)
				pass
			else:
				raise

	def make_recovery_copy(self, cache_file):
		path = re.sub(self.cache.cache_path, '', cache_file)
		path = re.sub('/files', '', path)
		recovery_file = self.recovery_path + path

		self.logger.info("PLUGIN copying file from '%s' to '%s'"%(cache_file, recovery_file))

		recovery_path = os.path.dirname(recovery_file)
		try:
			os.makedirs(recovery_path)
			self.logger.debug("PLUGIN created recovery path '%s' done" % recovery_path)
		except OSError as exc: # Python >2.5                                        
			if exc.errno == errno.EEXIST and os.path.isdir(recovery_path):
				self.logger.debug("PLUGIN create_dirs '%s' already there" % recovery_path)
				pass
			else:
				raise

	
		import shutil
		shutil.copyfile(cache_file, recovery_file)

		self.logger.info("PLUGIN copying file from '%s' to '%s' done"%(cache_file, recovery_file))

		return True



	def do_cmd_on_s3_now_w_retries(self, fn):
		# self, key, pub, action, args, kargs, retries = 1
		def wrapper(*args, **kargs):
			try:
				return fn(*args, **kargs)
			except Exception as e:
				self.logger.error("PLUGIN")
				selfless_args = None
				if args[1]:
					selfless_args = args[1:]
				self.logger.error("PLUGIN do_cmd_on_s3_now_w_retries FAILED" + " " + str(selfless_args))

				s = args[0]
				key = args[1]
				pub = args[2]
				action = args[3]
				arg = args[4]
				kargs = args[5]


				### trying to recover
				if pub[0] == 'upload':
					try:
						path = pub[1]
						cache_file = s.cache.get_cache_filename(path)
						cache_stat = os.stat(cache_file)
						etag = None
						etag_filename = s.cache.get_cache_etags_filename(path)
						if os.path.isfile(etag_filename):
								with open(etag_filename, mode='r') as etag_file:
										etag = etag_file.read()
					#	print etag_filename
					#	print etag


						json_recover = {
							"action" : action,
							"action_time" : self.epochseconds_to_iso8601(),
							"pub_action" : pub[0],
							"file" : path,
							"cache_file" : cache_file,
							"cache_stat" : self.stat_to_dict(cache_stat),
							# "cache_file_size" : cache_stat.st_size,
							# "cache_file_ctime" : self.epochseconds_to_iso8601(cache_stat.st_ctime),
							# "cache_file_mtime" : self.epochseconds_to_iso8601(cache_stat.st_mtime),
							"etag_filename": etag_filename,
							"etag": etag,
							"exception": str(e),
							"s3key" : dict(filter(self.s3key_json_filter, iter(key.__dict__.items())))
						}

						self.logger.error("RecoverYAS3FS PLUGIN UPLOAD FAILED "  + json.dumps(json_recover))

						self.make_recovery_copy(cache_file)

					except Exception as e:
						self.logger.exception(e)

			return args[2] #????
		return wrapper

