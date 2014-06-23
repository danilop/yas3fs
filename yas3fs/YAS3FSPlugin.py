#!/usr/bin/python 

import imp
import os
import inspect
import logging

class YAS3FSPlugin:
	@staticmethod
	def load_from_file(filepath, expected_class = None):
		class_inst = None

		try:
			mod_name,file_ext = os.path.splitext(os.path.split(filepath)[-1])
			if file_ext.lower() == '.py':
				py_mod = imp.load_source(mod_name, filepath)

			elif file_ext.lower() == '.pyc':
				py_mod = imp.load_compiled(mod_name, filepath)
			else:
				raise

			if not py_mod:
				raise

			for klass in inspect.getmembers(py_mod,inspect.isclass):
				if not issubclass(klass[1], YAS3FSPlugin):
					continue

				if expected_class == None or expected_class == klass[0]:
					class_inst = klass[1]()
					break
		except Exception, e:
			raise Exception("cannot load plugin file " + filepath + " " + e)


		return class_inst


	def __init__(self, logger=None):
		self.logger = logger
		if (not self.logger):
			self.logger = logging.getLogger('yas3fsPlugin')

	def do_cmd_on_s3_now_w_retries(self, fn):
		# self, key, pub, action, args, kargs, retries = 1
		def wrapper(*args, **kargs):
			try:	
				return fn(*args, **kargs)
			except Exception as e:
				selfless_args = None
				if args[1]:
					selfless_args = args[1:]
				self.logger.info("PLUGIN do_cmd_on_s3_now_w_retries FAILED" + " " + str(selfless_args))

			return args[2] #????
		return wrapper

